"""
Train ALS Model - Batch Job (Optimized)
=======================================
Workflow:
1. Xác định khung thời gian (Sliding Window) từ config.
2. Đọc dữ liệu log từ MinIO (Partition Pruning theo date_str).
3. Tính điểm (Implicit Feedback) dựa trên hành động:
   - complete: 1.0 điểm
   - listen: 0.5 điểm
   - skip: 0.0 điểm
4. Train ALS.
5. Đồng bộ kết quả sang MongoDB & Milvus.
"""

from datetime import datetime, timedelta
from pyspark.sql.functions import col, sum, when, lit
from pyspark.ml.recommendation import ALS
from pyspark.ml.feature import StringIndexer, IndexToString
from pymongo import MongoClient, UpdateOne #type: ignore
from pymilvus import connections, Collection, utility #type: ignore

import config as cfg
from utils import get_spark_session
from common.logger import get_logger

# Import Schemas và Params từ module chung
from common.schemas.milvus_schemas import (
    get_milvus_song_embedding_schema,
    index_params_milvus
)

# Khởi tạo Logger
logger = get_logger("ALS Training Job")

def run_training():
    logger.info("=" * 60)
    logger.info("BẮT ĐẦU HUẤN LUYỆN MÔ HÌNH GỢI Ý (ALS) - CÓ TRỌNG SỐ")
    logger.info(f"Thời gian bắt đầu: {datetime.now()}")
    logger.info("=" * 60)

    # 1. Khởi tạo Spark Session
    logger.info("Đang khởi tạo Spark Session...")
    spark = get_spark_session("ALS_Training_Batch")
    spark.sparkContext.setLogLevel("WARN")

    try:
        # ---------------------------------------------------------
        # 2. Đọc dữ liệu & Xử lý Sliding Window
        # ---------------------------------------------------------
        # Tính toán ngày cắt (Cutoff date)
        window_days = cfg.ALS_SLIDING_WINDOW_DAYS
        cutoff_date = (datetime.now() - timedelta(days=window_days)).strftime("%Y-%m-%d")
        
        logger.info(f"Áp dụng Cửa sổ trượt (Sliding Window): {window_days} ngày (Lấy dữ liệu từ {cutoff_date})")
        logger.info(f"Đang đọc dữ liệu từ MinIO: {cfg.MINIO_RAW_PATH}")
        
        # Đọc Parquet từ root
        df_raw = spark.read.parquet(cfg.MINIO_RAW_PATH)
        
        # KỸ THUẬT PARTITION PRUNING:
        # Spark sẽ chỉ quét các folder có date_str >= cutoff_date
        df_windowed = df_raw.filter(col("date_str") >= cutoff_date)
        
        total_rows = df_windowed.count()
        logger.info(f"Tổng số dòng log trong {window_days} ngày qua: {total_rows:,}")

        if total_rows == 0:
            logger.warning("Không có dữ liệu mới trong khoảng thời gian này. Dừng chương trình.")
            return

        # ---------------------------------------------------------
        # 3. Tính điểm Trọng số (Weighted Scoring)
        # ---------------------------------------------------------
        logger.info("Đang tính toán điểm tương tác (Implicit Ratings)...")
        
        # Gán điểm dựa trên Action
        df_scored = df_windowed.withColumn("score", 
            when(col("action") == "complete", 1.0)
            .when(col("action") == "listen", 0.5)
            .otherwise(0.0)
        )
        
        # Lọc bỏ những dòng có score = 0
        df_active = df_scored.filter(col("score") > 0)

        # ---------------------------------------------------------
        # 4. Indexing & Grouping
        # ---------------------------------------------------------
        # Chuyển đổi User ID (String) -> User Index (Int)
        user_indexer = StringIndexer(inputCol="user_id", outputCol="user_index", handleInvalid="skip")
        user_indexer_model = user_indexer.fit(df_active)
        df_indexed = user_indexer_model.transform(df_active)

        # Chuyển đổi Track ID (String) -> Item Index (Int)
        item_indexer = StringIndexer(inputCol="track_id", outputCol="item_index", handleInvalid="skip")
        item_indexer_model = item_indexer.fit(df_active)
        df_indexed = item_indexer_model.transform(df_indexed)

        # Tính tổng điểm (Sum)
        als_data = df_indexed.groupBy("user_index", "item_index") \
            .agg(sum("score").alias("rating")) \
            .select(
                col("user_index").cast("integer"),
                col("item_index").cast("integer"),
                col("rating").cast("float")
            )

        interaction_count = als_data.count()
        logger.info(f"Tổng số cặp tương tác (User-Item) sau khi lọc và gộp: {interaction_count:,}")

        # ---------------------------------------------------------
        # 5. Huấn luyện mô hình ALS
        # ---------------------------------------------------------
        logger.info(f"Đang huấn luyện ALS (Rank={cfg.ALS_RANK}, MaxIter={cfg.ALS_MAX_ITER}, Reg={cfg.ALS_REG_PARAM})...")
        
        als = ALS(
            rank=cfg.ALS_RANK,
            maxIter=cfg.ALS_MAX_ITER,
            regParam=cfg.ALS_REG_PARAM,
            alpha=cfg.ALS_ALPHA,
            implicitPrefs=True,
            userCol="user_index",
            itemCol="item_index",
            ratingCol="rating",
            coldStartStrategy="drop",
            nonnegative=True
        )
        
        model = als.fit(als_data)
        logger.info("Đã huấn luyện xong mô hình ALS!")

        # ---------------------------------------------------------
        # 6. Đồng bộ User Factors -> MongoDB
        # ---------------------------------------------------------
        logger.info("Bắt đầu đồng bộ User Vectors sang MongoDB...")
        
        user_factors = model.userFactors
        user_labels = user_indexer_model.labels
        
        # Converter Index -> String
        converter = IndexToString(inputCol="id", outputCol="user_id", labels=user_labels)
        user_factors_with_id = converter.transform(user_factors)

        # Collect về Driver
        user_vectors = user_factors_with_id.select("user_id", "features").collect()
        
        mongo_client = MongoClient(cfg.MONGO_URI)
        mongo_db = mongo_client[cfg.MONGO_DB]
        users_col = mongo_db[cfg.MONGO_USERS_COLLECTION]
        
        bulk_ops = []
        for row in user_vectors:
            u_id = row["user_id"]
            features = row["features"]
            vector_list = [float(x) for x in features]
            
            bulk_ops.append(UpdateOne(
                {"_id": u_id},
                {"$set": {
                    "latent_vector": vector_list,
                    "last_model_update": datetime.now()
                }},
                upsert=True
            ))
            
            if len(bulk_ops) >= 1000:
                users_col.bulk_write(bulk_ops, ordered=False)
                bulk_ops = []

        if bulk_ops:
            users_col.bulk_write(bulk_ops, ordered=False)
            
        logger.info(f"Đã cập nhật Vector cho {len(user_vectors)} người dùng.")
        mongo_client.close()

        # ---------------------------------------------------------
        # 7. Đồng bộ Item Factors -> Milvus
        # ---------------------------------------------------------
        logger.info("Bắt đầu đồng bộ Item Vectors sang Milvus...")
        
        connections.connect(alias="default", uri=cfg.MILVUS_URI)
        collection_name = cfg.MILVUS_ALS_COLLECTION
        
        if utility.has_collection(collection_name):
            utility.drop_collection(collection_name)
            logger.info(f"Đã xóa collection cũ '{collection_name}'")

        schema = get_milvus_song_embedding_schema(dim=cfg.ALS_RANK)
        collection = Collection(collection_name, schema)
        
        logger.info("Đang tạo Index cho Milvus...")
        collection.create_index("embedding", index_params_milvus)
        
        # Xử lý Item Factors
        item_factors = model.itemFactors
        item_labels = item_indexer_model.labels
        item_converter = IndexToString(inputCol="id", outputCol="track_id", labels=item_labels)
        item_factors_with_id = item_converter.transform(item_factors)
        
        item_vectors = item_factors_with_id.select("track_id", "features").collect()
        
        milvus_ids = []
        milvus_embeddings = []
        
        for row in item_vectors:
            milvus_ids.append(row["track_id"])
            milvus_embeddings.append([float(x) for x in row["features"]])
            
        # Batch Insert
        batch_size = 5000
        for i in range(0, len(milvus_ids), batch_size):
            end = i + batch_size
            collection.insert([
                milvus_ids[i:end],
                milvus_embeddings[i:end]
            ])
            
        collection.flush()
        logger.info(f"Milvus: Đã đồng bộ {collection.num_entities} bài hát vào collection '{collection_name}'.")
        
        connections.disconnect("default")

        logger.info("=" * 60)
        logger.info("HOÀN TẤT HUẤN LUYỆN VÀ ĐỒNG BỘ DỮ LIỆU!")
        logger.info("=" * 60)

    except Exception as e:
        logger.error(f"LỖI TRONG QUÁ TRÌNH TRAIN: {e}")
        import traceback
        traceback.print_exc()
    finally:
        spark.stop()

if __name__ == "__main__":
    run_training()