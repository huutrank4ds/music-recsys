"""
Train ALS Model - Nightly Batch Job
====================================
Workflow:
1. Load Parquet từ MinIO (90 days sliding window)
2. Train ALS (Alternating Least Squares)
3. Sync User Factors -> MongoDB (users.latent_vector)
4. Sync Item Factors -> Milvus (music_collection)
"""

import sys
from datetime import datetime, timedelta
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, current_timestamp, udf, expr
from pyspark.sql.types import ArrayType, FloatType, StringType
from pyspark.ml.recommendation import ALS
from pymongo import MongoClient, UpdateOne
from pymilvus import (
    connections,
    Collection,
    FieldSchema,
    CollectionSchema,
    DataType,
    utility
)

# ============================================================
# CONFIGURATION
# ============================================================
SPARK_PACKAGES = [
    "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0",
    "org.apache.hadoop:hadoop-aws:3.3.4",
]

# ALS Hyperparameters
ALS_RANK = 64           # Số chiều của latent vector
ALS_MAX_ITER = 15       # Số vòng lặp
ALS_REG_PARAM = 0.1     # Regularization
ALS_ALPHA = 40.0        # Confidence scaling (Implicit Feedback)

# Sliding Window (90 days)
SLIDING_WINDOW_DAYS = 90

# Connection configs
MINIO_ENDPOINT = "http://minio:9000"
MINIO_ACCESS_KEY = "minioadmin"
MINIO_SECRET_KEY = "minioadmin"
MONGODB_URI = "mongodb://mongodb:27017"
MILVUS_HOST = "milvus"
MILVUS_PORT = 19530

# Collection names
MILVUS_COLLECTION = "music_collection"
MONGO_DB = "music_recsys"
MONGO_USERS_COLLECTION = "users"


def create_spark_session():
    """Khởi tạo Spark Session với các configs cần thiết."""
    return SparkSession.builder \
        .appName("ALS_Batch_Training") \
        .master("spark://spark-master:7077") \
        .config("spark.jars.packages", ",".join(SPARK_PACKAGES)) \
        .config("spark.executor.memory", "2g") \
        .config("spark.executor.cores", "2") \
        .config("spark.cores.max", "4") \
        .config("spark.hadoop.fs.s3a.endpoint", MINIO_ENDPOINT) \
        .config("spark.hadoop.fs.s3a.access.key", MINIO_ACCESS_KEY) \
        .config("spark.hadoop.fs.s3a.secret.key", MINIO_SECRET_KEY) \
        .config("spark.hadoop.fs.s3a.path.style.access", "true") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .getOrCreate()


def load_training_data(spark, days=SLIDING_WINDOW_DAYS):
    """
    Load dữ liệu từ MinIO với Sliding Window.
    Chỉ lấy dữ liệu trong N ngày gần nhất.
    """
    print(f">>> Loading data from MinIO (Last {days} days)...")
    
    # Tính ngày bắt đầu của window
    cutoff_date = datetime.now() - timedelta(days=days)
    cutoff_str = cutoff_date.strftime("%Y-%m-%d")
    
    try:
        # Đọc toàn bộ Parquet từ MinIO
        df = spark.read.parquet("s3a://datalake/raw/music_logs/")
        
        # Filter theo timestamp (Sliding Window)
        df_filtered = df.filter(col("date_str") >= cutoff_str)
        
        print(f">>> Filtered {df_filtered.count()} records since {cutoff_str}")
        return df_filtered
        
    except Exception as e:
        print(f"ERROR loading from MinIO: {e}")
        return None


def prepare_als_data(df):
    """
    Chuẩn bị dữ liệu cho ALS.
    ALS cần: user_index (int), item_index (int), rating (float)
    """
    print(">>> Preparing data for ALS...")
    
    # Đếm số lần user nghe mỗi bài (Implicit Feedback)
    # user_id -> user_index (đã có sẵn trong data)
    # track_index -> item_index (đã có sẵn trong data)
    
    interactions = df.groupBy("user_id", "track_index").count() \
        .withColumnRenamed("count", "play_count")
    
    # Tạo user_index từ user_id (String -> Integer mapping)
    from pyspark.sql.window import Window
    from pyspark.sql.functions import dense_rank
    
    # Map user_id (String) -> user_index (Integer)
    user_mapping = df.select("user_id").distinct() \
        .withColumn("user_index", dense_rank().over(Window.orderBy("user_id")) - 1)
    
    # Join để có user_index
    interactions = interactions.join(user_mapping, on="user_id", how="left")
    
    # Chuẩn bị final dataframe cho ALS
    als_data = interactions.select(
        col("user_index").cast("integer"),
        col("track_index").cast("integer").alias("item_index"),
        col("play_count").cast("float").alias("rating")
    )
    
    print(f">>> ALS Data: {als_data.count()} interactions")
    return als_data, user_mapping


def train_als_model(als_data):
    """Train ALS Model với Implicit Feedback."""
    print(">>> Training ALS Model...")
    print(f"    Rank: {ALS_RANK}, MaxIter: {ALS_MAX_ITER}, RegParam: {ALS_REG_PARAM}")
    
    als = ALS(
        rank=ALS_RANK,
        maxIter=ALS_MAX_ITER,
        regParam=ALS_REG_PARAM,
        alpha=ALS_ALPHA,
        implicitPrefs=True,  # Quan trọng: Dùng Implicit Feedback
        userCol="user_index",
        itemCol="item_index",
        ratingCol="rating",
        coldStartStrategy="drop",
        nonnegative=True
    )
    
    model = als.fit(als_data)
    print(">>> ALS Model trained successfully!")
    return model


def sync_user_factors_to_mongodb(model, user_mapping, spark):
    """
    Sync User Factors từ ALS Model vào MongoDB.
    Collection: users
    Schema: {_id, username, latent_vector, last_updated}
    """
    print(">>> Syncing User Factors to MongoDB...")
    
    # Lấy User Factors từ model
    user_factors = model.userFactors  # DataFrame: [id, features]
    
    # Join với user_mapping để lấy user_id gốc
    user_factors_with_id = user_factors \
        .withColumnRenamed("id", "user_index") \
        .join(user_mapping, on="user_index", how="left")
    
    # Convert features sang List (Spark 3.5 đã trả về list, nhưng cần handle cả DenseVector)
    def convert_vector(v):
        if v is None:
            return None
        if isinstance(v, list):
            return [float(x) for x in v]
        return [float(x) for x in v.toArray().tolist()]
    vector_to_list = udf(convert_vector, ArrayType(FloatType()))
    
    user_data = user_factors_with_id.select(
        col("user_id").alias("_id"),
        col("user_id").alias("username"),  # Tạm dùng user_id làm username
        vector_to_list(col("features")).alias("latent_vector")
    ).collect()
    
    # Bulk update vào MongoDB
    client = MongoClient(MONGODB_URI)
    db = client[MONGO_DB]
    collection = db[MONGO_USERS_COLLECTION]
    
    bulk_ops = []
    current_time = datetime.now()
    
    for row in user_data:
        bulk_ops.append(
            UpdateOne(
                {"_id": row["_id"]},
                {
                    "$set": {
                        "username": row["username"],
                        "latent_vector": row["latent_vector"],
                        "last_updated": current_time
                    }
                },
                upsert=True
            )
        )
    
    if bulk_ops:
        result = collection.bulk_write(bulk_ops)
        print(f">>> MongoDB: Upserted {result.upserted_count + result.modified_count} users")
    
    client.close()
    return len(user_data)


def setup_milvus_collection(dimension):
    """
    Tạo/Setup Milvus Collection cho Item Factors.
    """
    print(f">>> Setting up Milvus collection '{MILVUS_COLLECTION}'...")
    
    # Kết nối Milvus
    connections.connect(host=MILVUS_HOST, port=MILVUS_PORT)
    
    # Xóa collection cũ nếu tồn tại (Replace strategy)
    if utility.has_collection(MILVUS_COLLECTION):
        print(f"    Dropping existing collection...")
        utility.drop_collection(MILVUS_COLLECTION)
    
    # Định nghĩa Schema
    fields = [
        FieldSchema(name="id", dtype=DataType.VARCHAR, is_primary=True, max_length=100),
        FieldSchema(name="embedding", dtype=DataType.FLOAT_VECTOR, dim=dimension)
    ]
    schema = CollectionSchema(fields, description="Music embeddings from ALS")
    
    # Tạo Collection
    collection = Collection(name=MILVUS_COLLECTION, schema=schema)
    
    # Tạo Index (IVF_FLAT cho Inner Product)
    index_params = {
        "index_type": "IVF_FLAT",
        "metric_type": "IP",  # Inner Product - tương thích với ALS
        "params": {"nlist": 128}
    }
    collection.create_index(field_name="embedding", index_params=index_params)
    
    print(f">>> Milvus collection created with dimension={dimension}")
    return collection


def sync_item_factors_to_milvus(model, spark):
    """
    Sync Item Factors từ ALS Model vào Milvus.
    Collection: music_collection
    Schema: {id (track_id), embedding (vector)}
    """
    print(">>> Syncing Item Factors to Milvus...")
    
    # Lấy Item Factors từ model
    item_factors = model.itemFactors  # DataFrame: [id, features]
    
    # Lấy dimension từ model
    dimension = ALS_RANK
    
    # Setup Milvus collection
    collection = setup_milvus_collection(dimension)
    
    # Cần mapping item_index -> track_id (từ data gốc)
    # Đọc lại data để lấy mapping
    try:
        df = spark.read.parquet("s3a://datalake/raw/music_logs/")
        track_mapping = df.select("track_index", "musicbrainz_track_id").distinct()
        
        # Join để có track_id
        item_factors_with_id = item_factors \
            .withColumnRenamed("id", "track_index") \
            .join(track_mapping, on="track_index", how="left")
        
        # Convert sang List để insert vào Milvus (Spark 3.5 đã trả về list)
        def convert_vector(v):
            if v is None:
                return None
            if isinstance(v, list):
                return [float(x) for x in v]
            return [float(x) for x in v.toArray().tolist()]
        vector_to_list = udf(convert_vector, ArrayType(FloatType()))
        
        item_data = item_factors_with_id.select(
            col("musicbrainz_track_id").alias("id"),
            vector_to_list(col("features")).alias("embedding")
        ).collect()
        
        # Chuẩn bị data cho Milvus (format: list of lists)
        ids = [row["id"] for row in item_data if row["id"] is not None]
        embeddings = [row["embedding"] for row in item_data if row["id"] is not None]
        
        # Insert vào Milvus theo batch
        BATCH_SIZE = 1000
        total_inserted = 0
        
        for i in range(0, len(ids), BATCH_SIZE):
            batch_ids = ids[i:i+BATCH_SIZE]
            batch_embeddings = embeddings[i:i+BATCH_SIZE]
            
            collection.insert([batch_ids, batch_embeddings])
            total_inserted += len(batch_ids)
            print(f"    Inserted batch {i//BATCH_SIZE + 1}: {len(batch_ids)} items")
        
        # Load collection để search
        collection.load()
        
        print(f">>> Milvus: Inserted {total_inserted} item embeddings")
        return total_inserted
        
    except Exception as e:
        print(f"ERROR syncing to Milvus: {e}")
        raise e
    finally:
        connections.disconnect("default")


def run_training_pipeline():
    """Main training pipeline."""
    print("=" * 60)
    print("🎵 MUSIC RECOMMENDATION - ALS BATCH TRAINING")
    print(f"   Started at: {datetime.now()}")
    print("=" * 60)
    
    # 1. Khởi tạo Spark
    spark = create_spark_session()
    spark.sparkContext.setLogLevel("WARN")
    
    try:
        # 2. Load dữ liệu từ MinIO (Sliding Window)
        df = load_training_data(spark, SLIDING_WINDOW_DAYS)
        if df is None or df.count() == 0:
            print("ERROR: No data available for training!")
            return
        
        # 3. Chuẩn bị dữ liệu cho ALS
        als_data, user_mapping = prepare_als_data(df)
        
        # 4. Train ALS Model
        model = train_als_model(als_data)
        
        # 5. Sync User Factors -> MongoDB
        num_users = sync_user_factors_to_mongodb(model, user_mapping, spark)
        
        # 6. Sync Item Factors -> Milvus
        num_items = sync_item_factors_to_milvus(model, spark)
        
        # Summary
        print("=" * 60)
        print("TRAINING COMPLETED SUCCESSFULLY!")
        print(f"   Users synced to MongoDB: {num_users}")
        print(f"   Items synced to Milvus: {num_items}")
        print(f"   Completed at: {datetime.now()}")
        print("=" * 60)
        
    except Exception as e:
        print(f"TRAINING FAILED: {e}")
        raise e
    finally:
        spark.stop()


if __name__ == "__main__":
    run_training_pipeline()
