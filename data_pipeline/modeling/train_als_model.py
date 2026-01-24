"""
Train ALS Model - Batch Job
============================
1. Load data từ processed_sorted
2. Train ALS (Alternating Least Squares)
3. Sync User Factors -> MongoDB (users.latent_vector)
4. Sync Item Factors -> Milvus (music_collection)
"""

from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count
from pyspark.sql.window import Window
from pyspark.sql.functions import dense_rank
from pyspark.sql.types import ArrayType, FloatType
from pyspark.ml.recommendation import ALS
from pymongo import MongoClient, UpdateOne
from pymilvus import connections, Collection, FieldSchema, CollectionSchema, DataType, utility

# ============================================================
# CONFIGURATION
# ============================================================
DATA_PATH = "/opt/data/processed_sorted"
MONGODB_URI = "mongodb://mongodb:27017"
MONGO_DB = "music_recsys"
MILVUS_HOST = "milvus"
MILVUS_PORT = 19530
MILVUS_COLLECTION = "music_collection"

# ALS Hyperparameters
ALS_RANK = 64
ALS_MAX_ITER = 15
ALS_REG_PARAM = 0.1
ALS_ALPHA = 40.0

def log(msg):
    print(msg, flush=True)
    with open("/tmp/als_train.log", "a") as f:
        f.write(f"{msg}\n")

def run_training():
    log("=" * 60)
    log("🎵 MUSIC RECOMMENDATION - ALS TRAINING")
    log(f"Started at: {datetime.now()}")
    log("=" * 60)
    
    # 1. Spark Session
    log("Khởi tạo Spark Session...")
    spark = SparkSession.builder \
        .appName("ALS_Training") \
        .getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    
    try:
        # 2. Load data
        log(f"Đọc dữ liệu từ: {DATA_PATH}")
        df = spark.read.parquet(DATA_PATH)
        total_rows = df.count()
        log(f"Tổng số dòng: {total_rows:,}")
        
        # 3. Tạo user-item interactions
        log("Chuẩn bị dữ liệu cho ALS...")
        
        # Đếm play count
        interactions = df.groupBy("user_id", "musicbrainz_track_id").agg(
            count("*").alias("play_count")
        )
        
        # Tạo user_index
        user_mapping = df.select("user_id").distinct() \
            .withColumn("user_index", dense_rank().over(Window.orderBy("user_id")) - 1)
        
        # Tạo item_index
        item_mapping = df.select("musicbrainz_track_id").distinct() \
            .withColumn("item_index", dense_rank().over(Window.orderBy("musicbrainz_track_id")) - 1)
        
        # Join để có index
        als_data = interactions \
            .join(user_mapping, on="user_id") \
            .join(item_mapping, on="musicbrainz_track_id") \
            .select(
                col("user_index").cast("integer"),
                col("item_index").cast("integer"),
                col("play_count").cast("float").alias("rating")
            )
        
        interaction_count = als_data.count()
        log(f"Số interactions: {interaction_count:,}")
        
        # 4. Train ALS
        log(f"Training ALS (Rank={ALS_RANK}, MaxIter={ALS_MAX_ITER})...")
        als = ALS(
            rank=ALS_RANK,
            maxIter=ALS_MAX_ITER,
            regParam=ALS_REG_PARAM,
            alpha=ALS_ALPHA,
            implicitPrefs=True,
            userCol="user_index",
            itemCol="item_index",
            ratingCol="rating",
            coldStartStrategy="drop",
            nonnegative=True
        )
        model = als.fit(als_data)
        log("✅ ALS Model trained!")
        
        # 5. Sync User Factors -> MongoDB
        log("Syncing User Factors to MongoDB...")
        user_factors = model.userFactors
        
        # Join với user_mapping
        user_factors_with_id = user_factors \
            .withColumnRenamed("id", "user_index") \
            .join(user_mapping, on="user_index")
        
        # Collect và update MongoDB
        user_data = user_factors_with_id.select("user_id", "features").collect()
        
        client = MongoClient(MONGODB_URI)
        db = client[MONGO_DB]
        users_col = db["users"]
        
        bulk_ops = []
        for row in user_data:
            # Convert features to list
            features = row["features"]
            if hasattr(features, 'toArray'):
                vector = [float(x) for x in features.toArray()]
            else:
                vector = [float(x) for x in features]
            
            bulk_ops.append(UpdateOne(
                {"_id": row["user_id"]},
                {"$set": {"latent_vector": vector, "last_updated": datetime.now()}},
                upsert=True
            ))
        
        if bulk_ops:
            result = users_col.bulk_write(bulk_ops)
            log(f"MongoDB: Updated {result.modified_count + result.upserted_count} users")
        
        client.close()
        
        # 6. Sync Item Factors -> Milvus
        log("Syncing Item Factors to Milvus...")
        
        # Connect Milvus
        connections.connect(host=MILVUS_HOST, port=MILVUS_PORT)
        
        # Drop old collection
        if utility.has_collection(MILVUS_COLLECTION):
            utility.drop_collection(MILVUS_COLLECTION)
        
        # Create collection
        fields = [
            FieldSchema(name="id", dtype=DataType.VARCHAR, is_primary=True, max_length=100),
            FieldSchema(name="embedding", dtype=DataType.FLOAT_VECTOR, dim=ALS_RANK)
        ]
        schema = CollectionSchema(fields, description="Music embeddings from ALS")
        collection = Collection(name=MILVUS_COLLECTION, schema=schema)
        
        # Create index
        index_params = {"index_type": "IVF_FLAT", "metric_type": "IP", "params": {"nlist": 128}}
        collection.create_index(field_name="embedding", index_params=index_params)
        
        # Get item factors
        item_factors = model.itemFactors
        item_factors_with_id = item_factors \
            .withColumnRenamed("id", "item_index") \
            .join(item_mapping, on="item_index")
        
        item_data = item_factors_with_id.select("musicbrainz_track_id", "features").collect()
        
        # Insert to Milvus
        ids = []
        embeddings = []
        for row in item_data:
            track_id = row["musicbrainz_track_id"]
            features = row["features"]
            if track_id and features:
                if hasattr(features, 'toArray'):
                    vector = [float(x) for x in features.toArray()]
                else:
                    vector = [float(x) for x in features]
                ids.append(track_id)
                embeddings.append(vector)
        
        # Batch insert
        BATCH_SIZE = 5000
        total_inserted = 0
        for i in range(0, len(ids), BATCH_SIZE):
            batch_ids = ids[i:i+BATCH_SIZE]
            batch_embeddings = embeddings[i:i+BATCH_SIZE]
            collection.insert([batch_ids, batch_embeddings])
            total_inserted += len(batch_ids)
            log(f"  Inserted batch: {total_inserted}/{len(ids)}")
        
        collection.load()
        log(f"Milvus: Inserted {total_inserted} items")
        
        connections.disconnect("default")
        
        # Summary
        log("=" * 60)
        log("✅ TRAINING COMPLETED!")
        log(f"Users: {len(user_data)}")
        log(f"Items: {total_inserted}")
        log(f"Completed at: {datetime.now()}")
        log("=" * 60)
        
    except Exception as e:
        log(f"❌ ERROR: {e}")
        import traceback
        traceback.print_exc()
        raise e
    finally:
        spark.stop()

if __name__ == "__main__":
    open("/tmp/als_train.log", "w").close()
    run_training()
