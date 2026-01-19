"""
ETL Users Collection - MongoDB
==============================
Trích xuất danh sách users duy nhất từ logs và tạo sẵn
trong MongoDB để chuẩn bị cho việc sync latent_vector sau này.
"""

import sys
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, first, current_timestamp
from pyspark.sql.types import ArrayType, FloatType

# Import config tập trung
import src.config as cfg

def run_users_etl():
    print("=" * 60)
    print("🎵 ETL Users Collection (MongoDB)")
    print(f"   Started at: {datetime.now()}")
    print("=" * 60)
    
    # 1. Khởi tạo Spark (Không cần spark.jars.packages - Docker đã tích hợp sẵn)
    spark = SparkSession.builder \
        .appName("ETL_Users_Master") \
        .master("spark://spark-master:7077") \
        .config("spark.executor.memory", "1g") \
        .config("spark.executor.cores", "1") \
        .config("spark.cores.max", "1") \
        .config("spark.hadoop.fs.s3a.endpoint", cfg.MINIO_ENDPOINT) \
        .config("spark.hadoop.fs.s3a.access.key", cfg.MINIO_ACCESS_KEY) \
        .config("spark.hadoop.fs.s3a.secret.key", cfg.MINIO_SECRET_KEY) \
        .config("spark.hadoop.fs.s3a.path.style.access", "true") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .config("spark.mongodb.write.connection.uri", f"{cfg.MONGO_URI}/{cfg.MONGO_DB}.{cfg.COLLECTION_USERS}") \
        .getOrCreate()

    # 2. Đọc dữ liệu từ MinIO
    print(">>> Đang đọc dữ liệu từ MinIO...")
    try:
        df = spark.read.parquet(cfg.MINIO_RAW_MUSIC_LOGS_PATH)
    except Exception as e:
        print(f"Lỗi đọc MinIO (Có thể do chưa có data): {e}")
        spark.stop()
        return

    # 3. Trích xuất danh sách users duy nhất
    print(">>> Đang lọc users duy nhất...")
    users_unique = df.select("user_id").distinct() \
        .withColumn("username", col("user_id")) \
        .withColumn("latent_vector", lit(None).cast(ArrayType(FloatType()))) \
        .withColumn("last_updated", lit(None).cast("timestamp")) \
        .select(
            col("user_id").alias("_id"),
            col("username"),
            col("latent_vector"),
            col("last_updated")
        )

    # 4. Ghi vào MongoDB (Mode: Append - Không ghi đè users đã có vector)
    # Sử dụng upsert để chỉ thêm users mới, không xóa latent_vector đã có
    print(">>> Đang ghi vào MongoDB...")
    
    # Đếm trước khi ghi
    user_count = users_unique.count()
    
    # Ghi với mode overwrite (sẽ được thay bằng upsert logic trong train_als_model.py)
    users_unique.write \
        .format("mongodb") \
        .mode("overwrite") \
        .option("database", cfg.MONGO_DB) \
        .option("collection", cfg.COLLECTION_USERS) \
        .save()

    print(f"THÀNH CÔNG! Đã lưu {user_count} users vào MongoDB.")
    print(f"   Completed at: {datetime.now()}")
    print("=" * 60)
    spark.stop()

if __name__ == "__main__":
    run_users_etl()
