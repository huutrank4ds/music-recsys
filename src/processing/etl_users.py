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

# Cấu hình Packages
PACKAGES = [
    "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0",
    "org.apache.hadoop:hadoop-aws:3.3.4",
    "org.mongodb.spark:mongo-spark-connector_2.12:10.3.0"
]

def run_users_etl():
    print("=" * 60)
    print("🎵 ETL Users Collection (MongoDB)")
    print(f"   Started at: {datetime.now()}")
    print("=" * 60)
    
    # 1. Khởi tạo Spark
    spark = SparkSession.builder \
        .appName("ETL_Users_Master") \
        .master("spark://spark-master:7077") \
        .config("spark.jars.packages", ",".join(PACKAGES)) \
        .config("spark.executor.memory", "1g") \
        .config("spark.executor.cores", "1") \
        .config("spark.cores.max", "1") \
        .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
        .config("spark.hadoop.fs.s3a.access.key", "minioadmin") \
        .config("spark.hadoop.fs.s3a.secret.key", "minioadmin") \
        .config("spark.hadoop.fs.s3a.path.style.access", "true") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .config("spark.mongodb.write.connection.uri", "mongodb://mongodb:27017/music_recsys.users") \
        .getOrCreate()

    # 2. Đọc dữ liệu từ MinIO
    print(">>> Đang đọc dữ liệu từ MinIO...")
    try:
        df = spark.read.parquet("s3a://datalake/raw/music_logs/")
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
        .option("database", "music_recsys") \
        .option("collection", "users") \
        .save()

    print(f"THÀNH CÔNG! Đã lưu {user_count} users vào MongoDB.")
    print(f"   Completed at: {datetime.now()}")
    print("=" * 60)
    spark.stop()

if __name__ == "__main__":
    run_users_etl()
