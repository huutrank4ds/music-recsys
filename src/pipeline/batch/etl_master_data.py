"""
ETL Master Data - Songs Collection (MongoDB)
=============================================
Trích xuất danh sách bài hát từ Data Lake và lưu vào MongoDB.
Schema: {_id, title, artist, artist_id}
"""

import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, first
from pyspark.sql.types import StructType, StructField, StringType, LongType

# Import config tập trung
import src.config as cfg

def run_master_data_etl():
    print("Bắt đầu ETL Master Data (Collection: songs)...")
    
    # 1. Khởi tạo Spark (Không cần spark.jars.packages - Docker đã tích hợp sẵn)
    spark = SparkSession.builder \
        .appName("ETL_Songs_Master") \
        .master("spark://spark-master:7077") \
        .config("spark.executor.memory", "1g") \
        .config("spark.executor.cores", "1") \
        .config("spark.cores.max", "1") \
        .config("spark.hadoop.fs.s3a.endpoint", cfg.MINIO_ENDPOINT) \
        .config("spark.hadoop.fs.s3a.access.key", cfg.MINIO_ACCESS_KEY) \
        .config("spark.hadoop.fs.s3a.secret.key", cfg.MINIO_SECRET_KEY) \
        .config("spark.hadoop.fs.s3a.path.style.access", "true") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .config("spark.mongodb.write.connection.uri", f"{cfg.MONGO_URI}/{cfg.MONGO_DB}.{cfg.COLLECTION_SONGS}") \
        .getOrCreate()

    # 2. Đọc dữ liệu từ MinIO (Data Lake)
    print(">>> Đang đọc dữ liệu từ MinIO...")
    try:
        df = spark.read.parquet(cfg.MINIO_RAW_MUSIC_LOGS_PATH)
    except Exception as e:
        print(f"Lỗi đọc MinIO (Có thể do chưa có data): {e}")
        spark.stop()
        return

    # 3. Transform (Schema theo README mới)
    # Mapping: {_id, title, artist, artist_id}
    songs_raw = df.select(
        col("musicbrainz_track_id").alias("_id"),        # PK: Track ID (UUID)
        col("track_name").alias("title"),                # Tên bài hát
        col("artist_name").alias("artist"),              # Tên nghệ sĩ
        col("musicbrainz_artist_id").alias("artist_id"), # Mã định danh nghệ sĩ
    )

    # 4. Deduplicate (Lọc trùng lặp)
    print(">>> Đang lọc bài hát duy nhất...")
    songs_unique = songs_raw.groupBy("_id").agg(
        first("title").alias("title"),
        first("artist").alias("artist"),
        first("artist_id").alias("artist_id"),
    )

    # 5. Load (Ghi vào MongoDB)
    print(">>> Đang ghi vào MongoDB...")
    songs_unique.write \
        .format("mongodb") \
        .mode("overwrite") \
        .option("database", cfg.MONGO_DB) \
        .option("collection", cfg.COLLECTION_SONGS) \
        .save()

    print(f"THÀNH CÔNG! Đã lưu {songs_unique.count()} bài hát vào MongoDB.")
    spark.stop()

if __name__ == "__main__":
    run_master_data_etl()