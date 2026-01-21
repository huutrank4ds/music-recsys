"""
ETL Master Data - Songs Collection (MongoDB)
=============================================
Trích xuất danh sách bài hát từ Data Lake và lưu vào MongoDB.
Schema: {_id, title, artist, artist_id}
"""

from pyspark.sql.functions import col, first

# Import config và utils tập trung
import config as cfg
from utils import get_spark_session
from common.logger import get_logger

# Khởi tạo logger
logger = get_logger("ETL_Songs")

def run_master_data_etl():
    logger.info("Bắt đầu ETL Master Data (Collection: songs)...")
    
    # 1. Khởi tạo Spark từ utils
    spark = get_spark_session("ETL_Songs_Master")
    spark.conf.set("spark.executor.memory", "1g")
    spark.conf.set("spark.executor.cores", "1")
    spark.conf.set("spark.cores.max", "1")

    # 2. Đọc dữ liệu từ MinIO (Data Lake)
    logger.info("Đang đọc dữ liệu từ MinIO...")
    try:
        df = spark.read.parquet(cfg.MINIO_RAW_MUSIC_LOGS_PATH)
    except Exception as e:
        logger.error(f"Lỗi đọc MinIO (Có thể do chưa có data): {e}")
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
    logger.info("Đang lọc bài hát duy nhất...")
    songs_unique = songs_raw.groupBy("_id").agg(
        first("title").alias("title"),
        first("artist").alias("artist"),
        first("artist_id").alias("artist_id"),
    )

    # 5. Load (Ghi vào MongoDB)
    logger.info("Đang ghi vào MongoDB...")
    songs_unique.write \
        .format("mongodb") \
        .mode("overwrite") \
        .option("database", cfg.MONGO_DB) \
        .option("collection", cfg.COLLECTION_SONGS) \
        .save()

    logger.info(f"THÀNH CÔNG! Đã lưu {songs_unique.count()} bài hát vào MongoDB.")
    spark.stop()

if __name__ == "__main__":
    run_master_data_etl()
