"""
ETL Users Collection - MongoDB
==============================
Trích xuất danh sách users duy nhất từ logs và tạo sẵn
trong MongoDB để chuẩn bị cho việc sync latent_vector sau này.
"""

from datetime import datetime
from pyspark.sql.functions import col, lit
from pyspark.sql.types import ArrayType, FloatType

# Import config và utils tập trung
import src.config as cfg
from src.utils import get_logger, get_spark_session

# Khởi tạo logger
logger = get_logger("ETL_Users")

def run_users_etl():
    logger.info("=" * 60)
    logger.info("ETL Users Collection (MongoDB)")
    logger.info(f"Started at: {datetime.now()}")
    logger.info("=" * 60)
    
    # 1. Khởi tạo Spark từ utils (đã có sẵn config MinIO & MongoDB)
    spark = get_spark_session("ETL_Users_Master")
    spark.conf.set("spark.executor.memory", "1g")
    spark.conf.set("spark.executor.cores", "1")
    spark.conf.set("spark.cores.max", "1")

    # 2. Đọc dữ liệu từ MinIO
    logger.info("Đang đọc dữ liệu từ MinIO...")
    try:
        df = spark.read.parquet(cfg.MINIO_RAW_MUSIC_LOGS_PATH)
    except Exception as e:
        logger.error(f"Lỗi đọc MinIO (Có thể do chưa có data): {e}")
        spark.stop()
        return

    # 3. Trích xuất danh sách users duy nhất
    logger.info("Đang lọc users duy nhất...")
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

    # 4. Ghi vào MongoDB
    logger.info("Đang ghi vào MongoDB...")
    
    # Đếm trước khi ghi
    user_count = users_unique.count()
    
    users_unique.write \
        .format("mongodb") \
        .mode("overwrite") \
        .option("database", cfg.MONGO_DB) \
        .option("collection", cfg.COLLECTION_USERS) \
        .save()

    logger.info(f"THÀNH CÔNG! Đã lưu {user_count} users vào MongoDB.")
    logger.info(f"Completed at: {datetime.now()}")
    logger.info("=" * 60)
    spark.stop()

if __name__ == "__main__":
    run_users_etl()
