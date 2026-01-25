"""
Spark Streaming ETL - Kafka to MinIO
=====================================
Đọc dữ liệu từ Kafka topic và ghi xuống MinIO (Parquet).
"""

from pyspark.sql.functions import from_json, col, to_timestamp, date_format
import config as cfg
from common.spark_schemas import get_music_log_schema
from utils import get_spark_session, ensure_minio_bucket, GracefulStopper
from common.logger import get_logger

# Khởi tạo logger
TASK_NAME = "Stream_to_MinIO"
logger = get_logger(TASK_NAME)


def run_etl():

    stopper = GracefulStopper(logger=logger)
    logger.info(f"[{TASK_NAME}] Khởi động Spark Streaming ETL...")
    
    # Khởi tạo Spark từ utils
    spark = get_spark_session(TASK_NAME)
    spark.sparkContext.setLogLevel("WARN")
    ensure_minio_bucket(cfg.DATALAKE_BUCKET, logger=logger)

    # Sử dụng schema từ spark_schemas.py
    schema = get_music_log_schema()

    # Đọc dữ liệu từ Kafka
    logger.info(f"[{TASK_NAME}] Đang lắng nghe Kafka Topic '{cfg.KAFKA_TOPIC}'...")
    kafka_df = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", cfg.KAFKA_BOOTSTRAP_SERVERS) \
        .option("subscribe", cfg.KAFKA_TOPIC) \
        .option("startingOffsets", "latest") \
        .load()

    # Transform & Parse
    processed_df = kafka_df.selectExpr("CAST(value AS STRING)") \
        .select(from_json(col("value"), schema).alias("data")) \
        .select("data.*") \
        .withColumn("event_time", to_timestamp(col("timestamp"))) \
        .withColumn("date_str", date_format(col("event_time"), "yyyy-MM-dd"))

    # Ghi xuống MinIO 
    logger.info(f"[{TASK_NAME}] Đang ghi xuống MinIO (Parquet)...")
    
    checkpoint_path = f"s3a://{cfg.DATALAKE_BUCKET}/checkpoints/music_logs/"
    output_path = cfg.MINIO_RAW_MUSIC_LOGS_PATH

    query = processed_df.writeStream \
        .format("parquet") \
        .option("path", output_path) \
        .option("checkpointLocation", checkpoint_path) \
        .partitionBy("date_str") \
        .outputMode("append") \
        .trigger(processingTime="1 minute") \
        .start()
    
    stopper.attach(query)
    
    try:
        query.awaitTermination()
    except Exception as e:
        logger.error(f"[{TASK_NAME}] Streaming bị lỗi: {e}")
    finally:
        # Đây là lúc chính thức trả Worker và đóng ứng dụng
        logger.info(f"[{TASK_NAME}] Đang đóng Spark Session...")
        spark.stop()
        logger.info(f"[{TASK_NAME}] ETL đã tắt thành công. Tài nguyên đã được giải phóng.")

if __name__ == "__main__":
    run_etl()
