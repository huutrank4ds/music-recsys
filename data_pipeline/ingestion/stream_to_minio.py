"""
Spark Streaming ETL - Kafka to MinIO
=====================================
"""

from pyspark.sql.functions import from_json, col, to_timestamp, date_format
import config as cfg
from common.schemas.spark_schemas import SparkSchemas
from utils import get_spark_session, ensure_minio_bucket, GracefulStopper, wait_for_topic
from common.logger import get_logger


# Khởi tạo logger
TASK_NAME = "Stream to MinIO"
logger = get_logger(TASK_NAME)

CHECKPOINT_PATH = cfg.MINIO_CHECKPOINT_PATH + "/stream_to_minio/"
OUTPUT_PATH = cfg.MINIO_RAW_PATH


def run_etl():
    stopper = GracefulStopper(logger=logger)
    
    # Đợi Topic trước khi khởi tạo Spark
    try:
        wait_for_topic(cfg.KAFKA_TOPIC, cfg.KAFKA_BOOTSTRAP_SERVERS, logger=logger)
    except Exception as e:
        logger.error("Dừng chương trình do không tìm thấy Kafka Topic.")
        return # Thoát luôn nếu không có topic

    logger.info(f"Khởi động Spark Streaming ETL...")
    
    # Khởi tạo Spark từ utils
    spark = get_spark_session(TASK_NAME)
    spark.sparkContext.setLogLevel("WARN")
    ensure_minio_bucket(cfg.MINIO_DATALAKE_BUCKET, logger=logger)

    # Sử dụng schema từ spark_schemas.py
    schema = SparkSchemas.music_log()

    # Đọc dữ liệu từ Kafka
    logger.info(f"Đang lắng nghe Kafka Topic '{cfg.KAFKA_TOPIC}'...")
    
    # Đọc stream từ Kafka
    kafka_df = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", cfg.KAFKA_BOOTSTRAP_SERVERS) \
        .option("subscribe", cfg.KAFKA_TOPIC) \
        .option("startingOffsets", "latest") \
        .option("failOnDataLoss", "false") \
        .load() # Thêm failOnDataLoss để an toàn hơn

    # Transform & Parse
    processed_df = kafka_df.selectExpr("CAST(value AS STRING)") \
        .select(from_json(col("value"), schema).alias("data")) \
        .select("data.*") \
        .withColumn("event_time", to_timestamp(col("timestamp") / 1000)) \
        .withColumn("date_str", date_format(col("event_time"), "yyyy-MM-dd"))

    # Ghi xuống MinIO 
    logger.info(f"Đang ghi xuống MinIO (Parquet)...")

    query = processed_df.writeStream \
        .format("parquet") \
        .option("path", OUTPUT_PATH) \
        .option("checkpointLocation", CHECKPOINT_PATH) \
        .partitionBy("date_str") \
        .outputMode("append") \
        .trigger(processingTime="1 minute") \
        .start()
    
    stopper.attach(query)
    
    try:
        query.awaitTermination()
    except Exception as e:
        logger.error(f"Streaming bị lỗi: {e}")
    finally:
        logger.info(f"Đang đóng Spark Session...")
        spark.stop()
        logger.info(f"ETL đã tắt thành công. Tài nguyên đã được giải phóng.")

if __name__ == "__main__":
    run_etl()