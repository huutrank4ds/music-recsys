"""
Spark Streaming ETL - Kafka to MinIO
=====================================
Đọc dữ liệu từ Kafka topic và ghi xuống MinIO (Parquet).
"""

from pyspark.sql.functions import from_json, col, to_timestamp, date_format

# Import config và utils tập trung
import src.config as cfg
from src.schemas import get_music_log_schema
from src.utils import get_logger, get_spark_session

# Khởi tạo logger
logger = get_logger("Streaming_ETL")

def run_etl():
    logger.info("Khởi động Spark Streaming ETL...")
    
    # Khởi tạo Spark từ utils
    spark = get_spark_session("LastFm_Streaming_ETL")
    spark.sparkContext.setLogLevel("WARN")

    # Sử dụng schema từ schemas.py
    schema = get_music_log_schema()

    # Đọc dữ liệu từ Kafka
    logger.info(f"Đang lắng nghe Kafka Topic '{cfg.KAFKA_TOPIC}'...")
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
    logger.info("Đang ghi xuống MinIO (Parquet)...")
    
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

    query.awaitTermination()

if __name__ == "__main__":
    run_etl()
