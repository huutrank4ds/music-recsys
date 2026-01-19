"""
Spark Streaming ETL - Kafka to MinIO
=====================================
Đọc dữ liệu từ Kafka topic và ghi xuống MinIO (Parquet).
"""

import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, to_timestamp, date_format
from pyspark.sql.types import StructType, StructField, StringType, LongType

# Import config tập trung
import src.config as cfg
from src.schemas import get_music_log_schema

def run_etl():
    print("Khởi động Spark Streaming ETL...")
    
    # Không cần spark.jars.packages - Docker đã tích hợp sẵn
    spark = SparkSession.builder \
        .appName("LastFm_Streaming_ETL") \
        .master("spark://spark-master:7077") \
        .config("spark.hadoop.fs.s3a.endpoint", cfg.MINIO_ENDPOINT) \
        .config("spark.hadoop.fs.s3a.access.key", cfg.MINIO_ACCESS_KEY) \
        .config("spark.hadoop.fs.s3a.secret.key", cfg.MINIO_SECRET_KEY) \
        .config("spark.hadoop.fs.s3a.path.style.access", "true") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false") \
        .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    # Sử dụng schema từ schemas.py
    schema = get_music_log_schema()

    # Đọc dữ liệu từ Kafka
    print(f"Đang lắng nghe Kafka Topic '{cfg.KAFKA_TOPIC}'...")
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
    print("Đang ghi xuống MinIO (Parquet)...")
    
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