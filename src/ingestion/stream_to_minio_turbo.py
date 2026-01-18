"""
TURBO STREAMING - Ghi dữ liệu vào MinIO với tốc độ tối đa
"""
import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, to_timestamp, date_format
from pyspark.sql.types import StructType, StructField, StringType, LongType

# Cấu hình Packages
PACKAGES = [
    "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0",
    "org.apache.hadoop:hadoop-aws:3.3.4"
]

def run_etl():
    print("Khởi động Spark Streaming TURBO ETL...")
    spark = SparkSession.builder \
        .appName("LastFm_Streaming_TURBO_ETL") \
        .master("spark://spark-master:7077") \
        .config("spark.jars.packages", ",".join(PACKAGES)) \
        .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
        .config("spark.hadoop.fs.s3a.access.key", "minioadmin") \
        .config("spark.hadoop.fs.s3a.secret.key", "minioadmin") \
        .config("spark.hadoop.fs.s3a.path.style.access", "true") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false") \
        .config("spark.hadoop.fs.s3a.fast.upload", "true") \
        .config("spark.hadoop.fs.s3a.fast.upload.buffer", "bytebuffer") \
        .config("spark.hadoop.fs.s3a.multipart.size", "104857600") \
        .config("spark.sql.shuffle.partitions", "8") \
        .config("spark.streaming.kafka.consumer.cache.enabled", "false") \
        .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    # ĐỊNH NGHĨA SCHEMA 
    schema = StructType([
        StructField("user_id", StringType(), True),
        StructField("timestamp", StringType(), True),
        StructField("musicbrainz_artist_id", StringType(), True),
        StructField("artist_name", StringType(), True),
        StructField("artist_index", LongType(), True), 
        StructField("musicbrainz_track_id", StringType(), True),
        StructField("track_name", StringType(), True),
        StructField("track_index", LongType(), True)   
    ])

    # Đọc dữ liệu từ Kafka với fetch size lớn hơn
    print("Đang lắng nghe Kafka Topic 'music_log' với TURBO settings...")
    kafka_df = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "kafka:9092") \
        .option("subscribe", "music_log") \
        .option("startingOffsets", "earliest") \
        .option("maxOffsetsPerTrigger", 500000) \
        .option("kafka.fetch.min.bytes", "1048576") \
        .option("kafka.fetch.max.wait.ms", "500") \
        .load()

    # Transform & Parse
    processed_df = kafka_df.selectExpr("CAST(value AS STRING)") \
        .select(from_json(col("value"), schema).alias("data")) \
        .select("data.*") \
        .withColumn("event_time", to_timestamp(col("timestamp"))) \
        .withColumn("date_str", date_format(col("event_time"), "yyyy-MM-dd"))

    # Ghi xuống MinIO MỖI 10 GIÂY (thay vì 1 phút)
    print("Đang ghi xuống MinIO (Parquet) - TURBO MODE (10s trigger)...")
    
    checkpoint_path = "s3a://datalake/checkpoints/music_logs_turbo/"
    output_path = "s3a://datalake/raw/music_logs/"

    query = processed_df.writeStream \
        .format("parquet") \
        .option("path", output_path) \
        .option("checkpointLocation", checkpoint_path) \
        .partitionBy("date_str") \
        .outputMode("append") \
        .trigger(processingTime="10 seconds") \
        .start()

    query.awaitTermination()

if __name__ == "__main__":
    run_etl()
