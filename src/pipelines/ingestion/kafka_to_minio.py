from pyspark.sql.functions import from_json, col, to_timestamp, date_format
import src.utils as utils
from src.schemas import get_music_log_schema
import src.configs as cfg


def run_etl():
    print("Khởi động Spark Streaming ETL...")
    spark = utils.get_spark_session("LastFm_Streaming_ETL")
    spark.sparkContext.setLogLevel("WARN")

    # ĐỊNH NGHĨA SCHEMA 
    schema = get_music_log_schema()

    # Kiểm tra và tạo bucket trên MinIO nếu chưa có
    utils.ensure_minio_bucket(cfg.DATALAKE_BUCKET)

    # Đọc dữ liệu từ Kafka
    print("Đang lắng nghe Kafka Topic 'music_log'...")
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

    #  Ghi xuống MinIO (Parquet)
    print("Đang ghi xuống MinIO (Parquet)...")
    
    checkpoint_path = cfg.MINIO_CHECKPOINT_PATH
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