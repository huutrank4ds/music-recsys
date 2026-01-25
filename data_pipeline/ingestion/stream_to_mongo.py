"""
Spark Streaming ETL - Kafka to MongoDB
=====================================
Đọc dữ liệu từ Kafka topic và ghi xuống MongoDB.
"""

from pyspark.sql.functions import from_json, col
from pymongo import MongoClient, UpdateOne #type: ignore
from common.spark_schemas import get_music_log_schema
from common.logger import get_logger
from utils import get_spark_session, GracefulStopper
import config as cfg

TASK_NAME = "Stream_to_Mongo"
logger = get_logger(TASK_NAME)

def update_mongo(batch_df, batch_id):
    # Chỉ lấy lượt nghe complete
    valid_plays = batch_df.filter(col("action") == "complete")
    
    if valid_plays.isEmpty(): return
    agg_counts = valid_plays.groupBy("track_id").count().collect()
    
    mongo_ops = []
    for row in agg_counts:
        mongo_ops.append(UpdateOne(
            {"_id": row['track_id']},
            {"$inc": {"plays_cumulative": row['count'], "plays_7d": row['count']}}
        ))
    
    if mongo_ops:
        with MongoClient(cfg.MONGO_URI) as client:
            client[cfg.MONGO_DB][cfg.COLLECTION_SONGS].bulk_write(mongo_ops)
            logger.info(f"[{TASK_NAME}] Batch {batch_id}: Updated {len(mongo_ops)} songs.")

def run_etl():
    stopper = GracefulStopper(logger=logger)

    logger.info(f"[{TASK_NAME}] Khởi động Spark Streaming ETL...")
    spark = get_spark_session(TASK_NAME)
    spark.sparkContext.setLogLevel("WARN")

    # 1. Đọc stream từ Kafka
    df_kafka = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", cfg.KAFKA_BOOTSTRAP_SERVERS) \
        .option("subscribe", cfg.KAFKA_TOPIC) \
        .load()

    schema = get_music_log_schema()
    df_parsed = df_kafka.select(from_json(col("value").cast("string"), schema).alias("data")).select("data.*")
    checkpoint_path = f"s3a://{cfg.DATALAKE_BUCKET}/checkpoints/stream_to_mongo/"

    # 2. Ghi Mongo qua foreachBatch
    query = df_parsed.writeStream \
        .foreachBatch(update_mongo) \
        .option("checkpointLocation", checkpoint_path) \
        .outputMode("update") \
        .trigger(processingTime="10 seconds") \
        .start()

    try:
        query.awaitTermination()
    except Exception as e:
        logger.error(f"[{TASK_NAME}] Streaming bị lỗi: {e}")
    finally:
        logger.info(f"[{TASK_NAME}] Đang đóng Spark Session...")
        spark.stop()
        logger.info(f"[{TASK_NAME}] ETL đã tắt thành công. Tài nguyên đã được giải phóng.")
if __name__ == "__main__":
    run_etl()