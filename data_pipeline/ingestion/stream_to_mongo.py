"""
Spark Streaming ETL - Kafka to MongoDB
=====================================
Đọc dữ liệu từ Kafka topic và ghi xuống MongoDB.
"""

from pyspark.sql.functions import from_json, col
from pymongo import MongoClient, UpdateOne #type: ignore
from common.schemas.spark_schemas import SparkSchemas
from common.logger import get_logger
from utils import get_spark_session, GracefulStopper, wait_for_topic
import config as cfg

TASK_NAME = "Stream to Mongo"
logger = get_logger(TASK_NAME)

CHECKPOINT_PATH = cfg.MINIO_CHECKPOINT_PATH + "/stream_to_mongo/"

def update_mongo(batch_df, batch_id):
    # Chỉ lấy lượt nghe complete hoặc listen
    valid_plays = batch_df.filter((col("action") == "complete") | (col("action") == "listen"))
    
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
            client[cfg.MONGO_DB][cfg.MONGO_SONGS_COLLECTION].bulk_write(mongo_ops)
            logger.info(f"Batch {batch_id}: Updated {len(mongo_ops)} songs.")

def run_etl():
    stopper = GracefulStopper(logger=logger)

    try:
        wait_for_topic(cfg.KAFKA_TOPIC, cfg.KAFKA_BOOTSTRAP_SERVERS, logger=logger)
    except Exception as e:
        logger.error("Dừng chương trình do không tìm thấy Kafka Topic.")
        return

    logger.info(f"Khởi động Spark Streaming ETL...")
    spark = get_spark_session(TASK_NAME)
    spark.sparkContext.setLogLevel("WARN")

    # 1. Đọc stream từ Kafka
    df_kafka = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", cfg.KAFKA_BOOTSTRAP_SERVERS) \
        .option("subscribe", cfg.KAFKA_TOPIC) \
        .option("startingOffsets", "latest") \
        .option("failOnDataLoss", "false") \
        .load() 

    schema = SparkSchemas.music_log()
    df_parsed = df_kafka.select(from_json(col("value").cast("string"), schema).alias("data")).select("data.*")

    # 2. Ghi Mongo qua foreachBatch
    query = df_parsed.writeStream \
        .foreachBatch(update_mongo) \
        .option("checkpointLocation", CHECKPOINT_PATH) \
        .outputMode("update") \
        .trigger(processingTime="10 seconds") \
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