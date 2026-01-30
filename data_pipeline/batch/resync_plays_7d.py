# src/batch/resync_plays_7d.py
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from datetime import datetime, timedelta
from pymongo import UpdateOne, MongoClient # type: ignore
import config as cfg
from utils import get_spark_session, get_mongo_db_client
from common.logger import get_logger

logger = get_logger("Resync Plays7D Job")


def resync_trending_7d():

    spark = get_spark_session("ResyncPlays7DJob")
    client, db = get_mongo_db_client()

    # 1. Tính toán mốc 7 ngày trước
    seven_days_ago = (datetime.now() - timedelta(days=7)).strftime("%Y-%m-%d")
    # 2. Đọc dữ liệu 7 ngày gần nhất (Sử dụng Partition Pruning cực nhanh)
    df_7d = spark.read.parquet(cfg.MINIO_RAW_PATH).filter(F.col("date_str") >= seven_days_ago)
    
    logger.info(f"Đang tính toán lại tổng lượt nghe trong 7 ngày gần nhất từ mốc {seven_days_ago}...")
    try:
        # 3. Tính toán tổng lượt nghe chính xác
        true_counts = df_7d.filter(F.col("action").isin(["listen", "complete"]))
    except Exception as e:
        logger.error(f"Lỗi khi tính toán tổng lượt nghe 7 ngày: {e}")
        return
    
    if true_counts.head(1):
        true_counts = true_counts.groupBy("track_id").count().collect()
        operations = []
        for row in true_counts:
            operations.append(
                UpdateOne(
                    {"_id": row["track_id"]},
                    {"$set": {"plays_7d": row["count"]}},
                    upsert=True
                )
            )
        
        # Gửi lệnh xuống MongoDB
        if operations:
            db["songs"].bulk_write(operations)
    else:
        logger.info("Không có bản ghi nào trong 7 ngày gần nhất để cập nhật.")

if __name__ == "__main__":
    resync_trending_7d()