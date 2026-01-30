# jobs/incremental_update_job.py
from pyspark.sql import functions as F
from datetime import datetime
from pymongo import UpdateOne # type: ignore
import config as cfg
from utils import get_spark_session, get_mongo_db_client
from common.logger import get_logger

MONGO_URI = cfg.MONGO_URI
DB_NAME = cfg.MONGO_DB
DATA_PATH = cfg.MINIO_RAW_PATH
logger = get_logger("Incremental Listen Count Job")

def get_last_processed_time(db):
    """Lấy mốc thời gian cuối cùng đã xử lý từ MongoDB"""
    # Tìm mốc thời gian của job 'incremental_counts'
    meta = db["job_metadata"].find_one({"job_name": "incremental_counts"})
    if meta:
        return meta["last_timestamp"]
    return 0 # Nếu chạy lần đầu, trả về timestamp 0

def run_incremental_job():
    spark = get_spark_session("IncrementalListenCountJob")
    client, db = get_mongo_db_client()

    last_ts = get_last_processed_time(db)
    last_dt = datetime.fromtimestamp(last_ts / 1000.0)
    last_date_str = last_dt.strftime("%Y-%m-%d")

    logger.info(f"Đang quét dữ liệu mới từ sau mốc: {last_dt}")
    try:
        # Đọc dữ liệu với Partition Pruning
        raw_df = spark.read.parquet(DATA_PATH) \
                      .filter(F.col("date_str") >= last_date_str) \
                      .filter(F.col("timestamp") > last_ts) # Lọc timestamp ngay tại Spark
    except Exception as e:
        logger.error(f"Lỗi khi đọc dữ liệu từ MinIO: {e}")
        return

    # Kiểm tra xem thực sự có bản ghi nào mới không (bao gồm cả skip)
    if raw_df.head(1):
        # 1. Lấy max_ts chuẩn từ mọi loại action để làm mốc Checkpoint chính xác
        max_ts = raw_df.agg(F.max("timestamp")).collect()[0][0]

        # 2. Lọc lấy dữ liệu để tính count
        new_counts_df = raw_df.filter(F.col("action").isin(["listen", "complete"]))

        if new_counts_df.head(1):
            counts = new_counts_df.groupBy("track_id").count().collect()
            
            # 3. Sử dụng Bulk Write để cập nhật hàng loạt vào MongoDB
            operations = []
            for row in counts:
                operations.append(
                    UpdateOne(
                        {"_id": row["track_id"]},
                        {"$inc": {
                            "plays_7d": row["count"], 
                            "plays_cummulative": row["count"]
                        }},
                        upsert=True
                    )
                )
            
            if operations:
                db["songs"].bulk_write(operations, ordered=False)
                logger.info(f"Đã cập nhật lượt nghe cho {len(operations)} bài hát.")

        # 4. Luôn cập nhật mốc thời gian mới nhất (dù mẻ này chỉ toàn là 'skip')
        db["job_metadata"].update_one(
            {"job_name": "incremental_counts"},
            {"$set": {"last_timestamp": max_ts}},
            upsert=True
        )
        logger.info(f"Checkpoint thành công. Mốc mới: {datetime.fromtimestamp(max_ts/1000.0)}")
    else:
        logger.info("Không có dữ liệu mới.")

    client.close()

if __name__ == "__main__":
    run_incremental_job()