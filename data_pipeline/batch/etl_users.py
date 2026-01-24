"""
ETL Users Collection - MongoDB
==============================
Trích xuất danh sách users duy nhất và lưu vào MongoDB.
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, first
from pyspark.sql.types import ArrayType, FloatType

# ============================================================
# CONFIGURATION
# ============================================================
MONGO_URI = "mongodb://mongodb:27017/music_recsys.users"
DATA_PATH = "/opt/data/processed_sorted"

def log(msg):
    """Log to both console and file"""
    print(msg, flush=True)
    with open("/tmp/etl_users.log", "a") as f:
        f.write(f"{msg}\n")

def run_users_etl():
    log("=" * 60)
    log("ETL Users Collection")
    log("=" * 60)
    
    # 1. Khởi tạo Spark với MongoDB config
    log("Đang khởi tạo Spark Session...")
    spark = SparkSession.builder \
        .appName("ETL_Users_Master") \
        .config("spark.mongodb.output.uri", MONGO_URI) \
        .getOrCreate()

    # 2. Đọc dữ liệu gốc
    log(f"Đang đọc từ: {DATA_PATH}")
    try:
        df = spark.read.parquet(DATA_PATH)
        total_rows = df.count()
        log(f"Tổng số dòng: {total_rows:,}")
    except Exception as e:
        log(f"LỖI đọc dữ liệu: {e}")
        spark.stop()
        return

    # 3. Lấy danh sách users duy nhất với thông tin profile
    log("Đang lọc users duy nhất với profile...")
    users_unique = df.groupBy("user_id").agg(
        first("gender").alias("gender"),
        first("age").alias("age"),
        first("country").alias("country")
    ).select(
        col("user_id").alias("_id"),
        col("user_id").alias("username"),
        col("gender"),
        col("age"),
        col("country"),
        lit(None).cast(ArrayType(FloatType())).alias("latent_vector")
    )
    
    user_count = users_unique.count()
    log(f"Tổng số users: {user_count:,}")

    # 4. Ghi vào MongoDB
    log("Đang ghi vào MongoDB...")
    try:
        users_unique.write \
            .format("mongo") \
            .mode("overwrite") \
            .save()
        log(f"THÀNH CÔNG! Đã lưu {user_count:,} users.")
    except Exception as e:
        log(f"LỖI ghi MongoDB: {e}")
    
    spark.stop()
    log("=" * 60)

if __name__ == "__main__":
    # Clear log file
    open("/tmp/etl_users.log", "w").close()
    run_users_etl()
