"""
ETL Master Data - Songs Collection (MongoDB)
=============================================
Trích xuất danh sách bài hát từ Data và lưu vào MongoDB.
Schema: {_id, title, artist, artist_id, listen_count}
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count

# ============================================================
# CONFIGURATION
# ============================================================
MONGO_URI = "mongodb://mongodb:27017/music_recsys.songs"
DATA_PATH = "/opt/data/processed_sorted"
MIN_LISTEN_COUNT = 3
TOP_SONGS_LIMIT = 200000

def log(msg):
    """Log to both console and file"""
    print(msg, flush=True)
    with open("/tmp/etl_songs.log", "a") as f:
        f.write(f"{msg}\n")

def run_master_data_etl():
    log("=" * 60)
    log("ETL Master Data - Songs Collection")
    log("=" * 60)
    
    # 1. Khởi tạo Spark với MongoDB config
    log("Đang khởi tạo Spark Session...")
    spark = SparkSession.builder \
        .appName("ETL_Songs_Master") \
        .config("spark.mongodb.output.uri", MONGO_URI) \
        .getOrCreate()

    # 2. Đọc dữ liệu gốc (có đầy đủ metadata)
    log(f"Đang đọc từ: {DATA_PATH}")
    try:
        df = spark.read.parquet(DATA_PATH)
        total_rows = df.count()
        log(f"Tổng số dòng: {total_rows:,}")
    except Exception as e:
        log(f"LỖI đọc dữ liệu: {e}")
        spark.stop()
        return

    # 3. Đếm số lượt nghe mỗi bài hát
    log("Đang aggregate...")
    songs_with_count = df.groupBy(
        col("musicbrainz_track_id").alias("_id"),
        col("track_name").alias("title"),
        col("artist_name").alias("artist"),
        col("musicbrainz_artist_id").alias("artist_id"),
    ).agg(
        count("*").alias("listen_count")
    )
    
    total_unique = songs_with_count.count()
    log(f"Tổng số bài hát unique: {total_unique:,}")

    # 4. Lọc bài phổ biến
    log(f"Lọc bài có >= {MIN_LISTEN_COUNT} lượt nghe...")
    popular_songs = songs_with_count.filter(col("listen_count") >= MIN_LISTEN_COUNT)
    after_filter = popular_songs.count()
    log(f"Sau lọc: {after_filter:,} bài")

    # 5. Giới hạn top N
    if TOP_SONGS_LIMIT and after_filter > TOP_SONGS_LIMIT:
        log(f"Giới hạn top {TOP_SONGS_LIMIT:,}...")
        final_songs = popular_songs.orderBy(col("listen_count").desc()).limit(TOP_SONGS_LIMIT)
    else:
        final_songs = popular_songs
    
    final_count = final_songs.count()
    log(f"Số bài cuối cùng: {final_count:,}")

    # 6. Ghi vào MongoDB
    log("Đang ghi vào MongoDB...")
    try:
        final_songs.write \
            .format("mongo") \
            .mode("overwrite") \
            .save()
        log(f"THÀNH CÔNG! Đã lưu {final_count:,} bài hát.")
    except Exception as e:
        log(f"LỖI ghi MongoDB: {e}")
    
    spark.stop()
    log("=" * 60)

if __name__ == "__main__":
    # Clear log file
    open("/tmp/etl_songs.log", "w").close()
    run_master_data_etl()
