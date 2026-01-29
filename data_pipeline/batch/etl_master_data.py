"""
ETL Master Data - Songs Collection (MongoDB)
=============================================
Trích xuất danh sách bài hát từ Data và lưu vào MongoDB.
Schema: {_id, title, artist, artist_id, plays_cumulative, plays_7d}
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, sum, when, current_timestamp, date_sub, max, to_timestamp

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
    log("ETL Master Data - Songs Collection (Enhanced)")
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
        # Load parquet
        df = spark.read.parquet(DATA_PATH)
        
        # NOTE: Dữ liệu log của bạn có thể có trường 'timestamp' dạng chuỗi hoặc số
        # Cần convert sang TimestampType để tính toán date
        # Nếu đã là timestamp thì lệnh này không ảnh hưởng
        if "timestamp" in df.columns:
             # Giả sử timestamp đang chuẩn, nếu không cần cast
             pass 
        
        total_rows = df.count()
        log(f"Tổng số dòng: {total_rows:,}")
    except Exception as e:
        log(f"LỖI đọc dữ liệu: {e}")
        spark.stop()
        return

    # 3. Tính toán các metric
    log("Đang calculate metrics (Cumulative & 7 Days)...")
    
    # Xác định mốc thời gian "hiện tại" của Dataset (Max date trong data)
    # Vì data có thể là data cũ, dùng current_timestamp() thật của hệ thống sẽ sai nếu data từ năm 2012
    max_date_row = df.select(max(col("timestamp")).alias("max_ts")).collect()
    anchor_date = max_date_row[0]["max_ts"]
    
    log(f"Anchor Date (Last data point): {anchor_date}")
    
    # Tính ngày giới hạn (Anchor - 7 days)
    # Spark SQL function date_sub works on DateType/TimestampType
    
    
    # Sửa lại logic một chút để tối ưu performance hơn (tránh join chéo không cần thiết)
    from pyspark.sql.functions import lit, udf
    from pyspark.sql.types import StringType
    import hashlib
    import urllib.parse

    # Hàm tạo ảnh bìa (UDF)
    def generate_cover(title):
        if not title: return "https://ui-avatars.com/api/?name=Music&background=random"
        try:
            hash_object = hashlib.md5(title.encode('utf-8'))
            bg = hash_object.hexdigest()[:6]
            safe_name = urllib.parse.quote(title)
            return f"https://ui-avatars.com/api/?name={safe_name}&background={bg}&color=fff&size=512&length=1&bold=true&font-size=0.7"
        except:
            return "https://ui-avatars.com/api/?name=Music&background=random"

    generate_cover_udf = udf(generate_cover, StringType())
    FIXED_YOUTUBE_URL = "IDksv0Z-dTk"

    # Thêm cột anchor vào để so sánh
    df_with_flag = df.withColumn("anchor_ts", lit(anchor_date))
    
    songs_aggregated = df_with_flag.groupBy(
        col("musicbrainz_track_id").alias("_id"),
        col("track_name"),
        col("artist_name"),
        col("musicbrainz_artist_id").alias("artist_id"),
    ).agg(
        count("*").alias("plays_cumulative"),
        sum(when(col("timestamp") >= date_sub(col("anchor_ts"), 7), 1).otherwise(0)).alias("plays_7d")
    )

    # Thêm Metadata bổ sung (Image, URL)
    final_songs = songs_aggregated \
        .withColumn("image_url", generate_cover_udf(col("track_name"))) \
        .withColumn("url", lit(FIXED_YOUTUBE_URL)) \
        .withColumn("plays_cumulative", col("plays_cumulative").cast("string")) \
        .withColumn("plays_7d", col("plays_7d").cast("string"))

    total_unique = final_songs.count()
    log(f"Tổng số bài hát unique: {total_unique:,}")

    # 4. Lọc bài phổ biến (Dựa trên plays_cumulative)
    log(f"Lọc bài có >= {MIN_LISTEN_COUNT} lượt nghe...")
    popular_songs = final_songs.filter(col("plays_cumulative") >= MIN_LISTEN_COUNT)
    
    # 5. Giới hạn top N
    # after_filter không còn tồn tại, dùng count trực tiếp hoặc simple check
    if TOP_SONGS_LIMIT:
        log(f"Giới hạn top {TOP_SONGS_LIMIT:,}...")
        popular_songs = popular_songs.orderBy(col("plays_cumulative").desc()).limit(TOP_SONGS_LIMIT)
    
    final_count = popular_songs.count()
    log(f"Số bài cuối cùng sẽ Upsert: {final_count:,}")

    # 6. Ghi vào MongoDB (UPSERT MODE)
    # mode("append") kết hợp với cấu hình replaceDocument=false -> Thực hiện $set (Update nếu có, Insert nếu chưa)
    # Lưu ý: "spark.mongodb.output.uri" đã được set ở trên
    log("Đang Upsert vào MongoDB (Bảo toàn Lyrics)...")
    try:
        popular_songs.write \
            .format("mongo") \
            .mode("append") \
            .option("replaceDocument", "false") \
            .save()
        log(f"THÀNH CÔNG! Đã cập nhật {final_count:,} bài hát.")
    except Exception as e:
        log(f"LỖI ghi MongoDB: {e}")
    
    spark.stop()
    log("=" * 60)

if __name__ == "__main__":
    # Clear log file
    open("/tmp/etl_songs.log", "w").close()
    run_master_data_etl()
