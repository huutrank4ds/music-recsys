import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, first
from pyspark.sql.types import StructType, StructField, StringType, LongType

# Cấu hình Packages: Cần Mongo Connector và Hadoop AWS
PACKAGES = [
    "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0",
    "org.apache.hadoop:hadoop-aws:3.3.4",
    "org.mongodb.spark:mongo-spark-connector_2.12:10.3.0" # Compatible with Spark 3.5
]

def run_master_data_etl():
    print("Bắt đầu ETL Master Data (Collection: songs)...")
    
    # 1. Khởi tạo Spark
    spark = SparkSession.builder \
        .appName("ETL_Songs_Master") \
        .master("spark://spark-master:7077") \
        .config("spark.jars.packages", ",".join(PACKAGES)) \
        .config("spark.executor.memory", "1g") \
        .config("spark.executor.cores", "1") \
        .config("spark.cores.max", "1") \
        .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
        .config("spark.hadoop.fs.s3a.access.key", "minioadmin") \
        .config("spark.hadoop.fs.s3a.secret.key", "minioadmin") \
        .config("spark.hadoop.fs.s3a.path.style.access", "true") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .config("spark.mongodb.write.connection.uri", "mongodb://mongodb:27017/music_recsys.songs") \
        .getOrCreate()

    # 2. Đọc dữ liệu từ MinIO (Data Lake)
    # Lưu ý: Đọc toàn bộ thư mục raw để lấy tất cả bài hát từng xuất hiện
    print(">>> Đang đọc dữ liệu từ MinIO...")
    try:
        df = spark.read.parquet("s3a://datalake/raw/music_logs/")
    except Exception as e:
        print(f"Lỗi đọc MinIO (Có thể do chưa có data): {e}")
        spark.stop()
        return

    # 3. Transform (Chọn cột & Đổi tên)
    # Mapping theo Schema bạn yêu cầu
    songs_raw = df.select(
        col("musicbrainz_track_id").alias("_id"),      # Khóa chính
        col("track_name").alias("title"),              # Tên bài
        col("artist_name").alias("artist"),            # Nghệ sĩ
        col("track_index").cast(LongType()),           # Index (Long)
        # Tạo URL giả lập (Vì data gốc không có URL audio)
        lit("https://www.soundhelix.com/examples/mp3/SoundHelix-Song-1.mp3").alias("url") 
    )

    # 4. Deduplicate (Lọc trùng lặp)
    # Group by ID và lấy thông tin đầu tiên tìm thấy
    print(">>>  Đang lọc bài hát duy nhất...")
    songs_unique = songs_raw.groupBy("_id").agg(
        first("title").alias("title"),
        first("artist").alias("artist"),
        first("track_index").alias("track_index"),
        first("url").alias("url")
    )

    # 5. Load (Ghi vào MongoDB)
    print(">>> Đang ghi vào MongoDB...")
    songs_unique.write \
        .format("mongodb") \
        .mode("overwrite") \
        .option("database", "music_recsys") \
        .option("collection", "songs") \
        .save()

    print(f"THÀNH CÔNG! Đã lưu {songs_unique.count()} bài hát vào MongoDB.")
    spark.stop()

if __name__ == "__main__":
    run_master_data_etl()