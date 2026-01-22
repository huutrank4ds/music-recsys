from pathlib import Path
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.sql.types import StructType, StructField, StringType, LongType
import hashlib
import urllib.parse
from common.logger import get_logger

logger = get_logger("Extract_Songs_To_File")

# Hàm tạo URL ảnh bìa nhất quán dựa trên tên bài hát
def generate_consistent_cover_url(song_title: str) -> str:
    # Băm tên bài hát ra mã Hex (MD5) -> Lấy 6 ký tự đầu làm màu
    hash_object = hashlib.md5(song_title.encode())
    consistent_bg = hash_object.hexdigest()[:6] # Ví dụ: 5d4140

    safe_name = urllib.parse.quote(song_title)
    
    return f"https://ui-avatars.com/api/?name={safe_name}&background={consistent_bg}&color=fff&size=512&length=1&bold=true"

# Hàm quét và lấy danh sách file parquet hợp lệ
def get_valid_parquet_files(data_dir_path):
    """
    Quét file parquet sử dụng pathlib
    Input: data_dir_path (Path object hoặc string)
    Output: List các đường dẫn định dạng URI (file://...)
    """
    # Chuyển đổi sang Path object nếu đầu vào là string
    data_path = Path(data_dir_path)
    
    logger.info(f"Đang quét file trong: {data_path}")
    
    if not data_path.exists():
        logger.error(f"Thư mục không tồn tại: {data_path}")
        return []

    valid_files = [
        f"file://{f.resolve()}" 
        for f in data_path.glob("*.parquet") 
        if not f.name.startswith(('.', '_'))
    ]
    
    # Sắp xếp để đảm bảo thứ tự đọc nhất quán
    valid_files.sort()
    return valid_files

# Hàm chính
def main():
    # Sử dụng Path object cho đường dẫn đầu vào
    BASE_DIR = Path("/opt/data/processed_sorted")
    
    # Đường dẫn đầu ra Spark vẫn nên để dạng string URI chuẩn
    OUTPUT_DIR = "file:///opt/data/songs_master_list"

    input_files = get_valid_parquet_files(BASE_DIR)
    
    if not input_files:
        logger.error("Không tìm thấy file!")
        return

    logger.info(f"Tìm thấy {len(input_files)} file sạch.")

    logger.info("\nKhởi tạo Spark Session...")
    spark = SparkSession.builder \
        .appName("ExtractSongsFixedType") \
        .config("spark.driver.memory", "3g") \
        .getOrCreate()

    # Định nghĩa schema tĩnh để tránh lỗi schema inference
    song_schema = StructType([
        StructField("musicbrainz_track_id", StringType(), True),
        StructField("track_name", StringType(), True),
        StructField("musicbrainz_artist_id", StringType(), True),
        StructField("artist_name", StringType(), True),
        StructField("track_index", LongType(), True),
        StructField("artist_index", LongType(), True)
    ])

    try:
        logger.info("Đang đọc dữ liệu...")
        # Spark nhận list các đường dẫn string
        raw_df = spark.read.schema(song_schema).parquet(*input_files)
        
        logger.info("Đang xử lý ETL...")
        songs_df = raw_df.select(
            col("musicbrainz_track_id").alias("_id"),
            col("track_name"),
            col("musicbrainz_artist_id"),
            col("artist_name")
        ).dropDuplicates(["_id"])

        count = songs_df.count()
        logger.info(f"Tìm thấy tổng cộng: {count} bài hát duy nhất.")

        logger.info(f"Đang ghi file JSON vào: {OUTPUT_DIR}")
        
        # Ghi song song (Không dùng coalesce để tránh OOM)
        songs_df.write \
            .mode("overwrite") \
            .json(OUTPUT_DIR)

        logger.info("THÀNH CÔNG! Bây giờ bạn hãy kiểm tra thư mục data.")

    except Exception as e:
        logger.error(f"VẪN CÒN LỖI: {e}")
    finally:
        spark.stop()

if __name__ == "__main__":
    main()