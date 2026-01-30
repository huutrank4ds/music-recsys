# /opt/src/scripts/extract_songs_to_file.py
"""
Khởi tạo dữ liệu bài hát master từ các file Parquet đã xử lý.
1. Đọc các file Parquet từ thư mục dữ liệu đã xử lý.
2. Chọn lọc các cột cần thiết và loại bỏ bản ghi trùng.
3. Tạo các cột Metadata giả lập (ảnh bìa, URL, lượt nghe...).
4. Ghi dữ liệu ra file JSON line.
"""
from pathlib import Path
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, rand, udf
from pyspark.sql.types import StringType
import hashlib
import urllib.parse
from common.logger import get_logger
from common.schemas.spark_schemas import SparkSchemas

logger = get_logger("Extract_Master_Data")
FIXED_YOUTUBE_URL = "JfVos4VSpmA" 

# Hàm tạo ảnh bìa
def _generate_cover_logic(song_title):
    if not song_title:
        return "https://ui-avatars.com/api/?name=Unknown&background=random"
    
    # Băm tên bài hát ra mã Hex để lấy màu cố định
    hash_object = hashlib.md5(song_title.encode())
    consistent_bg = hash_object.hexdigest()[:6] 

    safe_name = urllib.parse.quote(song_title)
    return f"https://ui-avatars.com/api/?name={safe_name}&background={consistent_bg}&color=fff&size=512&length=1&bold=true"

# Hàm quét file (Giữ nguyên)
def get_valid_parquet_files(data_dir_path):
    data_path = Path(data_dir_path)
    if not data_path.exists():
        logger.error(f"Thư mục không tồn tại: {data_path}")
        return []
    valid_files = [f"file://{f.resolve()}" for f in data_path.glob("*.parquet") if not f.name.startswith(('.', '_'))]
    valid_files.sort()
    return valid_files

# ================= MAIN =================
def main():
    BASE_DIR = Path("/opt/data/processed_sorted")
    OUTPUT_DIR = "file:///opt/data/songs_master_list"

    # Setup Spark
    spark = SparkSession.builder \
        .appName("Init_Master_Song_Data") \
        .config("spark.driver.memory", "2g") \
        .getOrCreate()

    try:
        # Lấy danh sách file
        input_files = get_valid_parquet_files(BASE_DIR)
        if not input_files: return

        logger.info(f"[Extract] Đang đọc {len(input_files)} file Parquet...")
        
        # Đọc dữ liệu thô
        raw_df = spark.read.parquet(*input_files)

        # Chọn cột và đổi tên cho khớp Schema đích
        df_clean = raw_df.select(
            col("musicbrainz_track_id").alias("_id"),
            col("track_name"),
            col("musicbrainz_artist_id").alias("artist_id"),
            col("artist_name")
        ).dropDuplicates(["_id"])
        count = df_clean.count()
        logger.info(f"[Extract] Tổng số bài hát Unique: {count}")

        # Tạo các cột Metadata giả lập
        logger.info("[Extract] Đang tạo Metadata giả lập...")
        cover_udf = udf(_generate_cover_logic, StringType())

        df_enriched = df_clean \
            .withColumn("image_url", cover_udf(col("track_name"))) \
            .withColumn("url", lit(FIXED_YOUTUBE_URL)) \
            .withColumn("plays_7d", lit(0).cast("int")) \
            .withColumn("plays_cumulative", lit(0).cast("long")) \
            .withColumn("duration_ms", lit(300000).cast("int")) 

        # Sắp xếp lại cột cho đúng chuẩn Schema
        target_schema = SparkSchemas.song_master()
        final_columns = [field.name for field in target_schema.fields]
        df_final = df_enriched.select(*final_columns)

        # Ghi ra JSON
        logger.info(f"[Extract] Đang ghi file JSON xuống: {OUTPUT_DIR}")
        
        df_final.write \
            .mode("overwrite") \
            .json(OUTPUT_DIR)

        logger.info("[Extract] KHỞI TẠO DỮ LIỆU THÀNH CÔNG!")

    except Exception as e:
        logger.error(f"Lỗi: {e}")
        import traceback
        traceback.print_exc()
    finally:
        spark.stop()

if __name__ == "__main__":
    main()