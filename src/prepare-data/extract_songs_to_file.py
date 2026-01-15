import os
import glob
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
# ⚠️ THAY ĐỔI QUAN TRỌNG: Thêm LongType
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, LongType 

# ================= 1. HÀM TIỆN ÍCH =================
def get_valid_parquet_files(data_dir):
    print(f"🔍 Đang quét file trong: {data_dir}")
    if not os.path.exists(data_dir):
        print(f"❌ Thư mục không tồn tại: {data_dir}")
        return []

    all_files = glob.glob(os.path.join(data_dir, "*.parquet"))
    valid_files = []
    for f in all_files:
        filename = os.path.basename(f)
        if filename.startswith('.') or filename.startswith('_'):
            continue
        valid_files.append(f"file://{f}")
    
    valid_files.sort()
    return valid_files

# ================= 2. HÀM CHÍNH =================
def main():
    BASE_DIR = "/opt/data/processed_sorted"
    OUTPUT_DIR = "file:///opt/data/songs_master_list"

    input_files = get_valid_parquet_files(BASE_DIR)
    if not input_files:
        print("❌ Không tìm thấy file!")
        return

    print(f"✅ Tìm thấy {len(input_files)} file sạch.")

    print("\n🚀 Khởi tạo Spark Session...")
    spark = SparkSession.builder \
        .appName("ExtractSongsFixedType") \
        .config("spark.driver.memory", "2g") \
        .getOrCreate()

    # ⚠️ SỬA LỖI TẠI ĐÂY: Dùng LongType cho các trường Index
    song_schema = StructType([
        StructField("musicbrainz_track_id", StringType(), True),
        StructField("track_name", StringType(), True),
        StructField("musicbrainz_artist_id", StringType(), True),
        StructField("artist_name", StringType(), True),
        StructField("track_index", LongType(), True),   # <--- Đã sửa thành LongType
        StructField("artist_index", LongType(), True)   # <--- Đã sửa thành LongType
    ])

    try:
        print("📖 Đang đọc dữ liệu...")
        raw_df = spark.read.schema(song_schema).parquet(*input_files)
        
        print("🔄 Đang xử lý ETL...")
        songs_df = raw_df.select(
            col("musicbrainz_track_id").alias("id"),
            col("track_name"),
            col("musicbrainz_artist_id"),
            col("artist_name"),
            col("track_index"),
            col("artist_index")
        ).dropDuplicates(["id"])

        count = songs_df.count()
        print(f"🎵 Tìm thấy tổng cộng: {count} bài hát duy nhất.")

        print(f"💾 Đang ghi file JSON vào: {OUTPUT_DIR}")
        
        # Ghi đè (overwrite) để xóa dữ liệu lỗi cũ nếu có
        songs_df.write \
            .mode("overwrite") \
            .json(OUTPUT_DIR)

        print("✅ THÀNH CÔNG! Bây giờ bạn hãy kiểm tra thư mục data.")

    except Exception as e:
        print(f"💥 VẪN CÒN LỖI: {e}")
    finally:
        spark.stop()

if __name__ == "__main__":
    main()