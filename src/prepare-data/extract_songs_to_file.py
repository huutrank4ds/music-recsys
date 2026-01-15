from pathlib import Path
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.sql.types import StructType, StructField, StringType, LongType

# ================= 1. HÀM TIỆN ÍCH =================
def get_valid_parquet_files(data_dir_path):
    """
    Quét file parquet sử dụng pathlib
    Input: data_dir_path (Path object hoặc string)
    Output: List các đường dẫn định dạng URI (file://...)
    """
    # Chuyển đổi sang Path object nếu đầu vào là string
    data_path = Path(data_dir_path)
    
    print(f"🔍 Đang quét file trong: {data_path}")
    
    if not data_path.exists():
        print(f"❌ Thư mục không tồn tại: {data_path}")
        return []

    # Sử dụng pathlib để glob và filter
    # f.name: tên file (vd: part-0000.parquet)
    # f.resolve(): đường dẫn tuyệt đối (vd: /opt/data/...)
    valid_files = [
        f"file://{f.resolve()}" 
        for f in data_path.glob("*.parquet") 
        if not f.name.startswith(('.', '_'))
    ]
    
    # Sắp xếp để đảm bảo thứ tự đọc nhất quán
    valid_files.sort()
    return valid_files

# ================= 2. HÀM CHÍNH =================
def main():
    # Sử dụng Path object cho đường dẫn đầu vào
    BASE_DIR = Path("/opt/data/processed_sorted")
    
    # Đường dẫn đầu ra Spark vẫn nên để dạng string URI chuẩn
    OUTPUT_DIR = "file:///opt/data/songs_master_list"

    input_files = get_valid_parquet_files(BASE_DIR)
    
    if not input_files:
        print("❌ Không tìm thấy file!")
        return

    print(f"✅ Tìm thấy {len(input_files)} file sạch.")

    print("\n🚀 Khởi tạo Spark Session...")
    spark = SparkSession.builder \
        .appName("ExtractSongsFixedType") \
        .config("spark.driver.memory", "3g") \
        .getOrCreate()

    # Schema giữ nguyên
    song_schema = StructType([
        StructField("musicbrainz_track_id", StringType(), True),
        StructField("track_name", StringType(), True),
        StructField("musicbrainz_artist_id", StringType(), True),
        StructField("artist_name", StringType(), True),
        StructField("track_index", LongType(), True),
        StructField("artist_index", LongType(), True)
    ])

    try:
        print("📖 Đang đọc dữ liệu...")
        # Spark nhận list các đường dẫn string
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
        
        # Ghi song song (Không dùng coalesce để tránh OOM)
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