from pyspark.sql import SparkSession
from pyspark.sql.functions import col
import os

# --- CẤU HÌNH ---
# Đọc từ thư mục đã được làm sạch (QUAN TRỌNG)
INPUT_PATH = "/opt/data_clean/*.parquet" 
# Ghi ra thư mục kết quả
OUTPUT_PATH = "/opt/data/processed_sorted"

def process_data():
    # 1. Khởi tạo Spark
    print("🚀 Khởi động Spark Session...")
    spark = SparkSession.builder \
        .appName("SortLastFM") \
        .config("spark.driver.memory", "2g") \
        .getOrCreate()
    
    # Giảm bớt log rác để dễ nhìn lỗi nếu có
    spark.sparkContext.setLogLevel("ERROR")

    print(f"⏳ Đang đọc dữ liệu sạch từ: {INPUT_PATH}")
    
    try:
        # 2. Đọc dữ liệu
        df = spark.read.parquet(INPUT_PATH)
        
        # In ra schema để kiểm tra chắc chắn timestamp là timestamp (không phải long/int)
        print("📊 Schema dữ liệu đầu vào:")
        df.printSchema()
        
        # Đếm sơ bộ
        count = df.count()
        print(f"   -> Tổng số dòng: {count:,}")

        # 3. Sắp xếp toàn cục (Global Sort)
        print("⏳ Đang sắp xếp theo thời gian (timestamp ASC)...")
        sorted_df = df.orderBy(col("timestamp").asc())

        # 4. Ghi ra đĩa
        print(f"💾 Đang ghi dữ liệu đã sắp xếp ra: {OUTPUT_PATH}")
        
        # coalesce(1): Gom lại thành 1 file duy nhất để Producer đọc cho dễ
        sorted_df.coalesce(1).write.mode("overwrite").parquet(OUTPUT_PATH)
        
        print("✅ THÀNH CÔNG! Dữ liệu đã sẵn sàng để bắn vào Kafka.")
        
    except Exception as e:
        print("❌ LỖI RỒI:")
        print(e)
        
    finally:
        spark.stop()

if __name__ == "__main__":
    process_data()