import pyarrow.parquet as pq
from pathlib import Path

# Đường dẫn thư mục dữ liệu
DATA_DIR = Path('/opt/data/')

def check_timestamp_type():
    print(f"🔍 Đang kiểm tra metadata của các file trong: {DATA_DIR}")
    print("-" * 60)
    print(f"{'TÊN FILE':<40} | {'KIỂU DỮ LIỆU TIMESTAMP'}")
    print("-" * 60)

    # Lấy danh sách tất cả file parquet
    files = sorted(list(DATA_DIR.glob("*.parquet")))
    
    error_count = 0
    ok_count = 0

    for file_path in files:
        try:
            # Mở file chế độ đọc Metadata (không tốn RAM)
            parquet_file = pq.ParquetFile(file_path)
            schema = parquet_file.schema.to_arrow_schema()
            
            # Tìm cột timestamp
            idx = schema.get_field_index('timestamp')
            
            if idx != -1:
                # Lấy kiểu dữ liệu thực tế
                ts_type = schema.field(idx).type
                
                # Kiểm tra xem có phải nanoseconds (ns) không
                # Spark ghét 'ns', chỉ thích 'us' hoặc 'ms'
                if 'ns' in str(ts_type):
                    status = "❌ LỖI (NANOS)"
                    error_count += 1
                else:
                    status = "✅ OK"
                    ok_count += 1
                
                print(f"{file_path.name:<40} | {str(ts_type)}  {status}")
            else:
                print(f"{file_path.name:<40} | ⚠️ Không có cột timestamp")

        except Exception as e:
            print(f"{file_path.name:<40} | 💀 File hỏng/Không đọc được")

    print("-" * 60)
    print(f"TỔNG KẾT: {ok_count} file OK, {error_count} file LỖI.")
    
    if error_count > 0:
        print("\n🚨 KẾT LUẬN: Spark vẫn chết là do các file có dấu ❌ ở trên.")
        print("👉 Bạn cần xóa hoặc sửa lại chính xác các file đó.")
    else:
        print("\n🎉 KẾT LUẬN: Tất cả file đều sạch. Nếu Spark vẫn lỗi, hãy kiểm tra lại đường dẫn đọc file.")

if __name__ == "__main__":
    check_timestamp_type()