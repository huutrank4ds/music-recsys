import pyarrow.parquet as pq
import json
import glob
import os
import shutil

# Cấu hình
INPUT_DIR = "/opt/data/processed_sorted"
OUTPUT_DIR = "/opt/data/songs_master_list"
OUTPUT_FILE = f"{OUTPUT_DIR}/songs.json"
BATCH_SIZE = 50000  # Xử lý 50.000 dòng mỗi lần (Rất an toàn cho RAM)

def main():
    print(f"🚀 Bắt đầu xử lý 15 triệu dòng log (Batch Size: {BATCH_SIZE})...")
    
    # 1. Tìm file input
    files = glob.glob(f"{INPUT_DIR}/*.parquet")
    if not files:
        print("❌ Không tìm thấy file input.")
        return
    
    # 2. Reset output
    if os.path.exists(OUTPUT_DIR):
        shutil.rmtree(OUTPUT_DIR)
    os.makedirs(OUTPUT_DIR, exist_ok=True)

    # 3. Bộ nhớ đệm Global để khử trùng lặp
    # Lưu 1 triệu ID bài hát (dạng chuỗi) chỉ tốn khoảng 50MB - 100MB RAM -> An toàn.
    seen_ids = set()
    total_processed = 0
    total_songs_saved = 0

    print("⏳ Đang chạy Streaming...")

    with open(OUTPUT_FILE, 'w', encoding='utf-8') as f_out:
        for file_path in files:
            print(f"   📂 Reading file: {os.path.basename(file_path)}")
            
            # Mở file Parquet ở chế độ Stream
            parquet_file = pq.ParquetFile(file_path)
            
            # Duyệt qua từng nhóm dòng (Batch)
            for batch in parquet_file.iter_batches(batch_size=BATCH_SIZE, columns=[
                "musicbrainz_track_id", "track_name", 
                "musicbrainz_artist_id", "artist_name"
            ]):
                # Chuyển Batch sang Pandas DataFrame (Chỉ tốn RAM cho 50k dòng)
                df = batch.to_pandas()
                
                # Đổi tên cột
                df = df.rename(columns={
                    "musicbrainz_track_id": "_id",
                    "track_name": "title",
                    "musicbrainz_artist_id": "artist_id",
                    "artist_name": "artist"
                })

                # Lọc rác
                df = df.dropna(subset=["_id", "title"])
                
                # Xử lý ghi file
                batch_json_lines = []
                for _, row in df.iterrows():
                    # Check trùng lặp cực nhanh bằng Set
                    if row['_id'] not in seen_ids:
                        seen_ids.add(row['_id'])
                        batch_json_lines.append(json.dumps(row.to_dict(), ensure_ascii=False))
                
                # Ghi xuống đĩa ngay lập tức
                if batch_json_lines:
                    f_out.write("\n".join(batch_json_lines) + "\n")
                    total_songs_saved += len(batch_json_lines)

                # Cập nhật tiến độ
                total_processed += len(df)
                if total_processed % 500000 == 0:
                    print(f"      -> Đã quét {total_processed:,} dòng... (Lấy được {total_songs_saved:,} bài)")

    print("✅ HOÀN TẤT!")
    print(f"   - Tổng log đã quét: {total_processed:,}")
    print(f"   - Tổng bài hát sạch: {total_songs_saved:,}")
    print(f"   - Output: {OUTPUT_FILE}")

if __name__ == "__main__":
    main()