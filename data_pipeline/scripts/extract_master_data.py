from pathlib import Path
import pyarrow.parquet as pq
import json
import shutil
import random
import pandas as pd
import hashlib
import urllib.parse
from common.logger import get_logger

# ================= CONFIG =================
logger = get_logger("Extract_Master_Data")

INPUT_DIR = Path("/opt/data/processed_sorted")
OUTPUT_DIR = Path("/opt/data/songs_master_list")
OUTPUT_FILE = OUTPUT_DIR / "songs.json"
META_FILE = OUTPUT_DIR / "metadata.json"

BATCH_SIZE = 100000
FIXED_YOUTUBE_URL = "JfVos4VSpmA" # Nhạc Lofi mặc định

# Hàm tạo ảnh bìa với màu cố định dựa trên tên bài hát
def generate_cover(title):
    """Tạo URL ảnh bìa dựa trên tên bài hát (giữ màu cố định)"""
    if not title: return ""
    # Băm tên để lấy mã màu hex
    hash_object = hashlib.md5(str(title).encode())
    bg = hash_object.hexdigest()[:6]
    # Encode tên bài hát cho URL
    safe_name = urllib.parse.quote(str(title))
    return f"https://ui-avatars.com/api/?name={safe_name}&background={bg}&color=fff&size=512&length=1&bold=true"

def main():
    logger.info(f"[Extract] Bắt đầu xử lý Metadata (Batch Size: {BATCH_SIZE})...")
    # Lấy danh sách file Parquet
    files = list(INPUT_DIR.glob("*.parquet"))
    
    if not files:
        logger.error(f"[Extract] Không tìm thấy file Parquet nào trong: {INPUT_DIR}")
        return
    
    logger.info(f"[Extract] Tìm thấy {len(files)} file input.")

    # Dọn dẹp thư mục output cũ nếu tồn tại
    if OUTPUT_DIR.exists():
        logger.info(f"[Extract] Dọn dẹp thư mục cũ: {OUTPUT_DIR}")
        shutil.rmtree(OUTPUT_DIR)
    
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    seen_ids = set()
    total_processed = 0
    total_songs_saved = 0

    try:
        with OUTPUT_FILE.open('w', encoding='utf-8') as f_out:
            
            for file_path in files:
                logger.info(f"[Extract] reading: {file_path.name}")
                try:
                    parquet_file = pq.ParquetFile(file_path)
                    iter_batches = parquet_file.iter_batches(
                        batch_size=BATCH_SIZE, 
                        columns=[
                            "musicbrainz_track_id", "track_name", 
                            "musicbrainz_artist_id", "artist_name"
                        ]
                    )

                    for batch in iter_batches:
                        df = batch.to_pandas()
                        # Chuẩn hóa schema
                        df = df.rename(columns={
                            "musicbrainz_track_id": "_id",
                            "track_name": "track_name",
                            "musicbrainz_artist_id": "artist_id",
                            "artist_name": "artist_name"
                        })

                        # Lọc rác & Null
                        df = df.dropna(subset=["_id", "track_name"])

                        # Khử trùng lặp
                        current_ids = df['_id'].tolist()
                        new_indices = []
                        
                        # Kiểm tra nhanh với Set seen_ids
                        for idx, uid in enumerate(current_ids):
                            if uid not in seen_ids:
                                seen_ids.add(uid)
                                new_indices.append(idx)
                        
                        # Nếu batch này toàn bài trùng -> Bỏ qua
                        if not new_indices:
                            continue
                        
                        # Tạo DataFrame mới chỉ chứa bài hát mới
                        df_new = df.iloc[new_indices].copy()

                        # Gán trực tiếp các cột Metadata giả lập
                        df_new['url'] = FIXED_YOUTUBE_URL
                        df_new['plays_7d'] = 0
                        df_new['plays_cumulative'] = 0
                        
                        # Giả lập độ dài bài hát 5 phút
                        df_new['duration_ms'] = 300000
                        df_new['release_date'] = '2026-01-01'
                        
                        # Tạo ảnh bìa
                        df_new['image_url'] = df_new['track_name'].apply(generate_cover)

                        # Ghi xuống file JSON Lines
                        # Dùng to_json của Pandas để tạo JSON Lines
                        json_str = df_new.to_json(orient='records', lines=True, force_ascii=False)
                        f_out.write(json_str)
                        f_out.write("\n") # Xuống dòng
                        
                        # Cập nhật metrics
                        count_new = len(df_new)
                        total_songs_saved += count_new
                        total_processed += len(df)
                        
                except Exception as e:
                    logger.error(f"[Extract] Lỗi khi đọc file {file_path.name}: {e}")
                    continue
        # Ghi file metadata
        try:
            metadata = {
                "total_songs": total_songs_saved,
                "schema": [
                    "_id", "track_name", "artist_id", "artist_name",
                    "image_url", "url", "duration_ms", "plays_7d", "plays_cumulative", "release_date"
                ],
                "processed_at": pd.Timestamp.now().isoformat()
            }
            with META_FILE.open('w', encoding='utf-8') as f_meta:
                json.dump(metadata, f_meta, ensure_ascii=False, indent=2)
        except Exception as e:
            logger.error(f"[Extract] Lỗi khi tạo metadata: {e}")
            metadata = {}

        logger.info("[Extract] HOÀN TẤT QUÁ TRÌNH ETL!")
        logger.info(f"[Extract] Tổng dòng đã quét: {total_processed:,}")
        logger.info(f"[Extract] Tổng bài hát lưu (Unique): {total_songs_saved:,}")
        logger.info(f"[Extract] File kết quả: {OUTPUT_FILE.absolute()}")
        logger.info(f"[Extract] File metadata: {META_FILE.absolute()}")
    except IOError as e:
        logger.error(f"[Extract] Lỗi ghi file output: {e}")

if __name__ == "__main__":
    main()