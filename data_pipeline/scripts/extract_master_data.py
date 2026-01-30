from pathlib import Path
import pyarrow as pa
import pyarrow.parquet as pq
import json
import shutil
import pandas as pd
import hashlib
import urllib.parse
from datetime import datetime
from common.logger import get_logger
import os
from pymongo import MongoClient #type: ignore

# ================= CONFIG =================
logger = get_logger("Extract_Simulation_Data")

# Đường dẫn Input/Output
INPUT_DIR = Path(os.getenv("MUSIC_LOGS_DATA_PATH", "/opt/data/processed_sorted"))

# Output cho Logs
OUTPUT_LOGS_DIR = Path(os.getenv("SIMULATION_LOGS_PATH", "/opt/data/simulation_logs"))
OUTPUT_LOGS_FILE = OUTPUT_LOGS_DIR / "listening_history.parquet"

# Output cho User Master
OUTPUT_USER_DIR = Path(os.getenv("USER_MASTER_DATA_PATH", "/opt/data/user_master_data"))
OUTPUT_USER_FILE = OUTPUT_USER_DIR / "users.jsonl"

# MongoDB Config
MONGO_URI = os.getenv("MONGO_URI")
DB_NAME = os.getenv("MONGO_DB")
COLLECTION_SONGS = "songs"

BATCH_SIZE = 100000

# ================= HELPER FUNCTIONS =================

def get_optimized_song_map():
    """
    Tải danh sách bài hát vào RAM.
    150k bài hát chỉ tốn khoảng ~30MB RAM, an toàn để load hết.
    """
    logger.info("[Extract] Đang tải Map Duration từ MongoDB...")
    try:
        client = MongoClient(MONGO_URI)
        db = client[DB_NAME]
        collection = db[COLLECTION_SONGS]
        
        # PROJECTION: Chỉ lấy _id và duration để tiết kiệm RAM tối đa
        cursor = collection.find({}, {"_id": 1, "duration": 1})
        
        songs_map = {}
        count = 0
        for doc in cursor:
            # Đảm bảo key là string để map với parquet
            s_id = str(doc["_id"])
            # Đảm bảo duration là float (mặc định 0 nếu thiếu)
            dur = float(doc.get("duration", 0.0))
            songs_map[s_id] = dur
            count += 1
            
        logger.info(f"[Extract] Đã tải {count} bài hát vào bộ nhớ.")
        return songs_map
    except Exception as e:
        logger.error(f"[Extract] Lỗi kết nối MongoDB: {e}")
        return {}

def generate_avatar(name):
    """Tạo URL avatar"""
    if not name: return ""
    hash_object = hashlib.md5(str(name).encode())
    bg = hash_object.hexdigest()[:6]
    safe_name = urllib.parse.quote(str(name))
    return f"https://ui-avatars.com/api/?name={safe_name}&background={bg}&color=fff&size=512&length=1&bold=true&font-size=0.7"

def generate_username(user_id):
    """Tạo username giả lập"""
    short_hash = hashlib.sha1(str(user_id).encode()).hexdigest()[:8]
    return f"{short_hash}"

# ================= MAIN =================

def main():
    logger.info(f"[Extract] Bắt đầu xử lý (Batch Size: {BATCH_SIZE})...")

    # 1. Reset thư mục Output
    if OUTPUT_LOGS_DIR.exists(): shutil.rmtree(OUTPUT_LOGS_DIR)
    OUTPUT_LOGS_DIR.mkdir(parents=True, exist_ok=True)
    
    if OUTPUT_USER_DIR.exists(): shutil.rmtree(OUTPUT_USER_DIR)
    OUTPUT_USER_DIR.mkdir(parents=True, exist_ok=True)

    # 2. Load Reference Data (Songs)
    song_duration_map = get_optimized_song_map()
    if not song_duration_map:
        logger.error("[Extract] Không lấy được dữ liệu Songs. Dừng chương trình.")
        return

    # 3. Quét file Input
    files = list(INPUT_DIR.glob("*.parquet"))
    if not files:
        logger.error(f"[Extract] Không tìm thấy file Parquet tại {INPUT_DIR}")
        return

    # 4. Định nghĩa Schema Output (PyArrow)
    # Timestamp: int64 (Unix timestamp)
    log_schema = pa.schema([
        ('user_id', pa.string()),
        ('track_id', pa.string()),
        ('timestamp', pa.int64()), # LongType
        ('action', pa.string()),
        ('source', pa.string()),
        ('duration', pa.float64()),
        ('total_duration', pa.float64())
    ])

    seen_user_ids = set()
    total_processed = 0
    total_logs_saved = 0
    total_users_saved = 0
    
    parquet_writer = None
    last_row_debug = None 
    last_user_debug = None # <--- [ADD 1] Biến lưu mẫu User

    try:
        with OUTPUT_USER_FILE.open('w', encoding='utf-8') as f_user_out:
            
            for file_path in files:
                logger.info(f"[Extract] Đang đọc file: {file_path.name}")
                
                try:
                    pq_file = pq.ParquetFile(file_path)
                    
                    # Đọc các cột cần thiết từ file gốc
                    # Lưu ý: timestamp ở đây đang là String (ISO 8601)
                    cols = ["user_id", "timestamp", "musicbrainz_track_id"]
                    
                    for batch in pq_file.iter_batches(batch_size=BATCH_SIZE, columns=cols):
                        df = batch.to_pandas()
                        
                        # --- BƯỚC 1: MAP & FILTER SONGS ---
                        df = df.rename(columns={"musicbrainz_track_id": "track_id"})
                        
                        # Map duration từ Dict (Rất nhanh)
                        df['mapped_duration'] = df['track_id'].map(song_duration_map)
                        
                        # Loại bỏ các dòng mà track_id không có trong Master Data
                        df_clean = df.dropna(subset=['mapped_duration']).copy()
                        
                        if df_clean.empty:
                            continue

                        # --- BƯỚC 2: XỬ LÝ TIMESTAMP (QUAN TRỌNG) ---
                        # Chuyển chuỗi "2009-05-04T13:06:09" -> Datetime -> Unix Timestamp (Milliseconds - Int64)
                        try:
                            df_clean['timestamp'] = pd.to_datetime(df_clean['timestamp'], errors='coerce').astype("datetime64[ns]")
                            # Chuyển về số mili giây (int64)
                            df_clean['timestamp'] = df_clean['timestamp'].astype('int64') // 10**6
                        except Exception as e:
                            logger.error(f"Lỗi convert timestamp: {e}")
                            continue

                        # --- BƯỚC 3: TẠO CÁC CỘT CÒN LẠI ---
                        df_clean['action'] = "complete"
                        df_clean['source'] = "simulation"
                        df_clean['duration'] = df_clean['mapped_duration'].astype('float')
                        df_clean['total_duration'] = df_clean['mapped_duration'].astype('float')
                        
                        # Chọn cột đúng thứ tự Schema
                        df_final = df_clean[[
                            "user_id", "track_id", "timestamp", "action", 
                            "source", "duration", "total_duration"
                        ]]

                        # Ghi Logs ra Parquet
                        table = pa.Table.from_pandas(df_final, schema=log_schema)
                        if parquet_writer is None:
                            parquet_writer = pq.ParquetWriter(OUTPUT_LOGS_FILE, log_schema)
                        parquet_writer.write_table(table)

                        # Lưu dòng mẫu để in
                        if not df_final.empty:
                            last_row_debug = df_final.iloc[-1].to_dict()
                        
                        total_logs_saved += len(df_final)

                        # --- BƯỚC 4: TRÍCH XUẤT USER MASTER ---
                        unique_users = df_clean['user_id'].unique()
                        new_users = []
                        signup_date_str = datetime.now().isoformat()

                        for uid in unique_users:
                            # Chuyển uid về string để chắc chắn
                            uid_str = str(uid)
                            if uid_str not in seen_user_ids:
                                seen_user_ids.add(uid_str)
                                
                                username = generate_username(uid_str)
                                user_obj = {
                                    "_id": uid_str,
                                    "username": username,
                                    "latent_vector": None,
                                    "signup_date": signup_date_str,
                                    "image_url": generate_avatar(username)
                                }
                                new_users.append(user_obj)
                        
                        # Ghi User ra JSONL
                        if new_users:
                            for u in new_users:
                                f_user_out.write(json.dumps(u, ensure_ascii=False) + "\n")
                            
                            # <--- [ADD 2] Lưu mẫu User cuối cùng của batch này để in
                            last_user_debug = new_users[-1]
                            
                            total_users_saved += len(new_users)

                        total_processed += len(df)

                except Exception as e:
                    logger.error(f"[Extract] Lỗi đọc file {file_path.name}: {e}")
                    continue

    finally:
        if parquet_writer:
            parquet_writer.close()

    # --- KẾT QUẢ ---
    logger.info("[Extract] HOÀN TẤT!")
    logger.info(f"Tổng dòng log đã quét: {total_processed:,}")
    logger.info(f"Tổng log hợp lệ (match songs): {total_logs_saved:,}")
    logger.info(f"Tổng user unique đã tạo: {total_users_saved:,}")
    logger.info(f"Output Logs: {OUTPUT_LOGS_FILE}")
    logger.info(f"Output Users: {OUTPUT_USER_FILE}")

    # <--- [ADD 3] In mẫu dữ liệu Log
    if last_row_debug:
        print("\n" + "="*50)
        print("SAMPLE DATA (Dữ liệu log cuối cùng):")
        print(json.dumps(last_row_debug, indent=4))
        print("="*50)

    # <--- [ADD 4] In mẫu dữ liệu User
    if last_user_debug:
        print("\n" + "="*50)
        print("SAMPLE USER DATA (Dữ liệu user cuối cùng):")
        print(json.dumps(last_user_debug, indent=4, ensure_ascii=False))
        print("="*50 + "\n")

if __name__ == "__main__":
    main()