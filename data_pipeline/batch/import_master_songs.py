import json
import time
from pathlib import Path
from pymongo import MongoClient, UpdateOne #type: ignore
import config as cfg
from common.logger import get_logger
from tqdm import tqdm

# ================= CẤU HÌNH =================
MONGO_URI = cfg.MONGO_URI
DB_NAME = cfg.MONGO_DB
COLLECTION_NAME = cfg.COLLECTION_SONGS
DATA_DIR = Path(cfg.SONGS_MASTER_LIST_PATH)
BATCH_SIZE = 2000 
logger = get_logger("Import_Master_Songs")

# ================= HÀM ƯỚC LƯỢNG =================
def estimate_total_lines(files):
    logger.info("Đang ước lượng khối lượng dữ liệu...")
    total_bytes = sum(f.stat().st_size for f in files)
    if total_bytes == 0: return 0

    sample_lines = 0
    sample_bytes = 0
    try:
        with files[0].open("r", encoding="utf-8") as f:
            for i, line in enumerate(f):
                if i >= 100: break
                sample_lines += 1
                sample_bytes += len(line.encode('utf-8')) 
    except Exception:
        return 1000000

    if sample_lines == 0 or sample_bytes == 0: return 0

    avg_bytes_per_line = sample_bytes / sample_lines
    estimated_total = int(total_bytes / avg_bytes_per_line)
    
    logger.info(f"   -> Ước tính: {estimated_total:,} dòng.")
    return estimated_total

# ================= HÀM CHÍNH =================
def sync_data():
    current_time_str = time.strftime('%Y-%m-%d %H:%M:%S')
    logger.info(f"[SYNC] Bắt đầu lúc {current_time_str}")

    # 1. Kết nối Mongo
    logger.info("Đang kết nối MongoDB...")
    try:
        client = MongoClient(MONGO_URI)
        db = client[DB_NAME]
        col = db[COLLECTION_NAME]
        client.admin.command('ping')
        logger.info("OK!")
    except Exception as e:
        logger.error(f"Lỗi kết nối DB: {e}")
        return

    # 2. Tìm file
    if not DATA_DIR.exists():
        logger.error(f"Thư mục {DATA_DIR} không tồn tại.")
        return

    json_files = list(DATA_DIR.glob("*.json"))
    if not json_files:
        logger.error("Không tìm thấy file dữ liệu.")
        return

    # Tính tổng
    total_records = estimate_total_lines(json_files)
    pbar = tqdm(total=total_records, desc="Đang đồng bộ dữ liệu", unit=" dòng")
    
    # 3. Xử lý chính
    logger.info(f"Bắt đầu xử lý với BATCH_SIZE={BATCH_SIZE}...")
    
    operations = []

    ALLOWED_FIELDS = {
        "title", 
        "artist", 
        "artist_id", # Giữ lại nếu muốn làm trang profile nghệ sĩ
    }
    
    for file_path in json_files:
        with file_path.open("r", encoding="utf-8") as f:
            for line in f:
                try:
                    record = json.loads(line)
                    song_id = record.get("id")
                    if not song_id: 
                        song_id = record.get("_id")
                    if not song_id:
                        pbar.update(1)
                        continue

                    # Lọc chỉ giữ các trường cần thiết
                    filtered_record = {k: v for k, v in record.items() if k in ALLOWED_FIELDS}
                    
                    ops = UpdateOne(
                        {"_id": song_id},           
                        {"$set": filtered_record},           
                        upsert=True                 
                    )
                    operations.append(ops)

                    if len(operations) >= BATCH_SIZE:
                        col.bulk_write(operations, ordered=False)
                        pbar.update(len(operations))
                        operations = [] 
                        
                except Exception as e:
                    logger.error(f"Lỗi khi xử lý dòng: {e}")
                    continue

    # Xử lý số dư cuối
    if operations:
        col.bulk_write(operations, ordered=False)
        pbar.update(len(operations))
    logger.info(f"[SYNC] HOÀN TẤT! Tổng đã xử lý: {pbar.n:,} dòng.")
    logger.info("Đang kiểm tra và khởi tạo Index cho tìm kiếm...")
    try:
        # Tạo Text Index cho title và artist để MusicService.search_songs hoạt động
        # Background=True giúp việc tạo index không làm khóa (lock) database
        col.create_index(
            [("title", "text"), ("artist", "text")],
            name="SongSearchIndex",
            background=True 
        )
        logger.info("XONG!")
    except Exception as e:
        logger.error(f"Cảnh báo lỗi Index: {e}")

if __name__ == "__main__":
    sync_data()