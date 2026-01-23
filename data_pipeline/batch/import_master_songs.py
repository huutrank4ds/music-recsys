import json
import time
from pathlib import Path
from pymongo import MongoClient, UpdateOne #type: ignore
import config as cfg
from common.logger import get_logger
from tqdm import tqdm

MONGO_URI = cfg.MONGO_URI
DB_NAME = cfg.MONGO_DB
COLLECTION_NAME = cfg.COLLECTION_SONGS
DATA_DIR = Path(cfg.SONGS_MASTER_LIST_PATH)
BATCH_SIZE = 10000
logger = get_logger("Import_Master_Songs")

# Hàm ước lượng tổng số dòng trong tất cả file
def estimate_total_lines(files):
    logger.info("[SYNC] Đang ước lượng khối lượng dữ liệu...")
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
    
    logger.info(f"[SYNC] Ước tính: {estimated_total:,} dòng.")
    return estimated_total

def get_mongo_collection():
    try:
        client = MongoClient(MONGO_URI)
        db = client[DB_NAME]
        col = db[COLLECTION_NAME]
        client.admin.command('ping')
        logger.info("[SYNC] Kết nối MongoDB thành công!")
        return col
    except Exception as e:
        logger.error(f"[SYNC] Lỗi kết nối DB: {e}")
        return None

# Hàm chính để đồng bộ dữ liệu
def sync_data():
    current_time_str = time.strftime('%Y-%m-%d %H:%M:%S')
    logger.info(f"[SYNC] Bắt đầu lúc {current_time_str}")

    # 1. Kết nối Mongo
    logger.info("[SYNC] Đang kết nối MongoDB...")
    col = get_mongo_collection()
    if col is None:
        return

    # 2. Tìm file
    if not DATA_DIR.exists():
        logger.error(f"[SYNC] Thư mục {DATA_DIR} không tồn tại.")
        return

    songs_file = DATA_DIR / "songs.json"
    if not songs_file.exists():
        logger.error(f"[SYNC] File {songs_file} không tồn tại.")
        return
    metadata_file = DATA_DIR / "metadata.json"
    if metadata_file.exists():
        logger.info(f"[SYNC] Tìm thấy file metadata: {metadata_file}")
        with metadata_file.open("r", encoding="utf-8") as f_meta:
            try:
                metadata = json.load(f_meta)
                total_songs = metadata.get("total_songs", "Không rõ")
                schema = metadata.get("schema", [])
                processed_at = metadata.get("processed_at", "Không rõ")
                logger.info(f"[SYNC] Metadata - Tổng bài hát: {total_songs}, Xử lý lúc: {processed_at}")
                logger.info(f"[SYNC] Metadata - Schema: {schema}")
            except Exception as e:
                logger.error(f"[SYNC] Lỗi đọc file metadata: {e}")
    else:
        logger.warning(f"[SYNC] Không tìm thấy file metadata: {metadata_file}")
    if not songs_file.exists():
        logger.error("[SYNC] Không tìm thấy file dữ liệu.")
        return

    # Tính tổng
    if total_songs != "Không rõ":
        total_records = total_songs
    else:
        total_records = estimate_total_lines([songs_file])
    pbar = tqdm(total=total_records, desc="[SYNC] Đang đồng bộ dữ liệu", unit=" dòng")
    
    # 3. Xử lý chính
    logger.info(f"[SYNC] Bắt đầu xử lý với BATCH_SIZE={BATCH_SIZE}...")
    
    operations = []
    with songs_file.open("r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                record = json.loads(line)
                song_id = record.get("id")
                if not song_id: 
                    song_id = record.get("_id")
                if not song_id:
                    pbar.update(1)
                    continue
                
                ops = UpdateOne(
                    {"_id": song_id},           
                    {"$set": {k: v for k, v in record.items() if k != "_id"}},           
                    upsert=True                 
                )
                operations.append(ops)

                if len(operations) >= BATCH_SIZE:
                    col.bulk_write(operations, ordered=False)
                    pbar.update(len(operations))
                    operations = [] 
                        
            except Exception as e:
                logger.error(f"[SYNC] Lỗi khi xử lý dòng: {e}")
                continue

    # Xử lý số dư cuối
    if operations:
        col.bulk_write(operations, ordered=False)
        pbar.update(len(operations))
    logger.info(f"[SYNC] HOÀN TẤT! Tổng đã xử lý: {pbar.n:,} dòng.")
    logger.info("[SYNC] Đang kiểm tra và khởi tạo Index cho tìm kiếm...")
    try:
        # Tạo Text Index cho title và artist để MusicService.search_songs hoạt động
        col.create_index(
            [("title", "text"), ("artist", "text")],
            name="SongSearchIndex",
            background=True 
        )
        logger.info("[SYNC] XONG!")
    except Exception as e:
        logger.error(f"[SYNC] Cảnh báo lỗi Index: {e}")

if __name__ == "__main__":
    sync_data()