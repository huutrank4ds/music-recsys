import json
import time
from pathlib import Path
from pymongo import MongoClient, UpdateOne  # type: ignore
import config as cfg
from common.logger import get_logger
from tqdm import tqdm

# --- CẤU HÌNH ---
MONGO_URI = cfg.MONGO_URI
DB_NAME = cfg.MONGO_DB
COLLECTION_NAME = cfg.MONGO_SONGS_COLLECTION
DATA_DIR = Path(cfg.SONGS_MASTER_DATA_PATH) #type: ignore
BATCH_SIZE = 10000
FILE_NAME = "songs_clean.jsonl"
logger = get_logger("Sync Master Songs")

# --- HÀM BỔ TRỢ ---

def estimate_total_lines(files):
    """Ước lượng số dòng dữ liệu để hiển thị thanh tiến trình."""
    logger.info("Đang ước lượng khối lượng dữ liệu từ file source...")
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
    
    logger.info(f"Ước tính khoảng: {estimated_total:,} dòng.")
    return estimated_total

def get_mongo_collection():
    """Kết nối MongoDB và trả về object collection."""
    try:
        client = MongoClient(MONGO_URI)
        db = client[DB_NAME]
        col = db[COLLECTION_NAME]
        client.admin.command('ping')
        return col
    except Exception as e:
        logger.error(f"Lỗi kết nối DB: {e}")
        return None

# --- CÁC CHỨC NĂNG CHÍNH ---

def sync_data():
    """Hàm chính để đồng bộ dữ liệu từ JSON vào MongoDB."""
    current_time_str = time.strftime('%Y-%m-%d %H:%M:%S')
    logger.info(f"=== BẮT ĐẦU ĐỒNG BỘ LÚC {current_time_str} ===")

    # 1. Kết nối Mongo
    col = get_mongo_collection()
    if col is None: return

    # 2. Kiểm tra file dữ liệu
    if not DATA_DIR.exists():
        logger.error(f"Thư mục {DATA_DIR} không tồn tại.")
        return

    songs_file = DATA_DIR / FILE_NAME

    if not songs_file.exists():
        logger.error(f"Không tìm thấy file {FILE_NAME}.")
        return

    # 3. Ước lượng tổng số dòng (Thay vì đọc metadata cũ)
    total_records = estimate_total_lines([songs_file])

    # 4. Xử lý chính (Batch Insert/Update)
    pbar = tqdm(total=total_records, desc="Đang đồng bộ", unit=" dòng")
    operations = []
    
    with songs_file.open("r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line: continue
            
            try:
                record = json.loads(line)
                
                # Ưu tiên lấy id hoặc _id làm khóa chính
                song_id = record.get("id") or record.get("_id")
                
                if not song_id:
                    pbar.update(1)
                    continue
                
                # Tạo lệnh UpdateOne (Upsert)
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
                continue

    # Xử lý số dư cuối cùng
    if operations:
        col.bulk_write(operations, ordered=False)
        pbar.update(len(operations))
    
    pbar.close()
    logger.info("Đồng bộ dữ liệu hoàn tất.")

    # 5. Tạo Index
    logger.info("Đang kiểm tra và khởi tạo Index...")
    try:    
        col.create_index(
            [("track_name", "text"), ("artist_name", "text")],
            name="SongSearchIndex",
            weights={"track_name": 10, "artist_name": 5},
            background=True 
        )
        col.create_index([("plays_7d", -1)], name="Plays7dDescIndex", background=True)
        col.create_index([("plays_cumulative", -1)], name="PlaysCumulativeDescIndex", background=True)
        logger.info("Đã tạo xong Index.")
    except Exception as e:
        logger.error(f"Lỗi tạo Index: {e}")

def generate_metadata_file():
    """
    Sau khi import xong, hàm này sẽ thống kê dữ liệu thực tế trong MongoDB
    và tạo ra file metadata.json mới.
    """
    logger.info("\n=== TẠO FILE METADATA MỚI ===")
    
    col = get_mongo_collection()
    if col is None: return

    try:
        # 1. Đếm tổng số bản ghi thực tế
        total_songs = col.count_documents({})
        logger.info(f"Tổng số bản ghi trong DB: {total_songs:,}")

        # 2. Lấy mẫu 1 document để trích xuất Schema (các trường dữ liệu)
        # Chúng ta giả định các document có cấu trúc tương tự nhau
        sample_doc = col.find_one()
        schema_fields = []
        
        if sample_doc:
            for key, value in sample_doc.items():
                # Xác định kiểu dữ liệu cơ bản để ghi vào document
                value_type = type(value).__name__
                if value is None:
                    value_type = "Null"
                elif isinstance(value, dict):
                    value_type = "Object"
                elif isinstance(value, list):
                    value_type = "Array"
                
                schema_fields.append({
                    "field": key,
                    "type": value_type
                })
        
        # 3. Chuẩn bị nội dung file metadata
        metadata_content = {
            "total_songs": total_songs,
            "last_updated": time.strftime('%Y-%m-%d %H:%M:%S'),
            "source_file": "FILE_NAME",
            "schema_count": len(schema_fields),
            "schema": schema_fields
        }

        # 4. Ghi ra file
        metadata_file = DATA_DIR / "metadata.json"
        with metadata_file.open("w", encoding="utf-8") as f:
            json.dump(metadata_content, f, indent=4, ensure_ascii=False)
            
        logger.info(f"Đã tạo file metadata thành công tại: {metadata_file}")
        logger.info("Nội dung bao gồm: Số lượng bản ghi, thời gian import và danh sách các trường.")

    except Exception as e:
        logger.error(f"Lỗi khi tạo file metadata: {e}")

def check_sample_data():
    """Lấy 1 dòng dữ liệu thực tế từ DB để kiểm tra hiển thị."""
    logger.info("\n=== KIỂM TRA MẪU DỮ LIỆU TỪ DB ===")
    col = get_mongo_collection()
    
    if col is None: 
        return

    sample = col.find_one()
    if sample:
        logger.info(f"Dữ liệu mẫu (ID: {sample.get('_id')}):")
        print(json.dumps(sample, indent=4, ensure_ascii=False, default=str))
    else:
        logger.warning("Collection rỗng, chưa có dữ liệu!")

# --- MAIN ---

if __name__ == "__main__":
    # 1. Thực hiện Import
    sync_data()
    
    # 2. Kiểm tra mẫu
    check_sample_data()
    
    # 3. Tạo file metadata mới dựa trên dữ liệu vừa import
    generate_metadata_file()