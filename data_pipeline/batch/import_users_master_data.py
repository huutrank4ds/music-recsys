import json
import time
from pathlib import Path
from pymongo import MongoClient, UpdateOne, ASCENDING, DESCENDING # type: ignore
import config as cfg
from common.logger import get_logger
from tqdm import tqdm

# --- CẤU HÌNH ---
MONGO_URI = cfg.MONGO_URI
DB_NAME = cfg.MONGO_DB
# Lưu ý: Đảm bảo bạn đã khai báo biến này trong config.py
COLLECTION_NAME = cfg.MONGO_USERS_COLLECTION 
DATA_DIR = Path(cfg.USER_MASTER_DATA_PATH) # type: ignore
BATCH_SIZE = 10000
FILE_NAME = "users.jsonl"
logger = get_logger("Sync Master Users")

# --- HÀM BỔ TRỢ (Giữ nguyên logic) ---

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
    logger.info(f"=== BẮT ĐẦU ĐỒNG BỘ USERS LÚC {current_time_str} ===")

    # 1. Kết nối Mongo
    col = get_mongo_collection()
    if col is None: return

    # 2. Kiểm tra file dữ liệu
    if not DATA_DIR.exists():
        logger.error(f"Thư mục {DATA_DIR} không tồn tại.")
        return

    users_file = DATA_DIR / FILE_NAME

    if not users_file.exists():
        logger.error(f"Không tìm thấy file {FILE_NAME}.")
        return

    # 3. Ước lượng tổng số dòng
    total_records = estimate_total_lines([users_file])

    # 4. Xử lý chính (Batch Insert/Update)
    pbar = tqdm(total=total_records, desc="Đang đồng bộ Users", unit=" user")
    operations = []
    
    with users_file.open("r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line: continue
            
            try:
                record = json.loads(line)
                
                # Logic lấy ID: Ưu tiên user_id, sau đó đến _id
                user_id = record.get("user_id") or record.get("_id") or record.get("id")
                
                if not user_id:
                    pbar.update(1)
                    continue
                
                # Chuẩn hóa lại ID thành string để nhất quán
                user_id = str(user_id)
                record["_id"] = user_id # Đảm bảo field _id trong Mongo khớp

                # Tạo lệnh UpdateOne (Upsert)
                ops = UpdateOne(
                    {"_id": user_id},           
                    {"$set": {k: v for k, v in record.items() if k != "_id"}},           
                    upsert=True                 
                )
                operations.append(ops)

                if len(operations) >= BATCH_SIZE:
                    col.bulk_write(operations, ordered=False)
                    pbar.update(len(operations))
                    operations = [] 
                        
            except Exception as e:
                # logger.error(f"Lỗi dòng: {e}") 
                continue

    # Xử lý số dư cuối cùng
    if operations:
        col.bulk_write(operations, ordered=False)
        pbar.update(len(operations))
    
    pbar.close()
    logger.info("Đồng bộ dữ liệu Users hoàn tất.")

    # 5. Tạo Index cho Users
    logger.info("Đang kiểm tra và khởi tạo Index...")
    try:    
        # Index tìm kiếm theo Username
        col.create_index(
            [("username", "text")],
            name="UserSearchIndex",
            background=True 
        )
        # Index cho ngày đăng ký (để thống kê user mới)
        col.create_index([("signup_date", DESCENDING)], name="SignupDateDescIndex", background=True)
        
        logger.info("Đã tạo xong Index.")
    except Exception as e:
        logger.error(f"Lỗi tạo Index: {e}")

def generate_metadata_file():
    """
    Tạo metadata.json thống kê cho Users
    """
    logger.info("\n=== TẠO FILE METADATA MỚI (USERS) ===")
    
    col = get_mongo_collection()
    if col is None: return

    try:
        # 1. Đếm tổng số bản ghi thực tế
        total_users = col.count_documents({})
        logger.info(f"Tổng số User trong DB: {total_users:,}")

        # 2. Lấy mẫu Schema
        sample_doc = col.find_one()
        schema_fields = []
        
        if sample_doc:
            for key, value in sample_doc.items():
                value_type = type(value).__name__
                if value is None: value_type = "Null"
                elif isinstance(value, dict): value_type = "Object"
                elif isinstance(value, list): value_type = "Array"
                
                schema_fields.append({
                    "field": key,
                    "type": value_type
                })
        
        # 3. Chuẩn bị nội dung
        metadata_content = {
            "total_users": total_users,
            "last_updated": time.strftime('%Y-%m-%d %H:%M:%S'),
            "source_file": FILE_NAME,
            "schema_count": len(schema_fields),
            "schema": schema_fields
        }

        # 4. Ghi ra file
        metadata_file = DATA_DIR / "metadata.json"
        with metadata_file.open("w", encoding="utf-8") as f:
            json.dump(metadata_content, f, indent=4, ensure_ascii=False)
            
        logger.info(f"Đã cập nhật metadata user tại: {metadata_file}")

    except Exception as e:
        logger.error(f"Lỗi khi tạo file metadata: {e}")

def check_sample_data(n):
    """Lấy n dòng dữ liệu thực tế từ DB để kiểm tra."""
    logger.info(f"\n=== KIỂM TRA MẪU USER TỪ DB ({n} dòng) ===")
    col = get_mongo_collection()
    
    if col is None: return
    samples = list(col.find().limit(n))
    
    if samples:
        logger.info(f"Tìm thấy {len(samples)} user mẫu:")
        
        for i, sample in enumerate(samples, 1):
            print(f"\n--- User #{i} (ID: {sample.get('_id')}) ---")
            print(json.dumps(sample, indent=4, ensure_ascii=False, default=str))
    else:
        logger.warning("Collection User rỗng!")

# --- MAIN ---

if __name__ == "__main__":
    sync_data()
    check_sample_data(2)
    generate_metadata_file()