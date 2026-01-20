import json
import time
from pathlib import Path
from pymongo import MongoClient, UpdateOne #type: ignore
import src.config as cfg

# ================= CẤU HÌNH =================
MONGO_URI = cfg.MONGO_URI
DB_NAME = cfg.MONGO_DB
COLLECTION_NAME = cfg.COLLECTION_SONGS
DATA_DIR = Path(cfg.SONGS_MASTER_LIST_PATH)
BATCH_SIZE = 2000 

# ================= CLASS: THANH TIẾN TRÌNH =================
class DockerProgressBar:
    def __init__(self, total, desc="Processing", min_interval=3.0):
        self.total = total
        self.desc = desc
        self.current = 0
        self.start_time = time.time()
        self.last_log_time = time.time()
        self.min_interval = min_interval

    def update(self, n=1):
        self.current += n
        now = time.time()
        if (now - self.last_log_time > self.min_interval) or (self.current >= self.total):
            self._print_log(now)
            self.last_log_time = now

    def _print_log(self, now):
        elapsed = now - self.start_time
        if elapsed == 0: elapsed = 0.001
        percent = self.current / self.total if self.total > 0 else 0
        percent = min(percent, 1.0)
        speed = self.current / elapsed
        remaining_items = self.total - self.current
        eta = remaining_items / speed if speed > 0 else 0
        bar_length = 30
        filled_length = int(bar_length * percent)

        if filled_length == 0:
            bar = " " * bar_length
        elif filled_length >= bar_length:
            bar = "=" * bar_length
        else:
            bar = "-" * (filled_length - 1) + ">" + " " * (bar_length - filled_length)
        eta_str = time.strftime("%M:%S", time.gmtime(eta))
        print(f"\r{self.desc}: |{bar}| {percent:.1%} [{self.current}/{self.total}] "
              f"Speed: {speed:.0f}/s | ETA: {eta_str}", end="", flush=True)

# ================= HÀM ƯỚC LƯỢNG =================
def estimate_total_lines(files):
    print("Đang ước lượng khối lượng dữ liệu...", flush=True)
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
    
    print(f"   -> Ước tính: {estimated_total:,} dòng.", flush=True)
    return estimated_total

# ================= HÀM CHÍNH =================
def sync_data():
    current_time_str = time.strftime('%Y-%m-%d %H:%M:%S')
    print(f"[SYNC] Bắt đầu lúc {current_time_str}", flush=True)

    # 1. Kết nối Mongo
    print("Đang kết nối MongoDB...", end=" ", flush=True)
    try:
        client = MongoClient(MONGO_URI)
        db = client[DB_NAME]
        col = db[COLLECTION_NAME]
        client.admin.command('ping')
        print("OK!", flush=True)
    except Exception as e:
        print(f"\nLỗi kết nối DB: {e}", flush=True)
        return

    # 2. Tìm file
    if not DATA_DIR.exists():
        print(f"Thư mục {DATA_DIR} không tồn tại.", flush=True)
        return

    json_files = list(DATA_DIR.glob("*.json"))
    if not json_files:
        print("Không tìm thấy file dữ liệu.", flush=True)
        return

    # Tính tổng
    total_records = estimate_total_lines(json_files)
    
    # 3. Xử lý chính
    print(f"Bắt đầu xử lý với BATCH_SIZE={BATCH_SIZE}...", flush=True)
    pbar = DockerProgressBar(total=total_records, desc="Syncing", min_interval=2.0)
    
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
                    continue

    # Xử lý số dư cuối
    if operations:
        col.bulk_write(operations, ordered=False)
        pbar.update(len(operations))

    print("\n") # Xuống dòng sau khi thanh bar chạy xong
    print("-------------------------------------------------------", flush=True)
    print(f"[SYNC] HOÀN TẤT! Tổng đã xử lý: {pbar.current}", flush=True)
    print("Đang kiểm tra và khởi tạo Index cho tìm kiếm...", end=" ", flush=True)
    try:
        # Tạo Text Index cho title và artist để MusicService.search_songs hoạt động
        # Background=True giúp việc tạo index không làm khóa (lock) database
        col.create_index(
            [("title", "text"), ("artist", "text")],
            name="SongSearchIndex",
            background=True 
        )
        print("XONG!", flush=True)
    except Exception as e:
        print(f"Cảnh báo lỗi Index: {e}", flush=True)

if __name__ == "__main__":
    sync_data()