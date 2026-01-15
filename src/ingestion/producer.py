import json
import socket
import time
import sys
from datetime import datetime
import pyarrow.parquet as pq
from pathlib import Path
# Thêm AdminClient và NewTopic
from confluent_kafka import Producer
from confluent_kafka.admin import AdminClient, NewTopic 

# ================= CẤU HÌNH TỐC ĐỘ =================
SPEED_FACTOR = 200.0  # Tốc độ nhanh hơn thời gian thực gấp bao nhiêu lần
MAX_SLEEP_SEC = 2.0   

# ================= CẤU HÌNH KAFKA =================
CONF = {
    'bootstrap.servers': 'kafka:9092',
    'client.id': socket.gethostname(),
    'acks': '1',
    'linger.ms': 5,
    'batch.size': 16384,
    'compression.type': 'gzip',
}

TOPIC = "music_log"
DATA_DIR = Path('/opt/data/processed_sorted')
TIMESTAMP_COL = 'timestamp'
NUM_PARTITIONS = 4
REPLICATION_FACTOR = 1

BATCH_SIZE = 2000  # Số bản ghi mỗi lô

# ================= HÀM QUẢN LÝ TOPIC =================
def ensure_topic_exists():
    """Kiểm tra và tạo Topic nếu chưa có"""
    print(f"🔧 Đang kiểm tra Topic '{TOPIC}'...")
    
    # Tạo AdminClient (dùng chung config với Producer)
    admin_client = AdminClient({'bootstrap.servers': CONF['bootstrap.servers']})
    
    # Lấy danh sách topic hiện có
    cluster_metadata = admin_client.list_topics(timeout=10)
    
    if TOPIC in cluster_metadata.topics:
        print(f"✅ Topic '{TOPIC}' đã tồn tại.")
    else:
        print(f"⚠️ Topic chưa có. Đang tạo mới với {NUM_PARTITIONS} partitions...")
        # Định nghĩa topic mới
        new_topic = NewTopic(
            topic=TOPIC, 
            num_partitions=NUM_PARTITIONS, 
            replication_factor=REPLICATION_FACTOR
        )
        # Gửi lệnh tạo
        fs = admin_client.create_topics([new_topic])
        
        # Chờ kết quả
        for topic, future in fs.items():
            try:
                future.result()  # Block chờ tạo xong
                print(f"🎉 Đã tạo thành công topic: {topic}")
            except Exception as e:
                print(f"❌ Không thể tạo topic {topic}: {e}")

# ================= GENERATOR =================
def source_data_generator(data_dir, skip_time=True):
    files = sorted([f for f in data_dir.glob("*.parquet") if f.is_file() and not f.name.startswith('.')])
    if not files:
        print("❌ Không tìm thấy file.")
        return

    first_data_ts = None # Thời gian dữ liệu đầu tiên
    wall_clock_start = None # Thời gian thực khi bắt đầu
    time_skip_accumulation = 0 # Tổng thời gian nhảy cóc

    print(f"🚀 Bắt đầu Replay với tốc độ: x{SPEED_FACTOR}")
    
    for file_path in files:
        print(f"\n📖 Đọc file: {file_path.name}")
        parquet_file = pq.ParquetFile(file_path)

        for batch in parquet_file.iter_batches(batch_size=BATCH_SIZE):
            records = batch.to_pylist()
            for record in records:
                original_ts_str = record.get(TIMESTAMP_COL)
                if not original_ts_str: continue
                try:
                    if isinstance(original_ts_str, str):
                        current_data_ts = datetime.fromisoformat(original_ts_str)
                    else:
                        current_data_ts = original_ts_str
                except ValueError: continue

                if first_data_ts is None:
                    first_data_ts = current_data_ts
                    wall_clock_start = time.time()

                elapsed_seconds_ts = (current_data_ts - first_data_ts).total_seconds()
                real_elapsed_ts = elapsed_seconds_ts / SPEED_FACTOR
                real_elapsed_ts -= time_skip_accumulation
                target_wall_time = wall_clock_start + real_elapsed_ts #type: ignore
                sleep_duration = target_wall_time - time.time()

                if sleep_duration > 0:
                    if sleep_duration > MAX_SLEEP_SEC and skip_time:
                        skip_amount = sleep_duration - MAX_SLEEP_SEC
                        time_skip_accumulation += skip_amount
                        print(f"⏩ Nhảy cóc {skip_amount:.1f}s...")
                        time.sleep(MAX_SLEEP_SEC)
                    else:
                        time.sleep(sleep_duration)

                record[TIMESTAMP_COL] = datetime.now().isoformat()
                yield record

# ================= MAIN =================
def delivery_report(err, msg):
    if err is not None: print(f'❌ Lỗi: {err}')

def run_producer():
    # 1. KIỂM TRA TOPIC TRƯỚC KHI CHẠY
    ensure_topic_exists()
    
    # 2. KHỞI TẠO PRODUCER
    print("🔌 Khởi tạo Producer...")
    producer = Producer(CONF)
    
    data_stream = source_data_generator(DATA_DIR, skip_time=False)
    total_sent = 0

    try:
        for record in data_stream:
            msg_value = json.dumps(record, default=str).encode('utf-8')
            producer.produce(TOPIC, value=msg_value, callback=delivery_report)
            producer.poll(0)
            total_sent += 1
            if total_sent % 100 == 0:
                print(f"✅ Sent: {total_sent} | Time: {record[TIMESTAMP_COL]}", end='\r')
        
        producer.flush(10)
        print(f"\n🎉 DONE: {total_sent}")
    except KeyboardInterrupt:
        print("\n🛑 Stopped.")
    except Exception as e:
        print(f"\n💥 Error: {e}")

if __name__ == "__main__":
    run_producer()