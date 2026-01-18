"""
BALANCED PRODUCER - Cân bằng giữa tốc độ và tính realtime
=========================================================
- Giữ thứ tự timestamp (quan trọng cho time-series)
- Gửi theo micro-batch (nhanh hơn từng event)
- Nhảy qua khoảng trống lớn (skip idle time)
- Tốc độ: ~1000-5000 msg/s (nhanh hơn Normal 1000x, chậm hơn TURBO 2-10x)
"""
import json
import socket
import time
from datetime import datetime
import pyarrow.parquet as pq
from pathlib import Path
from confluent_kafka import Producer
from confluent_kafka.admin import AdminClient, NewTopic

# ================= CẤU HÌNH =================
# Tốc độ tăng tốc (x1000 = 1 giờ data chạy trong 3.6s)
SPEED_FACTOR = 1000.0

# Micro-batch: Gửi bao nhiêu events trước khi check time
MICRO_BATCH_SIZE = 500

# Thời gian tối đa chờ giữa các batch (giây thực)
MAX_WAIT_BETWEEN_BATCHES = 0.5

# Nhảy qua khoảng trống lớn hơn X phút (trong data)
SKIP_GAP_MINUTES = 5

# ================= CẤU HÌNH KAFKA (TỐI ƯU) =================
CONF = {
    'bootstrap.servers': 'kafka:9092',
    'client.id': socket.gethostname(),
    'acks': '1',
    'linger.ms': 20,
    'batch.size': 65536,  # 64KB
    'compression.type': 'lz4',
    'queue.buffering.max.messages': 50000,
}

TOPIC = "music_log"
DATA_DIR = Path('/opt/data/processed_sorted')
TIMESTAMP_COL = 'timestamp'
NUM_PARTITIONS = 4
REPLICATION_FACTOR = 1

# ================= HÀM QUẢN LÝ TOPIC =================
def ensure_topic_exists():
    print(f"🔧 Đang kiểm tra Topic '{TOPIC}'...")
    admin_client = AdminClient({'bootstrap.servers': CONF['bootstrap.servers']})
    cluster_metadata = admin_client.list_topics(timeout=10)
    
    if TOPIC in cluster_metadata.topics:
        print(f"✅ Topic '{TOPIC}' đã tồn tại.")
    else:
        print(f"⚠️ Đang tạo topic với {NUM_PARTITIONS} partitions...")
        new_topic = NewTopic(topic=TOPIC, num_partitions=NUM_PARTITIONS, replication_factor=REPLICATION_FACTOR)
        fs = admin_client.create_topics([new_topic])
        for topic, future in fs.items():
            try:
                future.result()
                print(f"🎉 Đã tạo topic: {topic}")
            except Exception as e:
                print(f"❌ Lỗi tạo topic: {e}")

# ================= BALANCED GENERATOR =================
def balanced_data_generator(data_dir):
    """
    Generator với cơ chế:
    1. Đọc theo micro-batch (không phải từng event)
    2. Giữ thứ tự timestamp
    3. Tăng tốc theo SPEED_FACTOR
    4. Nhảy qua khoảng trống lớn
    """
    files = sorted([f for f in data_dir.glob("*.parquet") if f.is_file() and not f.name.startswith('.')])
    if not files:
        print("❌ Không tìm thấy file.")
        return

    print(f"⚡ BALANCED MODE: x{SPEED_FACTOR} speed, batch {MICRO_BATCH_SIZE}")
    print(f"   Skip gaps > {SKIP_GAP_MINUTES} minutes")
    
    first_data_ts = None
    wall_clock_start = None
    total_skipped_time = 0
    last_data_ts = None
    
    for file_path in files:
        print(f"\n📖 Đọc file: {file_path.name}")
        parquet_file = pq.ParquetFile(file_path)

        for batch in parquet_file.iter_batches(batch_size=MICRO_BATCH_SIZE):
            records = batch.to_pylist()
            batch_records = []
            
            for record in records:
                original_ts_str = record.get(TIMESTAMP_COL)
                if not original_ts_str:
                    continue
                    
                try:
                    if isinstance(original_ts_str, str):
                        current_data_ts = datetime.fromisoformat(original_ts_str)
                    else:
                        current_data_ts = original_ts_str
                except ValueError:
                    continue

                # Khởi tạo lần đầu
                if first_data_ts is None:
                    first_data_ts = current_data_ts
                    wall_clock_start = time.time()
                    last_data_ts = current_data_ts

                # Kiểm tra có gap lớn không
                if last_data_ts:
                    gap_seconds = (current_data_ts - last_data_ts).total_seconds()
                    if gap_seconds > SKIP_GAP_MINUTES * 60:
                        skip_amount = gap_seconds - 1  # Giữ lại 1 giây
                        total_skipped_time += skip_amount
                        print(f"⏩ Skip gap {gap_seconds/60:.1f} phút")

                last_data_ts = current_data_ts
                
                # QUAN TRỌNG: Giữ timestamp gốc (không ghi đè)
                batch_records.append(record)

            # Yield toàn bộ batch
            if batch_records:
                # Tính thời gian cần chờ cho batch này
                if first_data_ts and wall_clock_start:
                    batch_end_ts = last_data_ts
                    elapsed_data_seconds = (batch_end_ts - first_data_ts).total_seconds() - total_skipped_time
                    target_wall_time = wall_clock_start + (elapsed_data_seconds / SPEED_FACTOR)
                    sleep_time = target_wall_time - time.time()
                    
                    if 0 < sleep_time <= MAX_WAIT_BETWEEN_BATCHES:
                        time.sleep(sleep_time)
                    elif sleep_time > MAX_WAIT_BETWEEN_BATCHES:
                        # Nếu cần chờ quá lâu, chỉ chờ max
                        time.sleep(MAX_WAIT_BETWEEN_BATCHES)
                
                yield batch_records

# ================= MAIN =================
def delivery_report(err, msg):
    if err is not None:
        print(f'❌ Lỗi: {err}')

def run_producer():
    start_time = time.time()
    
    ensure_topic_exists()
    
    print("🔌 Khởi tạo Producer BALANCED...")
    producer = Producer(CONF)
    
    total_sent = 0
    batch_count = 0

    try:
        for batch_records in balanced_data_generator(DATA_DIR):
            # Gửi toàn bộ batch nhanh chóng
            for record in batch_records:
                msg_value = json.dumps(record, default=str).encode('utf-8')
                producer.produce(TOPIC, value=msg_value, callback=delivery_report)
                total_sent += 1
            
            # Poll để xử lý callbacks
            producer.poll(0)
            batch_count += 1
            
            # Log mỗi 10 batch
            if batch_count % 10 == 0:
                elapsed = time.time() - start_time
                rate = total_sent / elapsed if elapsed > 0 else 0
                print(f"📊 Sent: {total_sent:,} | Rate: {rate:,.0f} msg/s | Elapsed: {elapsed:.1f}s")
        
        producer.flush(30)
        elapsed = time.time() - start_time
        rate = total_sent / elapsed if elapsed > 0 else 0
        print(f"\n🎉 DONE: {total_sent:,} messages in {elapsed:.1f}s ({rate:,.0f} msg/s)")
        
    except KeyboardInterrupt:
        print("\n🛑 Stopped.")
    except Exception as e:
        print(f"\n💥 Error: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    run_producer()
