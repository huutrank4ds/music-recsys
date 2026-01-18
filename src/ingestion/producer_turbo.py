"""
TURBO PRODUCER - Gửi dữ liệu với tốc độ tối đa (không giả lập thời gian thực)
"""
import json
import socket
from datetime import datetime
import pyarrow.parquet as pq
from pathlib import Path
from confluent_kafka import Producer
from confluent_kafka.admin import AdminClient, NewTopic

# ================= CẤU HÌNH KAFKA (ĐÃ TỐI ƯU) =================
CONF = {
    'bootstrap.servers': 'kafka:9092',
    'client.id': socket.gethostname(),
    'acks': '1',                    # Chỉ cần 1 broker xác nhận
    'linger.ms': 50,                # Chờ 50ms để gom batch (tăng từ 5)
    'batch.size': 131072,           # Batch 128KB (tăng từ 16KB)
    'compression.type': 'lz4',      # LZ4 nhanh hơn gzip
    'queue.buffering.max.messages': 100000,  # Buffer lớn hơn
    'queue.buffering.max.kbytes': 1048576,   # 1GB buffer
}

TOPIC = "music_log"
DATA_DIR = Path('/opt/data/processed_sorted')
TIMESTAMP_COL = 'timestamp'
NUM_PARTITIONS = 4
REPLICATION_FACTOR = 1

BATCH_SIZE = 10000  # Tăng batch size đọc file (từ 2000 lên 10000)
FLUSH_EVERY = 50000  # Flush sau mỗi 50k messages

# ================= HÀM QUẢN LÝ TOPIC =================
def ensure_topic_exists():
    print(f"🔧 Đang kiểm tra Topic '{TOPIC}'...")
    admin_client = AdminClient({'bootstrap.servers': CONF['bootstrap.servers']})
    cluster_metadata = admin_client.list_topics(timeout=10)
    
    if TOPIC in cluster_metadata.topics:
        print(f" Topic '{TOPIC}' đã tồn tại.")
    else:
        print(f"Topic chưa có. Đang tạo mới với {NUM_PARTITIONS} partitions...")
        new_topic = NewTopic(
            topic=TOPIC, 
            num_partitions=NUM_PARTITIONS, 
            replication_factor=REPLICATION_FACTOR
        )
        fs = admin_client.create_topics([new_topic])
        for topic, future in fs.items():
            try:
                future.result()
                print(f" Đã tạo thành công topic: {topic}")
            except Exception as e:
                print(f" Không thể tạo topic {topic}: {e}")

# ================= TURBO GENERATOR (KHÔNG CÓ DELAY) =================
def turbo_data_generator(data_dir):
    """Generator đọc dữ liệu KHÔNG có delay - tốc độ tối đa"""
    files = sorted([f for f in data_dir.glob("*.parquet") if f.is_file() and not f.name.startswith('.')])
    if not files:
        print("Không tìm thấy file.")
        return

    print(f"🚀 TURBO MODE: Đọc {len(files)} files với tốc độ TỐI ĐA!")
    
    for file_path in files:
        print(f"\n📖 Đọc file: {file_path.name}")
        parquet_file = pq.ParquetFile(file_path)

        for batch in parquet_file.iter_batches(batch_size=BATCH_SIZE):
            records = batch.to_pylist()
            for record in records:
                # Cập nhật timestamp thành thời điểm hiện tại
                record[TIMESTAMP_COL] = datetime.now().isoformat()
                yield record

# ================= MAIN =================
def delivery_report(err, msg):
    if err is not None: 
        print(f'Lỗi: {err}')

def run_producer():
    import time
    start_time = time.time()
    
    # 1. KIỂM TRA TOPIC
    ensure_topic_exists()
    
    # 2. KHỞI TẠO PRODUCER
    print("🔌 Khởi tạo Producer TURBO...")
    producer = Producer(CONF)
    
    data_stream = turbo_data_generator(DATA_DIR)
    total_sent = 0

    try:
        for record in data_stream:
            msg_value = json.dumps(record, default=str).encode('utf-8')
            producer.produce(TOPIC, value=msg_value, callback=delivery_report)
            
            # Poll không đợi để tối đa throughput
            producer.poll(0)
            total_sent += 1
            
            # Flush định kỳ để tránh buffer đầy
            if total_sent % FLUSH_EVERY == 0:
                producer.flush(timeout=5)
                elapsed = time.time() - start_time
                rate = total_sent / elapsed
                print(f" Sent: {total_sent:,} | Rate: {rate:,.0f} msg/s | Elapsed: {elapsed:.1f}s")
        
        producer.flush(30)
        elapsed = time.time() - start_time
        rate = total_sent / elapsed if elapsed > 0 else 0
        print(f"\n DONE: {total_sent:,} messages in {elapsed:.1f}s ({rate:,.0f} msg/s)")
        
    except KeyboardInterrupt:
        print("\n Stopped.")
    except Exception as e:
        print(f"\n Error: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    run_producer()
