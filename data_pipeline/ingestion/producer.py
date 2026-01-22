import json
import time
import socket
from datetime import datetime
from pathlib import Path
import pyarrow.parquet as pq
from confluent_kafka import Producer #type: ignore
from confluent_kafka.admin import AdminClient, NewTopic #type: ignore 
import config as cfg
from common.logger import get_logger

logger = get_logger("Kafka_Producer")

# ================= CLASS 1: QUẢN LÝ KAFKA (HẠ TẦNG) =================
class KafkaService:
    def __init__(self):
        self.conf = {
            'bootstrap.servers': cfg.KAFKA_BOOTSTRAP_SERVERS,
            'client.id': socket.gethostname(),
            'acks': '1',
            'linger.ms': 5,
            'batch.size': 16384,
            'compression.type': 'gzip',
        }
        self.topic = cfg.KAFKA_TOPIC
        self.producer = Producer(self.conf)
        self.admin = AdminClient(self.conf)

    def ensure_topic_exists(self, num_partitions=None, replication_factor=None):
        """Kiểm tra và tạo Topic nếu chưa có"""
        # Sử dụng cấu hình từ file configs nếu không truyền vào
        if num_partitions is None:
            num_partitions = cfg.KAFKA_NUM_PARTITIONS
        if replication_factor is None:
            replication_factor = cfg.KAFKA_REPLICATION_FACTOR
        
        # Kiểm tra tồn tại Topic
        logger.info(f"[Kafka] Đang kiểm tra Topic '{self.topic}'...")
        metadata = self.admin.list_topics(timeout=10)

        if self.topic in metadata.topics:
            logger.info(f"[Kafka] Topic '{self.topic}' đã tồn tại.")
        else:
            logger.warning(f"[Kafka] Topic chưa có. Đang tạo mới...")
            new_topic = NewTopic(self.topic, num_partitions, replication_factor)
            fs = self.admin.create_topics([new_topic])
            for topic, future in fs.items():
                try:
                    future.result()
                    logger.info(f"[Kafka] Đã tạo thành công topic: {topic}")
                except Exception as e:
                    logger.error(f"[Kafka] Không thể tạo topic {topic}: {e}")

    def send_message(self, record):
        """Gửi 1 bản ghi vào Kafka"""
        try:
            msg_value = json.dumps(record, default=str).encode('utf-8')
            self.producer.produce(self.topic, value=msg_value)
            self.producer.poll(0)
        except BufferError:
            logger.warning("[Kafka] Hàng đợi đầy, đang chờ xả bớt...")
            self.producer.poll(1) # Chờ 1s để giải phóng bộ đệm

    def close(self):
        logger.info("[Kafka] Đang đóng Producer...")
        self.producer.flush(10)

# ================= TRÌNH TẠO LOG NGƯỜI DÙNG =================
class MusicStreamPlayer:
    def __init__(self, data_dir, speed_factor=200.0, max_sleep_sec=2.0):
        self.data_dir = Path(data_dir)
        self.speed_factor = speed_factor
        self.max_sleep_sec = max_sleep_sec
        # State variables
        self.first_data_ts = None
        self.wall_clock_start = None
        self.time_skip_accumulation = 0

    def stream_records(self):
        """Generator trả về từng dòng dữ liệu đã được căn chỉnh thời gian"""
        # Sử dụng config đường dẫn từ file Config (cần bỏ prefix file:// nếu có)
        clean_path = str(self.data_dir).replace("file://", "")
        path_obj = Path(clean_path)
        
        files = sorted([f for f in path_obj.glob("*.parquet") if f.is_file() and not f.name.startswith('.')])
        
        if not files:
            logger.error(f"[MusicStreamPlayer] Không tìm thấy file parquet nào tại: {clean_path}")
            return

        logger.info(f"[MusicStreamPlayer] Bắt đầu Replay {len(files)} file với tốc độ: x{self.speed_factor}")
        
        for file_path in files:
            logger.info(f"[MusicStreamPlayer] Đang đọc file: {file_path.name}")
            try:
                pf = pq.ParquetFile(file_path)
            except Exception as e:
                logger.error(f"[MusicStreamPlayer] Lỗi đọc file {file_path.name}: {e}")
                continue

            for batch in pf.iter_batches(batch_size=2000):
                records = batch.to_pylist()
                for record in records:
                    record_after_sleep = self._process_time_travel(record)
                    
                    if not record_after_sleep:
                        continue
                    final_record = self._normalize_record(record_after_sleep)
                    if final_record:
                        yield final_record

    def _normalize_record(self, record: dict) -> dict:
        """
        Map dữ liệu thô từ Parquet sang Schema chuẩn của hệ thống.
        Schema này PHẢI KHỚP với API Backend (app/api/logging.py).
        """
        try:
            # 1. Map ID (Xử lý trường hợp file parquet dùng tên cột khác)
            # Ưu tiên lấy user_id, nếu không có thì tìm userId, không có nữa thì bỏ qua
            user_id = str(record.get('user_id'))
            track_id = str(record.get('musicbrainz_track_id'))

            # Nếu dữ liệu rác không có ID thì bỏ qua
            if not user_id or not track_id:
                return None # type: ignore

            # 2. Chuẩn hóa Timestamp (về Milliseconds Int giống API)
            # _process_time_travel đã update 'timestamp' thành chuỗi ISO hiện tại
            # Ta convert nó sang Unix Timestamp (ms)
            iso_ts = record.get('timestamp')
            if isinstance(iso_ts, str):
                dt = datetime.fromisoformat(iso_ts)
                ts_ms = int(dt.timestamp() * 1000)
            else:
                ts_ms = int(time.time() * 1000)

            # 3. Trả về Dictionary đúng chuẩn
            return {
                "user_id": user_id,
                "track_id": track_id,
                "timestamp": ts_ms,          # Kiểu Int (Epoch millis)
                "action": str(record.get('action', 'listen')), # Mặc định là listen
                "duration": int(record.get('duration', 5*60000)), # Độ dài hành động hoặc mặc định 5 phút
                "total_duration": int(record.get('total_duration', 5*60000)), # Tổng độ dài bài hoặc mặc định 5 phút
                "source": "simulation"       # Tag phân biệt
            }
        except Exception:
            return None # type: ignore

    def _process_time_travel(self, record):
        """Xử lý logic Time Travel cho từng bản ghi"""
        ts_val = record.get('timestamp')
        if not ts_val: return record

        # Parse timestamp
        try:
            current_data_ts = datetime.fromisoformat(ts_val) if isinstance(ts_val, str) else ts_val
        except ValueError:
            return record

        # Init mốc thời gian
        if self.first_data_ts is None:
            self.first_data_ts = current_data_ts
            self.wall_clock_start = time.time()

        # Tính toán độ trễ
        elapsed_seconds = (current_data_ts - self.first_data_ts).total_seconds()
        real_elapsed = (elapsed_seconds / self.speed_factor) - self.time_skip_accumulation
        target_time = self.wall_clock_start + real_elapsed #type: ignore
        
        sleep_duration = target_time - time.time()

        if sleep_duration > 0:
            if sleep_duration > self.max_sleep_sec:
                skip = sleep_duration - self.max_sleep_sec
                self.time_skip_accumulation += skip
                logger.info(f"Nhảy cóc {skip:.1f}s ...")
                time.sleep(self.max_sleep_sec)
            else:
                time.sleep(sleep_duration)

        record['timestamp'] = datetime.now().isoformat()
        return record

# ================= MAIN PROGRAM =================
def run():
    import argparse
    
    parser = argparse.ArgumentParser(description="Kafka Music Log Producer")
    parser.add_argument("--boost", action="store_true", help="Chế độ nhanh hơn một chút (500x)")
    parser.add_argument("--fast", action="store_true", help="Chế độ nhanh (2000x speed)")
    parser.add_argument("--turbo", action="store_true", help="Chế độ MAX speed (không delay)")
    parser.add_argument("--speed", type=float, default=200.0, help="Tốc độ tùy chỉnh (mặc định: 200x)")
    args = parser.parse_args()
    
    # Xác định speed_factor
    if args.turbo:
        speed_factor = float('inf')  # Không delay
        mode_name = "TURBO (MAX)"
    elif args.fast:
        speed_factor = 2000.0  # Nhanh
        mode_name = "FAST (2000x)"
    elif args.boost:
        speed_factor = 500.0  # Nhanh hơn một chút
        mode_name = "BOOST (500x)"
    else:
        speed_factor = args.speed
        mode_name = f"REALTIME ({speed_factor}x)"
    
    logger.info(f" Chế độ: {mode_name}")
    
    # 1. Khởi tạo Service
    kafka_svc = KafkaService()
    kafka_svc.ensure_topic_exists()

    # 2. Khởi tạo Player
    player = MusicStreamPlayer(
        data_dir=cfg.MUSIC_LOGS_DATA_PATH,
        speed_factor=speed_factor
    )

    # 3. Chạy vòng lặp chính
    total_sent = 0
    start_time = time.time()
    
    try:
        for record in player.stream_records():
            if record:
                kafka_svc.send_message(record)
                total_sent += 1
                if total_sent % 1000 == 0:
                    elapsed = time.time() - start_time
                    rate = total_sent / elapsed if elapsed > 0 else 0
                    # Dùng \r để cập nhật dòng hiện tại
                    print(f"\rSent: {total_sent:,} records | Speed: {rate:.0f} msg/s", end='', flush=True)
        
        elapsed = time.time() - start_time
        rate = total_sent / elapsed if elapsed > 0 else 0
        print()  # Xuống dòng
        logger.info(f"HOÀN TẤT! Tổng: {total_sent:,} | Thời gian: {elapsed:.1f}s | Tốc độ: {rate:.0f} msg/s")

    except KeyboardInterrupt:
        logger.info("\nDừng chương trình theo yêu cầu.")
    except Exception as e:
        logger.error(f"Lỗi không mong muốn: {e}")
    finally:
        kafka_svc.close()

if __name__ == "__main__":
    run()