import sys
import os
import time
import json
import pyarrow.parquet as pq
from pathlib import Path
from datetime import datetime
from pydantic import ValidationError #type: ignore
from typing import Optional
import config as cfg
from common.logger import get_logger
from app.core.kafka_client import kafka_manager
from common.schemas.log_schemas import UserLogRequest 

NAME_TASK = "Simulate Traffic"
logger = get_logger(NAME_TASK)

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
        """Generator trả về UserLogRequest"""
        clean_path = str(cfg.MUSIC_LOGS_DATA_PATH).replace("file://", "")
        path_obj = Path(clean_path)
        
        files = sorted([f for f in path_obj.glob("*.parquet") if f.is_file() and not f.name.startswith('.')])
        
        if not files:
            logger.error(f"[{NAME_TASK}] Không tìm thấy file parquet tại: {clean_path}")
            return

        logger.info(f"[{NAME_TASK}] Bắt đầu Replay {len(files)} file | Speed: x{self.speed_factor}")
        
        for file_path in files:
            logger.info(f"[{NAME_TASK}] Reading: {file_path.name}")
            try:
                pf = pq.ParquetFile(file_path)
            except Exception as e:
                logger.error(f"Lỗi đọc file {file_path.name}: {e}")
                continue

            for batch in pf.iter_batches(batch_size=2000):
                records = batch.to_pylist()
                for record in records:
                    # 1. Xử lý trôi thời gian (Time Travel)
                    record_after_sleep = self._process_time_travel(record)
                    if not record_after_sleep: continue
                    
                    # 2. Convert sang UserLogRequest
                    log_request = self._normalize_record(record_after_sleep)
                    if log_request:
                        yield log_request

    def _normalize_record(self, record: dict) -> Optional[UserLogRequest]:
        """
        Map dữ liệu thô -> Pydantic Model (UserLogRequest)
        """
        try:
            # --- Mapping Fields ---
            # Lưu ý: Parquet có thể dùng tên cột khác nhau (userId vs user_id)
            user_id = str(record.get('user_id') or record.get('userId'))
            # Ưu tiên musicbrainz_track_id, nếu không có thì lấy track_id
            track_id = str(record.get('musicbrainz_track_id') or record.get('track_id'))

            if not user_id or not track_id or user_id == 'None' or track_id == 'None':
                return None

            # Tính toán Duration (Giây)
            # Nếu trong data không có duration, giả định 5 phút (300s)
            duration_val = record.get('duration')
            duration = int(duration_val) if duration_val else 300

            # Xử lý Timestamp (Convert ISO string -> Milliseconds Int)
            iso_ts = record.get('timestamp')
            if isinstance(iso_ts, str):
                dt = datetime.fromisoformat(iso_ts)
                ts_ms = int(dt.timestamp() * 1000)
            else:
                ts_ms = int(time.time() * 1000)

            # --- TẠO OBJECT PYDANTIC ---
            # Action giả lập mặc định là 'complete' hoặc 'listen'
            return UserLogRequest(
                user_id=user_id,
                track_id=track_id,
                timestamp=ts_ms,
                action="complete",      # Giả lập hành vi nghe hết bài
                source="simulation",    # Đánh dấu nguồn giả
                duration=duration,      # Đã nghe hết (vì action=complete)
                total_duration=duration # Tổng thời lượng
            )
        except (ValidationError, ValueError, TypeError):
            return None 

    def _process_time_travel(self, record):
        """Giữ nguyên logic Time Travel của bạn"""
        ts_val = record.get('timestamp')
        if not ts_val: return record

        try:
            current_data_ts = datetime.fromisoformat(ts_val) if isinstance(ts_val, str) else ts_val
        except ValueError:
            return record

        if self.first_data_ts is None:
            self.first_data_ts = current_data_ts
            self.wall_clock_start = time.time()

        elapsed_seconds = (current_data_ts - self.first_data_ts).total_seconds()
        real_elapsed = (elapsed_seconds / self.speed_factor) - self.time_skip_accumulation
        target_time = self.wall_clock_start + real_elapsed #type: ignore
        
        sleep_duration = target_time - time.time()

        if sleep_duration > 0:
            if sleep_duration > self.max_sleep_sec:
                skip = sleep_duration - self.max_sleep_sec
                self.time_skip_accumulation += skip
                # logger.debug(f"Skip {skip:.1f}s")
                time.sleep(self.max_sleep_sec)
            else:
                time.sleep(sleep_duration)

        # Cập nhật lại timestamp thành thời gian thực tế hiện tại
        record['timestamp'] = datetime.now().isoformat()
        return record

# ================= MAIN RUN =================
def run():
    import argparse
    
    # ... (Phần ArgumentParser giữ nguyên) ...
    parser = argparse.ArgumentParser()
    parser.add_argument("--speed", type=float, default=200.0)
    parser.add_argument("--turbo", action="store_true")
    args = parser.parse_args()
    
    speed_factor = float('inf') if args.turbo else args.speed
    
    # 1. Khởi động Kafka Client (Sử dụng biến kafka_manager từ app/core)
    # Lưu ý: kafka_manager trong core thường load config từ ENV. 
    # Đảm bảo bạn chạy script này trong Docker hoặc đã set biến môi trường.
    logger.info("🚀 Đang khởi tạo Kafka Producer...")
    kafka_manager.start()

    # 2. Khởi tạo Player
    player = MusicStreamPlayer(
        data_dir=cfg.MUSIC_LOGS_DATA_PATH,
        speed_factor=speed_factor
    )

    total_sent = 0
    start_time = time.time()

    try:
        for log_data in player.stream_records():
            if log_data:
                spark_msg = {
                    "user_id": str(log_data.user_id),
                    "track_id": str(log_data.track_id),
                    "timestamp": log_data.timestamp or int(time.time() * 1000),
                    "action": str(log_data.action),
                    "source": str(log_data.source or "simulated"),
                    "duration": int(log_data.duration),
                    "total_duration": int(log_data.total_duration)
                }

                # --- GỬI VÀO KAFKA ---
                # kafka_manager đã có sẵn serializer json.dumps bên trong (như các bài trước)
                # nên ta chỉ cần gửi dict vào.
                msg_bytes = json.dumps(spark_msg).encode('utf-8')
                kafka_manager.produce(cfg.KAFKA_TOPIC, msg_bytes)
                
                total_sent += 1
                if total_sent % 500 == 0:
                    print(f"\rSent: {total_sent} logs...", end='', flush=True)

        logger.info(f"\n✅ HOÀN TẤT! Tổng log đã gửi: {total_sent}")

    except KeyboardInterrupt:
        logger.info("\n🛑 Dừng giả lập.")
    except Exception as e:
        logger.error(f"Lỗi Fatal: {e}")
    finally:
        kafka_manager.stop() # Hoặc .close() tùy vào implementation trong core của bạn

if __name__ == "__main__":
    run()