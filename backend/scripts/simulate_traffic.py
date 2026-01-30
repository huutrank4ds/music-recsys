# scripts/simulate_traffic.py
import time
import json
import pyarrow.parquet as pq
from pathlib import Path
from pydantic import ValidationError #type: ignore
from typing import Optional
from datetime import datetime

import config as cfg
from common.logger import get_logger
from app.core.kafka_client import kafka_manager
from common.schemas.log_schemas import UserLogRequest 


NAME_TASK = "Simulate Traffic"
logger = get_logger(NAME_TASK)

class MusicStreamPlayer:
    def __init__(self, data_dir=cfg.MUSIC_LOGS_DATA_PATH, file_name="listening_history.parquet", speed_factor=200.0, max_sleep_sec=2.0, simulation_lag=5184000000):
        self.data_dir = Path(data_dir)
        self.file_name = file_name
        self.speed_factor = speed_factor
        self.max_sleep_sec = max_sleep_sec
        
        # State variables
        self.first_data_ts = None     # Timestamp gốc của bản ghi đầu tiên trong file
        self.wall_clock_start = None  # Thời gian thực tế lúc bắt đầu chạy script
        self.time_offset = 0          # Độ lệch cần cộng vào timestamp gốc
        self.time_skip_accumulation = 0
        self.simulation_lag = simulation_lag # Độ trễ giả lập (mặc định 2 tháng = 5184000000 ms)

    def stream_records(self):
        """Generator trả về UserLogRequest"""
        file_path = self.data_dir / self.file_name 
        
        if not file_path.exists():
            logger.error(f"Không tìm thấy file parquet tại: {file_path}")
            return

        logger.info(f"Bắt đầu Replay file {file_path.name} | Speed: x{self.speed_factor} | Lag: {self.simulation_lag/1000/60/60/24:.1f} days")
        
        try:
            pf = pq.ParquetFile(file_path)
        except Exception as e:
            logger.error(f"Lỗi đọc file {file_path.name}: {e}")
            return

        for batch in pf.iter_batches(batch_size=2000):
            records = batch.to_pylist()
            for record in records:
                # Hàm này giờ sẽ xử lý cả việc sleep và convert sang Model
                log_request = self._process_time_travel(record)
                
                if log_request:
                    yield log_request

    def _process_time_travel(self, record: dict) -> Optional[UserLogRequest]:
        """
        1. Tính toán thời gian sleep để giả lập tốc độ.
        2. Tính timestamp mới = (RealTime_Start - Lag) + (Relative_Time_In_Log).
        3. Trả về UserLogRequest.
        """
        ts_val = record.get('timestamp')
        
        # Bỏ qua nếu dữ liệu lỗi
        if ts_val is None: return None

        # Khởi tạo offset và thời gian bắt đầu từ bản ghi đầu tiên
        if self.first_data_ts is None:
            self.first_data_ts = ts_val
            self.wall_clock_start = time.time() # Thời gian thực tế (giây)

            # Target Start = Hiện tại - Độ trễ (Lag)
            now_ms = int(self.wall_clock_start * 1000)
            target_start_ms = now_ms - self.simulation_lag

            # Offset = Thời điểm mong muốn - Timestamp gốc
            self.time_offset = target_start_ms - self.first_data_ts

        # Logic sleep để giả lập tốc độ
        # Khoảng thời gian đã trôi qua trong log gốc (ms)
        elapsed_ms = ts_val - self.first_data_ts
        # Quy đổi ra giây để tính toán sleep
        elapsed_seconds = elapsed_ms / 1000.0
        # Thời gian thực tế cần đạt được
        real_elapsed = (elapsed_seconds / self.speed_factor) - self.time_skip_accumulation
        target_wall_time = self.wall_clock_start + real_elapsed
        
        sleep_duration = target_wall_time - time.time()
        if sleep_duration > 0:
            if sleep_duration > self.max_sleep_sec:
                # Nếu phải ngủ quá lâu, thì bỏ qua bớt (skip) để tua nhanh
                skip = sleep_duration - self.max_sleep_sec
                self.time_skip_accumulation += skip
                time.sleep(self.max_sleep_sec)
            else:
                time.sleep(sleep_duration)

        # Tạo timestamp mới và object UserLogRequest
        try:
            # Timestamp mới = Timestamp cũ + Offset
            new_timestamp = int(ts_val + self.time_offset)
            
            return UserLogRequest(
                user_id=str(record['user_id']),
                track_id=str(record['track_id']),
                timestamp=new_timestamp,
                action=record.get('action', "complete"),
                source=record.get('source', "simulation"),
                duration=int(record.get('duration', 300)),
                total_duration=int(record.get('total_duration', 300))
            )
        except (ValidationError, ValueError, TypeError) as e:
            logger.warning(f"Skipping bad record: {e}")
            return None

# ================= MAIN RUN =================
def run():
    import argparse
    
    parser = argparse.ArgumentParser()
    parser.add_argument("--speed", type=float, default=200.0, help="Tốc độ replay (mặc định x200)")
    parser.add_argument("--lag", type=int, default=5184000000, help="Độ trễ giả lập tính bằng ms (Mặc định 60 ngày)")
    parser.add_argument("--turbo", action="store_true", help="Chạy tốc độ tối đa (không sleep)")
    args = parser.parse_args()
    
    speed_factor = float('inf') if args.turbo else args.speed
    
    logger.info("Đang khởi tạo Kafka Producer...")
    kafka_manager.start()

    # Khởi tạo Player
    player = MusicStreamPlayer(
        data_dir=cfg.MUSIC_LOGS_DATA_PATH,
        speed_factor=speed_factor,
        simulation_lag=args.lag
    )

    total_sent = 0
    
    try:
        for log_request in player.stream_records():
            if log_request:
                # log_request lúc này đã là object UserLogRequest hoàn chỉnh với timestamp mới
                
                # Serialize sang JSON string -> Bytes
                msg_dict = log_request.dict()
                msg_bytes = json.dumps(msg_dict).encode('utf-8')
                
                kafka_manager.produce(cfg.KAFKA_TOPIC, msg_bytes)
                
                total_sent += 1
                if total_sent % 1000 == 0:
                    # In ra timestamp giả lập để dễ debug
                    ts = log_request.timestamp if log_request.timestamp else 0
                    ts_preview = datetime.fromtimestamp(ts / 1000).strftime('%Y-%m-%d %H:%M:%S')
                    logger.info(f"\r[Sim] Sent: {total_sent} logs | Last TS: {ts_preview} | Speed: x{speed_factor}")

        logger.info(f"HOÀN TẤT! Tổng log đã gửi: {total_sent}")

    except KeyboardInterrupt:
        logger.info("Dừng giả lập.")
    except Exception as e:
        logger.error(f"Lỗi Fatal: {e}")
    finally:
        kafka_manager.stop()

if __name__ == "__main__":
    run()