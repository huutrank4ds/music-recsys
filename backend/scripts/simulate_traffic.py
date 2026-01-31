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
    def __init__(self, data_dir=cfg.MUSIC_LOGS_DATA_PATH, file_name="listening_history.parquet", 
                 speed_factor=200.0, max_sleep_sec=2.0, simulation_lag=5184000000,
                 auto_realtime=True): # Thêm tùy chọn auto_realtime
        self.data_dir = Path(data_dir)
        self.file_name = file_name
        self.speed_factor = speed_factor
        self.max_sleep_sec = max_sleep_sec
        self.auto_realtime = auto_realtime # Lưu trạng thái tùy chọn
        
        # State variables
        self.first_data_ts = None
        self.wall_clock_start = None
        self.time_offset = 0
        self.time_skip_accumulation = 0
        self.simulation_lag = simulation_lag
        self.prev_ts_val = None # Lưu timestamp trước đó để tính gap

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
        ts_val = record.get('timestamp')
        if ts_val is None: return None

        now_ms = int(time.time() * 1000)

        # 1. Khởi tạo các mốc thời gian tại bản ghi đầu tiên
        if self.first_data_ts is None:
            self.first_data_ts = ts_val
            self.wall_clock_start = time.time()
            target_start_ms = now_ms - self.simulation_lag
            self.time_offset = target_start_ms - self.first_data_ts

        # 2. Tính toán Timestamp mới dự kiến cho log
        new_timestamp = int(ts_val + self.time_offset)

        # 3. Kiểm tra điều kiện Real-time (nếu tùy chọn được bật)
        is_reached_now = self.auto_realtime and (new_timestamp >= now_ms)

        if is_reached_now:
            # CHẾ ĐỘ THỜI GIAN THỰC (Real-time mode)
            # Khóa offset để new_timestamp luôn bám sát thời gian thực tại
            self.time_offset = int(time.time() * 1000) - ts_val
            new_timestamp = int(ts_val + self.time_offset)
            
            # Đợi theo khoảng cách thực tế giữa các bài hát (tốc độ x1)
            if self.prev_ts_val:
                real_gap = (ts_val - self.prev_ts_val) / 1000.0
                if real_gap > 0:
                    time.sleep(min(real_gap, self.max_sleep_sec))
            # logger.debug("Real-time mode active: Speed reset to x1.0")
        else:
            # CHẾ ĐỘ TUA NHANH (Turbo/Speed mode)
            elapsed_ms = ts_val - self.first_data_ts
            elapsed_seconds = elapsed_ms / 1000.0
            real_elapsed = (elapsed_seconds / self.speed_factor) - self.time_skip_accumulation
            target_wall_time = self.wall_clock_start + real_elapsed
            
            sleep_duration = target_wall_time - time.time()
            if sleep_duration > 0:
                if sleep_duration > self.max_sleep_sec:
                    skip = sleep_duration - self.max_sleep_sec
                    self.time_skip_accumulation += skip
                    time.sleep(self.max_sleep_sec)
                else:
                    time.sleep(sleep_duration)

        # Cập nhật trạng thái cho bản ghi kế tiếp
        self.prev_ts_val = ts_val

        # 4. Trả về object UserLogRequest
        try:
            return UserLogRequest(
                user_id=str(record['user_id']),
                track_id=str(record['track_id']),
                timestamp=new_timestamp,
                action=record.get('action', "complete"),
                source=record.get('source', "simulation"),
                duration=int(record.get('duration', 300)),
                total_duration=int(record.get('total_duration', 300))
            )
        except Exception as e:
            logger.warning(f"Skipping bad record: {e}")
            return None

# ================= MAIN RUN =================
def run():
    import argparse
    import time # Đảm bảo đã import time
    
    parser = argparse.ArgumentParser()
    parser.add_argument("--speed", type=float, default=200.0, help="Tốc độ replay (mặc định x200)")
    parser.add_argument("--lag", type=int, default=5184000000, help="Độ trễ giả lập tính bằng ms (Mặc định 60 ngày)")
    parser.add_argument("--turbo", action="store_true", help="Chạy tốc độ tối đa (không sleep)")
    parser.add_argument("--auto-realtime", action="store_true", default=True, help="Tự động chuyển sang chế độ real-time khi bắt kịp thời gian hiện tại")
    args = parser.parse_args()
    
    speed_factor = float('inf') if args.turbo else args.speed
    
    logger.info("Đang khởi tạo Kafka Producer...")
    kafka_manager.start()

    # Khởi tạo Player
    player = MusicStreamPlayer(
        data_dir=cfg.MUSIC_LOGS_DATA_PATH,
        speed_factor=speed_factor,
        simulation_lag=args.lag,
        auto_realtime=args.auto_realtime
    )

    total_sent = 0
    batch_start_time = time.time() # [MỚI] Mốc thời gian bắt đầu đợt 1000 tin
    
    try:
        for log_request in player.stream_records():
            if log_request:
                # Serialize sang JSON string -> Bytes
                msg_dict = log_request.dict()
                msg_bytes = json.dumps(msg_dict).encode('utf-8')
                
                kafka_manager.produce(cfg.KAFKA_TOPIC, msg_bytes)
                
                total_sent += 1
                
                # Logic in log mỗi 1000 tin
                if total_sent % 1000 == 0:
                    current_time = time.time()
                    elapsed_time = current_time - batch_start_time
                    
                    # [MỚI] Tính tốc độ (logs/s)
                    if elapsed_time > 0:
                        rate = 1000 / elapsed_time
                    else:
                        rate = 0 # Tránh chia cho 0 nếu quá nhanh

                    # In ra timestamp giả lập để dễ debug
                    ts = log_request.timestamp if log_request.timestamp else 0
                    ts_preview = datetime.fromtimestamp(ts / 1000).strftime('%Y-%m-%d %H:%M:%S')
                    
                    # [CẬP NHẬT] Thêm hiển thị Rate
                    # Sử dụng \033[K để xóa tàn dư dòng cũ nếu dòng mới ngắn hơn
                    logger.info(f"\r[Sim] Sent: {total_sent} | Rate: {rate:6.1f} logs/s | Last TS: {ts_preview} | Speed: x{speed_factor} \033[K")
                    
                    # [MỚI] Reset mốc thời gian cho đợt 1000 tin tiếp theo
                    batch_start_time = current_time

        logger.info(f"\nHOÀN TẤT! Tổng log đã gửi: {total_sent}")

    except KeyboardInterrupt:
        logger.info("\nDừng giả lập.")
    except Exception as e:
        logger.error(f"Lỗi Fatal: {e}")
    finally:
        kafka_manager.stop()

if __name__ == "__main__":
    run()