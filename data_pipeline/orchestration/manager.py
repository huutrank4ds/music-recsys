import schedule #type: ignore
import time
import subprocess
import os
from common.logger import get_logger

# Cấu hình chung
SPARK_MASTER_CONTAINER = "spark-master"
MASTER = "spark://spark-master:7077"
MONGO_SCRIPT = "/opt/src/ingestion/stream_to_mongo.py"
MINIO_SCRIPT = "/opt/src/ingestion/stream_to_minio.py"
INCREMENTAL_MONGO_SCRIPT = "/opt/src/batch/incremental_update_listen_count.py"
RESYNC_MONGO_SCRIPT = "/opt/src/batch/resync_plays_7d_mongo.py"
TRAIN_ALS_SCRIPT = "/opt/src/modeling/train_als_batch.py"

# Biến môi trường MinIO
MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "http://minio:9000")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY")

# Cờ trạng thái hệ thống 
IS_TRAINING = False 

logger = get_logger("Orchestrator")

def is_job_running(script_name):
    """Kiểm tra process trong container"""
    try:
        cmd = f"docker exec {SPARK_MASTER_CONTAINER} ps -ef | grep -v grep | grep {script_name}"
        subprocess.check_output(cmd, shell=True)
        return True
    except subprocess.CalledProcessError:
        return False

# def start_mongo_stream():
#     """Mongo Stream chạy 24/7, không liên quan đến việc Training"""
#     if is_job_running("stream_to_mongo.py"):
#         return

#     logger.info("[Mongo] Phát hiện Job đã tắt. Đang khởi động lại...")
#     cmd = [
#         "docker", "exec", "-d", SPARK_MASTER_CONTAINER,
#         "spark-submit",
#         "--master", MASTER,
#         "--name", "Stream-Mongo",
#         "--driver-memory", "512m",
#         "--executor-memory", "512m",
#         "--total-executor-cores", "1",
#         MONGO_SCRIPT
#     ]
#     subprocess.run(cmd)
#     logger.info("[Mongo] Đã gửi lệnh restart.")

def start_minio_stream_check():
    """
    Health Check cho MinIO:
    Chỉ restart nếu Job chết VÀ hệ thống KHÔNG trong trạng thái Training.
    """
    global IS_TRAINING
    
    if IS_TRAINING:
        # Nếu đang train, việc job MinIO tắt là ĐÚNG QUY TRÌNH. Bỏ qua check.
        return

    if is_job_running("stream_to_minio.py"):
        return # Đang chạy ổn

    logger.info("[MinIO] Phát hiện Job đã tắt (ngoài giờ train). Đang khởi động lại...")
    
    # Lệnh Start MinIO đầy đủ config
    cmd = [
        "docker", "exec", "-d", SPARK_MASTER_CONTAINER,
        "spark-submit",
        "--master", MASTER,
        "--name", "Stream-MinIO",
        "--driver-memory", "512m",
        "--executor-memory", "512m",
        "--total-executor-cores", "1",
        MINIO_SCRIPT
    ]
    subprocess.run(cmd)
    logger.info("[MinIO] Đã gửi lệnh restart.")

def start_incremental_mongo_job():
    """Khởi động job MongoDB Incremental (chạy 1 lần)"""
    if is_job_running("incremental_update_listen_count.py"):
        logger.info("[Incremental Mongo] Job đang chạy. Bỏ qua khởi động lại.")
        return

    logger.info("[Incremental Mongo] Khởi động job Incremental MongoDB...")
    cmd = [
        "docker", "exec", SPARK_MASTER_CONTAINER,
        "spark-submit",
        "--master", MASTER,
        "--driver-memory", "1g",
        "--executor-memory", "1g",
        INCREMENTAL_MONGO_SCRIPT
    ]
    subprocess.run(cmd)
    logger.info("[Incremental Mongo] Đã gửi lệnh khởi động.")

def resync_plays_7d_mongo_job():
    """Khởi động job Resync Plays 7d MongoDB (chạy 1 lần)"""
    if is_job_running("resync_plays_7d_mongo.py"):
        logger.info("[Resync Plays 7d] Job đang chạy. Bỏ qua khởi động lại.")
        return

    logger.info("[Resync Plays 7d] Khởi động job Resync Plays 7d MongoDB...")
    cmd = [
        "docker", "exec", SPARK_MASTER_CONTAINER,
        "spark-submit",
        "--master", MASTER,
        "--driver-memory", "1g",
        "--executor-memory", "1g",
        RESYNC_MONGO_SCRIPT
    ]
    subprocess.run(cmd, check=True)
    logger.info("[Resync Plays 7d] Đã đồng bộ thành công.")

def train_als_model():
    """Khởi động job train ALS Model (chạy 1 lần)"""
    logger.info("[Train ALS] Khởi động job huấn luyện mô hình ALS...")
    cmd = [
        "docker", "exec", SPARK_MASTER_CONTAINER,
        "spark-submit",
        "--master", MASTER,
        "--driver-memory", "700m",
        "--executor-memory", "700m",
        "--executor-cores", "1",
        "--total-executor-cores", "2",
        TRAIN_ALS_SCRIPT
    ]
    subprocess.run(cmd, check=True)
    logger.info("[Train ALS] Đã huấn luyện thành công.")

def safe_execute(job_func, job_name):
    """Hàm wrapper để chạy các job một cách an toàn"""
    try:
        logger.info(f"--- Bắt đầu: {job_name} ---")
        job_func()
        return True
    except subprocess.CalledProcessError as e:
        logger.error(f"Lỗi tại {job_name}: Exit code {e.returncode}")
    except Exception as e:
        logger.error(f"Lỗi không mong muốn tại {job_name}: {e}")
    return False


def run_pipeline_cycle():
    """Quy trình bảo trì định kỳ"""
    global IS_TRAINING
    logger.info("\n[Orchestrator] Bắt đầu quy trình bảo trì định kỳ...") 
    IS_TRAINING = True # Bật cờ trainning
    logger.info("[Lock] Đã bật chế độ bảo trì. Health Check MinIO sẽ tạm dừng.")
    try:
        # 1. DỌN DẸP TÀI NGUYÊN (Không cần check=True, lỗi pkill thì thôi)
        logger.info("[Cleanup] Dừng các tiến trình đang chạy để lấy RAM...")
        subprocess.run(f"docker exec {SPARK_MASTER_CONTAINER} pkill -f {MINIO_SCRIPT}", shell=True)
        subprocess.run(f"docker exec {SPARK_MASTER_CONTAINER} pkill -f {INCREMENTAL_MONGO_SCRIPT}", shell=True)
        time.sleep(15)

        # 2. THỰC THI TUẦN TỰ (Job sau chỉ chạy khi job trước xong)
        # Resync 7d
        if not safe_execute(resync_plays_7d_mongo_job, "Resync Trending"):
            logger.warning("Resync thất bại, nhưng vẫn sẽ cố gắng tiến hành Training...")

        # Train ALS
        if not safe_execute(train_als_model, "Training ALS"):
            logger.error("Training ALS thất bại hoàn toàn.")
    finally:
        # 3. PHỤC HỒI HỆ THỐNG
        IS_TRAINING = False 
        logger.info("[Recovery] Khôi phục trạng thái vận hành...")
        start_minio_stream_check()
        start_incremental_mongo_job()
        logger.info("[Unlock] Chế độ bảo trì kết thúc.\n")

# Luồng chính
logger.info("Orchestrator khởi động...")
# Kiểm tra ngay lập tức khi bật container
logger.info("Startup Check")
# start_mongo_stream()
start_minio_stream_check()
start_incremental_mongo_job()
logger.info("Kết thúc Startup Check")

# Lên lịch Resync và Training (2h sáng)
schedule.every().day.at("02:00").do(run_pipeline_cycle)

# 3. Lên lịch Health Check
time_checking_stream_interval = 5 # phút
time_checking_incremental_mongo_interval = 15 # phút
# schedule.every(time_checking_interval).minutes.do(start_mongo_stream)       # Check Mongo
schedule.every(time_checking_stream_interval).minutes.do(start_minio_stream_check) # Check MinIO (có điều kiện)
schedule.every(time_checking_incremental_mongo_interval).minutes.do(start_incremental_mongo_job) # Check Incremental MongoDB

logger.info(f"Đã lên lịch giám sát MinIO ({time_checking_stream_interval} phút/lần), Incremental MongoDB ({time_checking_incremental_mongo_interval} phút/lần) và training (02:00 hàng ngày).")

while True:
    schedule.run_pending()
    time.sleep(10)