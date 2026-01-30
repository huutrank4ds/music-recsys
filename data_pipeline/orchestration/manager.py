import schedule #type: ignore
import time
import subprocess
import os
import sys
from common.logger import get_logger

# --- CẤU HÌNH ---
SPARK_MASTER_CONTAINER = "spark-master"
MASTER = "spark://spark-master:7077"
MONGO_SCRIPT = "/opt/src/ingestion/stream_to_mongo.py"
MINIO_SCRIPT = "/opt/src/ingestion/stream_to_minio.py"

# Biến môi trường MinIO
MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "http://minio:9000")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY")

# --- TRẠNG THÁI HỆ THỐNG (FLAG) ---
# Biến này cực quan trọng: Để ngăn Health Check bật lại job trong lúc đang train
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

def start_mongo_stream():
    """Mongo Stream chạy 24/7, không liên quan đến việc Training"""
    if is_job_running("stream_to_mongo.py"):
        return

    logger.info("[Mongo] Phát hiện Job đã tắt. Đang khởi động lại...")
    cmd = [
        "docker", "exec", "-d", SPARK_MASTER_CONTAINER,
        "spark-submit",
        "--master", MASTER,
        "--name", "Stream-Mongo",
        "--driver-memory", "512m",
        "--executor-memory", "512m",
        "--total-executor-cores", "1",
        "--conf", "spark.kafka.consumer.cache.enabled=false",
        MONGO_SCRIPT
    ]
    subprocess.run(cmd)
    logger.info("[Mongo] Đã gửi lệnh restart.")

def start_minio_stream_check():
    """
    Health Check cho MinIO:
    Chỉ restart nếu Job chết VÀ hệ thống KHÔNG trong trạng thái Training.
    """
    global IS_TRAINING
    
    if IS_TRAINING:
        # Nếu đang train, việc job MinIO tắt là ĐÚNG QUY TRÌNH. Bỏ qua check.
        # print("[MinIO Check] Đang trong chế độ Training. Bỏ qua health check.")
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
        "--conf", "spark.kafka.consumer.cache.enabled=false",
        f"--conf", f"spark.hadoop.fs.s3a.endpoint={MINIO_ENDPOINT}",
        f"--conf", f"spark.hadoop.fs.s3a.access.key={MINIO_ACCESS_KEY}",
        f"--conf", f"spark.hadoop.fs.s3a.secret.key={MINIO_SECRET_KEY}",
        MINIO_SCRIPT
    ]
    subprocess.run(cmd)
    logger.info("[MinIO] Đã gửi lệnh restart.")

def run_pipeline_cycle():
    """Quy trình bảo trì định kỳ"""
    global IS_TRAINING
    logger.info("\n[Orchestrator] Bắt đầu quy trình Training định kỳ...")
    
    # 1. BẬT CỜ TRAINING (Khóa Health Check)
    IS_TRAINING = True
    logger.info("[Lock] Đã bật chế độ bảo trì. Health Check MinIO sẽ tạm dừng.")
    try:
        # 2. Stop MinIO
        logger.info("[Stop] Dừng MinIO Stream...")
        subprocess.run(f"docker exec {SPARK_MASTER_CONTAINER} pkill -f stream_to_minio.py", shell=True)
        time.sleep(10) # Đợi tắt hẳn

        # 3. Train ALS
        logger.info("[Train] Bắt đầu huấn luyện...")
        train_cmd = [
            "docker", "exec", SPARK_MASTER_CONTAINER,
            "spark-submit",
            "--master", MASTER,
            "--driver-memory", "2g", 
            "--executor-memory", "2g",
            "/opt/src/modeling/train_als_model.py"
        ]
        subprocess.run(train_cmd, check=True)
        logger.info("[Train] Huấn luyện thành công.")

    except subprocess.CalledProcessError:
        logger.error("[Train] Huấn luyện thất bại! Kiểm tra log.")
    
    except Exception as e:
        logger.error(f"[Train] Lỗi không mong muốn: {e}")

    finally:
        # 4. START LẠI MINIO & TẮT CỜ (QUAN TRỌNG NHẤT)
        # Dù train thành công hay thất bại, cũng phải bật lại stream và mở khóa
        logger.info("[Restart] Đang khôi phục lại Stream to MinIO...")
        
        # Chúng ta tắt cờ trước để hàm start_minio_stream_check bên dưới hoạt động
        IS_TRAINING = False 
        
        # Gọi hàm check để nó tự thấy job tắt và bật lại (Code tái sử dụng)
        start_minio_stream_check()
        
        logger.info("[Unlock] Đã tắt chế độ bảo trì. Hệ thống hoạt động bình thường.\n")

# --- MAIN FLOW ---

logger.info("Orchestrator khởi động v2.0 (With Smart Locking)...")
# 1. Kiểm tra ngay lập tức khi bật container
logger.info("--- Startup Check ---")
start_mongo_stream()
start_minio_stream_check()

# 2. Lên lịch Training (2h sáng)
schedule.every().day.at("02:00").do(run_pipeline_cycle)

# 3. Lên lịch Health Check
time_checking_interval = 5 # phút
schedule.every(time_checking_interval).minutes.do(start_mongo_stream)       # Check Mongo
schedule.every(time_checking_interval).minutes.do(start_minio_stream_check) # Check MinIO (có điều kiện)

logger.info(f"Đã lên lịch giám sát ({time_checking_interval} phút/lần) và training (02:00 hàng ngày).")

while True:
    schedule.run_pending()
    time.sleep(10)