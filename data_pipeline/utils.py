import signal
import sys
from pyspark.sql import SparkSession
from pymongo import MongoClient #type: ignore
import config as cfg #type: ignore
from minio import Minio  #type: ignore
from common.logger import get_logger

def get_spark_session(app_name):
    """
    Tạo và trả về một SparkSession với cấu hình kết nối MinIO & MongoDB.
    """
    builder = SparkSession.builder \
        .appName(app_name) \
        .config("spark.hadoop.fs.s3a.endpoint", cfg.MINIO_ENDPOINT) \
        .config("spark.hadoop.fs.s3a.access.key", cfg.MINIO_ACCESS_KEY) \
        .config("spark.hadoop.fs.s3a.secret.key", cfg.MINIO_SECRET_KEY) \
        .config("spark.hadoop.fs.s3a.path.style.access", "true") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false") \
        .config("spark.mongodb.read.connection.uri", cfg.MONGO_URI) \
        .config("spark.mongodb.write.connection.uri", cfg.MONGO_URI)
        
    return builder.getOrCreate()

def get_mongo_collection(collection_name):
    """
    Lấy object collection của MongoDB để thao tác (find, insert...).
    """
    client = MongoClient(cfg.MONGO_URI)
    db = client[cfg.MONGO_DB]
    return db[collection_name]

def ensure_minio_bucket(bucket_name, logger=None):
    """
    Kiểm tra bucket trên MinIO, nếu chưa có thì tạo mới.
    """
    logger = logger or get_logger("MinIO_Check")
    
    # Lưu ý: Chạy trong Docker nên endpoint là minio:9000
    # Cần cắt bỏ 'http://' vì thư viện Minio không cần scheme
    endpoint = cfg.MINIO_ENDPOINT.replace("http://", "")
    
    client = Minio(
        endpoint,
        access_key=cfg.MINIO_ACCESS_KEY,
        secret_key=cfg.MINIO_SECRET_KEY,
        secure=False
    )
    
    try:
        if not client.bucket_exists(bucket_name):
            logger.warning(f" [{logger.name}] Bucket '{bucket_name}' chưa tồn tại. Đang tạo mới...")
            client.make_bucket(bucket_name)
            logger.info(f" [{logger.name}] Đã tạo bucket '{bucket_name}' thành công.")
        else:
            logger.info(f" [{logger.name}] Bucket '{bucket_name}' đã tồn tại.")
    except Exception as e:
        logger.error(f" Lỗi khi kiểm tra MinIO Bucket: {e}")

class GracefulStopper:
    def __init__(self, logger=None):
        self.query = None
        self.logger = logger or get_logger("GracefulStopper")
        
        signal.signal(signal.SIGINT, self._handler)
        signal.signal(signal.SIGTERM, self._handler)

    def attach(self, query):
        """Gắn query đang chạy vào bộ xử lý"""
        self.query = query

    def _handler(self, sig, frame):
        """Hàm xử lý nội bộ"""
        self.logger.warning(f"[{self.logger.name}] Nhận tín hiệu dừng (Signal: {sig})! Đang tắt Gracefully...")
        
        if self.query:
            # Nếu đã có query, ra lệnh dừng và đợi xử lý nốt batch
            self.logger.info(f"[{self.logger.name}] Đang đợi batch hiện tại hoàn thành...")
            self.query.stop()
        else:
            # Nếu chưa có query nào chạy (đang khởi động), thoát luôn
            sys.exit(0)
