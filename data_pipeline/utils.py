import signal
import sys
from pyspark.sql import SparkSession
from pymongo import MongoClient #type: ignore
import config as cfg #type: ignore
from minio import Minio  #type: ignore
from common.logger import get_logger
from confluent_kafka.admin import AdminClient #type: ignore
from confluent_kafka import KafkaException #type: ignore
import time


def get_spark_session(app_name):
    """
    Tạo và trả về một SparkSession với cấu hình kết nối MinIO & MongoDB.
    """
    builder = SparkSession.builder \
        .appName(app_name) \
        .config("spark.kafka.consumer.cache.enabled", "false") \
        .config("spark.hadoop.fs.s3a.endpoint", cfg.MINIO_ENDPOINT) \
        .config("spark.hadoop.fs.s3a.access.key", cfg.MINIO_ACCESS_KEY) \
        .config("spark.hadoop.fs.s3a.secret.key", cfg.MINIO_SECRET_KEY) \
        .config("spark.hadoop.fs.s3a.path.style.access", "true") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false") \
        .config("spark.mongodb.read.connection.uri", cfg.MONGO_URI) \
        .config("spark.mongodb.write.connection.uri", cfg.MONGO_URI) \
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
        .config("spark.kryoserializer.buffer.max", "128m")
        
    return builder.getOrCreate()

def get_mongo_db_client():
    """
    Lấy object collection của MongoDB để thao tác (find, insert...).
    """
    client = MongoClient(cfg.MONGO_URI)
    db = client[cfg.MONGO_DB]
    return client, db

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

def wait_for_topic(topic_name, bootstrap_servers, logger, timeout=300):
    """
    Sử dụng Confluent Kafka AdminClient để kiểm tra topic.
    """
    logger.info(f"Đang kiểm tra topic '{topic_name}' trên {bootstrap_servers} (dùng confluent_kafka)...")
    
    # Cấu hình AdminClient
    conf = {'bootstrap.servers': bootstrap_servers}
    admin_client = AdminClient(conf)
    
    start_time = time.time()

    while True:
        try:
            cluster_metadata = admin_client.list_topics(timeout=10)
            if topic_name in cluster_metadata.topics:
                logger.info(f"Đã tìm thấy topic '{topic_name}'. Bắt đầu Spark Job.")
                return True
            
        except KafkaException as e:
            logger.warning(f"Lỗi kết nối Kafka: {e}")
        except Exception as e:
            logger.warning(f"Lỗi không xác định khi check topic: {e}")

        # Kiểm tra timeout tổng
        if time.time() - start_time > timeout:
            logger.error(f"Timeout! Topic '{topic_name}' không xuất hiện sau {timeout}s.")
            raise TimeoutError(f"Topic {topic_name} not found")

        logger.info(f"Topic '{topic_name}' chưa có. Đợi 10s...")
        time.sleep(10)