import logging
import sys
from pyspark.sql import SparkSession
from pymongo import MongoClient #type: ignore
import src.config as cfg #type: ignore
from minio import Minio  #type: ignore

def get_logger(name):
    """
    Tạo logger chuẩn cho ứng dụng.
    Output: Log ra màn hình console (stdout) với định dạng dễ đọc.
    """
    logger = logging.getLogger(name)
    logger.setLevel(logging.INFO)
    
    # Kiểm tra xem logger đã có handler chưa để tránh duplicate log
    if not logger.handlers:
        handler = logging.StreamHandler(sys.stdout)
        # Định dạng: [Thời gian] - [Tên Module] - [Level] - [Nội dung]
        formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        handler.setFormatter(formatter)
        logger.addHandler(handler)
        
    return logger

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

def ensure_minio_bucket(bucket_name):
    """
    Kiểm tra bucket trên MinIO, nếu chưa có thì tạo mới.
    """
    logger = get_logger("MinIO_Check")
    
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
            logger.warning(f" Bucket '{bucket_name}' chưa tồn tại. Đang tạo mới...")
            client.make_bucket(bucket_name)
            logger.info(f" Đã tạo bucket '{bucket_name}' thành công.")
        else:
            logger.info(f" Bucket '{bucket_name}' đã tồn tại.")
    except Exception as e:
        logger.error(f" Lỗi khi kiểm tra MinIO Bucket: {e}")