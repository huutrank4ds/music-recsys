import os
from dotenv import load_dotenv

# Tự động tìm và load file .env (nếu chạy local outside Docker)
load_dotenv()

# KAFKA CONFIGURATIONS
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC")
KAFKA_NUM_PARTITIONS = int(os.getenv("KAFKA_NUM_PARTITIONS", "4"))
KAFKA_REPLICATION_FACTOR = int(os.getenv("KAFKA_REPLICATION_FACTOR", "1"))

# MINIO CONFIGURATIONS
MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY")
MINIO_DATALAKE_BUCKET = os.getenv("MINIO_DATALAKE_BUCKET")

# MONGODB CONFIGURATIONS
MONGO_URI = os.getenv("MONGO_URI")
MONGO_DB = os.getenv("MONGO_DB")
MONGO_SONGS_COLLECTION = os.getenv("MONGO_SONGS_COLLECTION")
MONGO_USERS_COLLECTION = os.getenv("MONGO_USERS_COLLECTION")
MONGO_METADATA_COLLECTION = os.getenv("MONGO_METADATA_COLLECTION")

# DATA PATHS
# Đuong dẫn dữ liệu master và logs
SONGS_MASTER_DATA_PATH = os.getenv("SONGS_MASTER_DATA_PATH")
USER_MASTER_DATA_PATH = os.getenv("USER_MASTER_DATA_PATH")
MUSIC_LOGS_DATA_PATH = os.getenv("MUSIC_LOGS_DATA_PATH")
RAW_DATA_DIR = os.getenv("RAW_DATA_DIR")


# Đường dẫn Checkpoint (Ổ cứng hoặc MinIO)
MINIO_CHECKPOINT_PATH = os.getenv("MINIO_CHECKPOINT_PATH", f"s3a://{MINIO_DATALAKE_BUCKET}/checkpoints")
# Đường dẫn dữ liệu thô trên MinIO (s3a://...)
MINIO_RAW_PATH = os.getenv("MINIO_RAW_PATH", f"s3a://{MINIO_DATALAKE_BUCKET}/raw/music_logs/")

# MILVUS CONFIGURATIONS
MILVUS_URI = os.getenv("MILVUS_URI")
MILVUS_CONTENT_COLLECTION = os.getenv("MILVUS_CONTENT_COLLECTION")
MILVUS_ALS_COLLECTION = os.getenv("MILVUS_ALS_COLLECTION")

# ALS MODEL HYPERPARAMETERS
ALS_RANK = int(os.getenv("ALS_RANK", "64"))           # Số chiều của latent vector
ALS_MAX_ITER = int(os.getenv("ALS_MAX_ITER", "15"))   # Số vòng lặp
ALS_REG_PARAM = float(os.getenv("ALS_REG_PARAM", "0.1"))  # Regularization
ALS_ALPHA = float(os.getenv("ALS_ALPHA", "40.0"))     # Confidence scaling 
ALS_SLIDING_WINDOW_DAYS = int(os.getenv("ALS_SLIDING_WINDOW_DAYS", "90"))  # Ngày dữ liệu

# CONTENT-BASED FILTERING HYPERPARAMETERS
EMBEDDING_MODEL = "all-MiniLM-L6-v2"
EMBEDDING_DIM = 384