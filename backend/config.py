import os

KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "music_log")
KAFKA_NUM_PARTITIONS = int(os.getenv("KAFKA_NUM_PARTITIONS", "3"))
KAFKA_REPLICATION_FACTOR = int(os.getenv("KAFKA_REPLICATION_FACTOR", "1"))

MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "localhost:9000")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "minioadmin")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "minioadmin")
DATALAKE_BUCKET = os.getenv("DATALAKE_BUCKET", "music-logs-bucket")

MONGO_URI = os.getenv("MONGO_URI", "mongodb://mongodb:27017")
MONGO_DB = os.getenv("MONGO_DB", "music_recsys")
COLLECTION_SONGS = os.getenv("COLLECTION_SONGS", "songs")
COLLECTION_USERS = os.getenv("COLLECTION_USERS", "users")

ALS_MILVUS_COLLECTION = os.getenv("ALS_MILVUS_COLLECTION", "music_collection")
CONTENT_MILVUS_COLLECTION = os.getenv("CONTENT_MILVUS_COLLECTION", "lyrics_embeddings")

ALS_RANK = int(os.getenv("ALS_RANK", 64))
ALS_MAX_ITER = int(os.getenv("ALS_MAX_ITER", 15))
ALS_REG_PARAM = float(os.getenv("ALS_REG_PARAM", 0.1))
ALS_ALPHA = float(os.getenv("ALS_ALPHA", 40.0))
SLIDING_WINDOW_DAYS = int(os.getenv("SLIDING_WINDOW_DAYS", 30))
MUSIC_LOGS_DATA_PATH = "/data/music-logs/"

FRONTEND_PORT = int(os.getenv("FRONTEND_PORT", "5173"))

LONG_KEY_PREFIX = "user:long:"
SHORT_KEY_PREFIX = "user:short:"
KEY_VECTOR_TTL = 3600 * 4  # 4 hours

FEED_KEY_PREFIX = "user:feed:"
SESSION_FEED_KEY_PREFIX = "user:session_feed:"
FEED_TTL = 3600  # 1 hour

PLAYLIST_KEY_PREFIX = "user:playlist:"
SESSION_PLAYLIST_KEY_PREFIX = "user:session_playlist:"
PLAYLIST_TTL = 3600  # 1 hour

# Content: Rất ít đổi -> Cache 7 ngày hoặc 30 ngày
CONTENT_VECTOR_TTL = 7 * 24 * 60 * 60  
CONTENT_VECTOR_KEY_PREFIX = "vec:content:"

# ALS: Đổi hàng ngày -> Cache 25 giờ (để tránh cache hết hạn đúng lúc tái huấn luyện)
ALS_VECTOR_TTL = 25 * 60 * 60 
ALS_VECTOR_KEY_PREFIX = "vec:als:"

FEED_BATCH_SIZE = 100       
FEED_MIN_THRESHOLD = 20
MAX_FEED_SESSION = 500
FEED_TTL = 86400

PLAYLIST_BATCH_SIZE = 50
PLAYLIST_MIN_THRESHOLD = 15
MAX_PLAYLIST_SESSION = 200