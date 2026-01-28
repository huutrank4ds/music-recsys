# common/spark_schemas.py
from pyspark.sql.types import StructType, StructField, StringType, LongType, IntegerType, DoubleType, ArrayType, DateType

class SparkSchemas:
    """
    Class quản lý tập trung toàn bộ Spark Schema.
    Dùng @staticmethod để không cần khởi tạo instance của class.
    """
    
    @staticmethod
    def music_log() -> StructType:
        return StructType([
            StructField("user_id", StringType(), True),
            StructField("track_id", StringType(), True),
            StructField("timestamp", LongType(), True),
            StructField("action", StringType(), True),
            StructField("source", StringType(), True),
            StructField("duration", DoubleType(), True),
            StructField("total_duration", DoubleType(), True) 
        ])

    @staticmethod
    def song_master() -> StructType:
        return StructType([
            StructField("_id", StringType(), True),
            StructField("track_name", StringType(), True),
            StructField("artist_id", StringType(), True),
            StructField("artist_name", StringType(), True),
            StructField("image_url", StringType(), True),
            StructField("url", StringType(), True),
            StructField("duration", DoubleType(), True),
            StructField("plays_7d", IntegerType(), True),
            StructField("plays_cumulative", LongType(), True),
            StructField("release_date", DateType(), True),
            StructField("lrclib_plain_lyrics", StringType(), True),
            StructField("lrclib_synced_lyrics", StringType(), True)
        ])

    @staticmethod
    def user_profile() -> StructType:
        return StructType([
            StructField("_id", StringType(), True),
            StructField("username", StringType(), True),
            StructField("latent_vector", ArrayType(DoubleType()), True),
            StructField("signup_date", DateType(), True),
            StructField("image_url", StringType(), True)
        ])