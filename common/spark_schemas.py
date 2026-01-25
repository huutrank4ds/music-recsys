from pyspark.sql.types import StructType, StructField, StringType, LongType, IntegerType, DoubleType, ArrayType


def get_music_log_schema():
    return StructType([
        StructField("user_id", StringType(), True),
        StructField("track_id", StringType(), True),
        StructField("timestamp", LongType(), True),
        StructField("action", StringType(), True),
        StructField("source", StringType(), True),
        StructField("duration", IntegerType(), True),
        StructField("total_duration", IntegerType(), True) 
    ])

def get_song_master_schema():
    """
    Schema cho danh sách bài hát (Master Data).
    Dùng cho Job Import Master Songs để đảm bảo đúng kiểu dữ liệu.
    """
    return StructType([
        StructField("_id", StringType(), True),
        StructField("track_name", StringType(), True),
        StructField("artist_id", StringType(), True),
        StructField("artist_name", StringType(), True),
        StructField("image_url", StringType(), True),
        StructField("url", StringType(), True),
        StructField("duration_ms", IntegerType(), True),
        StructField("plays_7d", IntegerType(), True),
        StructField("plays_cumulative", LongType(), True)
    ])

def get_user_schema():
    """
    Schema cho danh sách user.
    Dùng cho Job Import Users để đảm bảo đúng kiểu dữ liệu.
    """
    return StructType([
        StructField("_id", StringType(), True),
        StructField("user_name", StringType(), True),
        StructField("latent_vector", ArrayType(DoubleType()), True),
        StructField("last_updated", LongType(), True)
    ])