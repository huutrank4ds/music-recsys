from pyspark.sql.types import StructType, StructField, StringType, LongType, TimestampType

from pyspark.sql.types import StructType, StructField, StringType, LongType, IntegerType

def get_music_log_schema():
    """
    Schema khớp hoàn toàn với dữ liệu JSON từ Kafka Producer & API Backend.
    """
    return StructType([
        StructField("user_id", StringType(), True),
        StructField("track_id", StringType(), True), 
        StructField("timestamp", LongType(), True),
        StructField("action", StringType(), True),
        StructField("source", StringType(), True),
    ])

def get_song_master_schema():
    """
    Schema cho danh sách bài hát (Master Data).
    Dùng cho Job Import Master Songs để đảm bảo đúng kiểu dữ liệu.
    """
    return StructType([
        StructField("musicbrainz_track_id", StringType(), True),
        StructField("track_name", StringType(), True),
        StructField("musicbrainz_artist_id", StringType(), True),
        StructField("artist_name", StringType(), True),
        StructField("track_index", LongType(), True),
        StructField("artist_index", LongType(), True)
    ])