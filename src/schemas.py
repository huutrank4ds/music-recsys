from pyspark.sql.types import StructType, StructField, StringType, LongType, TimestampType

def get_music_log_schema():
    """
    Schema cho dữ liệu Log hành vi người dùng (Kafka & MinIO).
    Đảm bảo tính nhất quán giữa Producer và Spark Streaming.
    """
    return StructType([
        StructField("user_id", StringType(), True),
        StructField("timestamp", StringType(), True),
        StructField("musicbrainz_artist_id", StringType(), True),
        StructField("artist_name", StringType(), True),
        StructField("artist_index", LongType(), True), 
        StructField("musicbrainz_track_id", StringType(), True),
        StructField("track_name", StringType(), True),
        StructField("track_index", LongType(), True)   
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