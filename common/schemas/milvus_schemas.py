from pymilvus import FieldSchema, CollectionSchema, DataType #type: ignore
import os

# Constants
# ALS Rank (Collaborative Filtering)
ALS_RANK = int(os.getenv("ALS_RANK", 64))
# Content Dimension (Lyrics/Audio features)
CONTENT_RANK = int(os.getenv("CONTENT_RANK", 384)) 

class MilvusSchemas:
    """
    Class quản lý toàn bộ Schema và Params cho Milvus.
    """

    @staticmethod
    def song_embedding_schema(dim=ALS_RANK) -> CollectionSchema:
        """
        Schema lưu vector ALS (Collaborative Filtering).
        """
        fields = [
            FieldSchema(name="id", dtype=DataType.VARCHAR, max_length=64, is_primary=True, auto_id=False, description="ID bài hát"),
            FieldSchema(name="embedding", dtype=DataType.FLOAT_VECTOR, dim=dim, description="Vector ALS")
        ]
        return CollectionSchema(fields, description="ALS Song Embeddings")

    @staticmethod
    def content_embedding_schema(dim=CONTENT_RANK) -> CollectionSchema:
        """
        Schema lưu vector nội dung (Content-based: Lyrics, Audio...).
        """
        fields = [
            FieldSchema(name="id", dtype=DataType.VARCHAR, max_length=64, is_primary=True, auto_id=False, description="ID bài hát"),
            FieldSchema(name="embedding", dtype=DataType.FLOAT_VECTOR, dim=dim, description="Vector nội dung (Lyrics/Audio)")
        ]
        return CollectionSchema(fields, description="Content/Lyrics Embeddings")

    @staticmethod
    def index_params() -> dict:
        """
        Cấu hình Index (HNSW là tối ưu nhất cho search nhanh).
        """
        return {
            "metric_type": "IP", # Inner Product (Tốt cho Recommender System/Cosine Similarity)
            "index_type": "HNSW",
            "params": {"M": 16, "efConstruction": 200}
        }

    @staticmethod
    def search_params() -> dict:
        """
        Cấu hình Search (ef càng lớn tìm càng chính xác nhưng chậm hơn).
        """
        return {
            "metric_type": "IP",
            "params": {"ef": 256}
        }


get_milvus_song_embedding_schema = MilvusSchemas.song_embedding_schema
get_milvus_content_embedding_schema = MilvusSchemas.content_embedding_schema
index_params_milvus = MilvusSchemas.index_params
search_params_milvus = MilvusSchemas.search_params