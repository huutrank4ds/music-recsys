from pymilvus import FieldSchema, CollectionSchema, DataType #type: ignore
import os

ALS_RANK = os.getenv("ALS_RANK", 64)


def get_milvus_song_embedding_schema():
    """
    Schema cho collection Milvus lưu trữ embedding của bài hát.
    Dùng để tạo collection nếu chưa tồn tại.
    """
    fields = [
        FieldSchema(name="id", dtype=DataType.VARCHAR, max_length=64, is_primary=True, auto_id=False, description="ID của bài hát"),
        FieldSchema(name="embedding", dtype=DataType.FLOAT_VECTOR, dim=ALS_RANK, description="Embedding của bài hát")
    ]
    schema = CollectionSchema(fields, description="Collection lưu trữ embedding của bài hát")
    return schema

def index_params_milvus():
    """
    Tham số tạo index cho Milvus.
    """
    return {
        "metric_type": "IP", # Inner Product (Cho dot product)
        "index_type": "HNSW",
        "params": {"M": 16, "efConstruction": 100}
    }

def search_params_milvus():
    """
    Tham số tìm kiếm cho Milvus.
    """
    return {
        "metric_type": "IP",
        "params": {"ef": 100}
    }