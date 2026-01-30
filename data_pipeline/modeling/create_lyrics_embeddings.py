"""
Create Lyrics Embeddings - Content-Based Filtering
===================================================
Sử dụng NLP (Sentence Transformers) để tạo embeddings từ lyrics.
Embeddings này dùng cho Content-Based Filtering.

Workflow:
1. Load songs với lyrics từ MongoDB
2. Tạo embeddings bằng Sentence Transformers
3. Lưu vào Milvus collection riêng
"""

from pymongo import MongoClient
from pymilvus import connections, Collection, utility
from sentence_transformers import SentenceTransformer
from tqdm import tqdm
import config as cfg
from common.logger import get_logger

# Import Schema và Index Params chuẩn từ module chung
from common.schemas.milvus_schemas import (
    get_milvus_content_embedding_schema, 
    index_params_milvus
)

# ============================================================
# CONFIGURATION
# ============================================================
logger = get_logger("Create Lyrics Embeddings")

# MongoDB Config
MONGODB_URI = cfg.MONGO_URI
MONGO_DB = cfg.MONGO_DB
MONGO_COLLECTION = cfg.MONGO_SONGS_COLLECTION

# Milvus Config
MILVUS_URI = cfg.MILVUS_URI
MILVUS_COLLECTION = cfg.MILVUS_CONTENT_COLLECTION
EMBEDDING_DIM = cfg.EMBEDDING_DIM

# Model Config
EMBEDDING_MODEL_NAME = cfg.EMBEDDING_MODEL
BATCH_SIZE = 100
MAX_SONGS = None  # None = xử lý tất cả

# ============================================================
# MAIN FUNCTIONS
# ============================================================

def setup_milvus_collection():
    """
    Tạo hoặc load Milvus Collection sử dụng Schema chuẩn.
    """
    # 1. Kết nối Milvus
    connections.connect(alias="default", uri=MILVUS_URI)
    
    # 2. Kiểm tra collection tồn tại
    if utility.has_collection(MILVUS_COLLECTION):
        logger.info(f"Collection '{MILVUS_COLLECTION}' đã tồn tại. Sẽ tiếp tục chèn dữ liệu (Chế độ RESUME).")
        collection = Collection(MILVUS_COLLECTION)
        collection.load()
        logger.info(f"Đã tải collection hiện có. Số lượng bản ghi hiện tại: {collection.num_entities}")
        return collection
    
    logger.info(f"Đang tạo mới collection '{MILVUS_COLLECTION}'...")
    
    # 3. Lấy Schema từ module common
    # Lưu ý: Hàm schema cần nhận tham số dim để khớp với model
    schema = get_milvus_content_embedding_schema(dim=EMBEDDING_DIM)
    
    # 4. Tạo collection
    collection = Collection(MILVUS_COLLECTION, schema)
    
    # 5. Tạo index (Sử dụng index_params_milvus từ import)
    # index_params_milvus thường có dạng: {"metric_type": "IP", "index_type": "IVF_FLAT", "params": {"nlist": 128}}
    logger.info("Đang tạo chỉ mục (Index) cho vector...")
    collection.create_index("embedding", index_params_milvus)
    
    logger.info(f"Đã tạo thành công Milvus collection: {MILVUS_COLLECTION} (dim={EMBEDDING_DIM})")
    return collection


def get_existing_ids(collection):
    """
    Lấy danh sách ID đã tồn tại trong Milvus để bỏ qua, tránh xử lý trùng lặp.
    """
    logger.info("Đang kiểm tra các vector đã tồn tại để tiếp tục xử lý...")
    try:
        if collection.num_entities == 0:
            return set()
            
        # Chỉ query trường "id" để tối ưu hiệu suất
        # Lưu ý: Nếu dữ liệu quá lớn (>1 triệu), nên dùng iterator thay vì query tất cả một lần
        res = collection.query(expr="id != ''", output_fields=["id"])
        existing_ids = set([item['id'] for item in res])
        
        logger.info(f"Tìm thấy {len(existing_ids)} bản ghi đã tồn tại. Sẽ bỏ qua các bài hát này.")
        return existing_ids
    except Exception as e:
        logger.warning(f"Không thể lấy danh sách ID hiện có ({e}). Sẽ thử chèn toàn bộ dữ liệu.")
        return set()


def create_lyrics_embeddings():
    """
    Hàm chính: Tạo embeddings từ lyrics và lưu vào Milvus.
    """
    logger.info("=== BẮT ĐẦU QUÁ TRÌNH TẠO EMBEDDINGS TỪ LỜI BÀI HÁT ===")
    
    # 1. Kết nối Milvus và chuẩn bị Collection
    logger.info("Đang thiết lập kết nối Milvus...")
    try:
        milvus_collection = setup_milvus_collection()
        existing_ids = get_existing_ids(milvus_collection)
    except Exception as e:
        logger.error(f"Lỗi kết nối Milvus: {e}")
        return

    # 2. Tải Model NLP
    logger.info(f"Đang tải mô hình NLP: {EMBEDDING_MODEL_NAME}...")
    try:
        model = SentenceTransformer(EMBEDDING_MODEL_NAME)
        logger.info(f"Kích thước vector của mô hình: {EMBEDDING_DIM}")
    except Exception as e:
        logger.error(f"Không thể tải mô hình: {e}")
        return
    
    # 3. Kết nối MongoDB
    logger.info("Đang kết nối đến MongoDB...")
    client = MongoClient(MONGODB_URI)
    db = client[MONGO_DB]
    mongo_col = db[MONGO_COLLECTION]
    
    # 4. Lấy danh sách bài hát có lyrics
    # Điều kiện: Có trường lyrics, không null, không rỗng
    query = {
        "lrclib_plain_lyrics": {"$exists": True, "$ne": None, "$ne": ""}
    }
    projection = {"_id": 1, "title": 1, "artist": 1, "lrclib_plain_lyrics": 1}
    
    logger.info("Đang truy vấn bài hát từ MongoDB...")
    cursor = mongo_col.find(query, projection)
    
    if MAX_SONGS:
        cursor = cursor.limit(MAX_SONGS)
    
    # Chuyển sang list để biết tổng số lượng (lưu ý RAM nếu dữ liệu quá lớn)
    songs = list(cursor)
    total_songs = len(songs)
    logger.info(f"Tìm thấy {total_songs} bài hát có lời trong cơ sở dữ liệu.")
    
    if total_songs == 0:
        logger.warning("Không tìm thấy bài hát nào có dữ liệu lời bài hát. Vui lòng chạy bước làm giàu dữ liệu (Enrichment) trước.")
        return
    
    # 5. Xử lý tạo Embeddings theo Batch
    logger.info(f"Đang xử lý tạo vector (Kích thước lô: {BATCH_SIZE})...")
    
    ids_batch = []
    embeddings_batch = []
    processed_count = 0
    skipped_count = 0
    error_count = 0
    
    # Sử dụng tqdm để hiển thị thanh tiến trình
    for song in tqdm(songs, desc="Đang xử lý"):
        track_id = str(song["_id"])
        
        # Bỏ qua nếu đã tồn tại
        if track_id in existing_ids:
            skipped_count += 1
            continue
            
        lyrics = song.get("lrclib_plain_lyrics", "")
        
        # Bỏ qua nếu lyrics quá ngắn (dữ liệu rác)
        if not lyrics or len(lyrics.strip()) < 50:
            continue
        
        # Cắt ngắn nếu lyrics quá dài (giới hạn token của BERT models)
        if len(lyrics) > 5000:
            lyrics = lyrics[:5000]
        
        try:
            # Tạo embedding
            # normalize_embeddings=True giúp tính Cosine Similarity chính xác hơn bằng Dot Product
            embedding = model.encode(lyrics, normalize_embeddings=True)
            
            ids_batch.append(track_id)
            embeddings_batch.append(embedding.tolist())
            
            # Ghi vào Milvus khi đủ Batch
            if len(ids_batch) >= BATCH_SIZE:
                milvus_collection.insert([ids_batch, embeddings_batch])
                processed_count += len(ids_batch)
                
                # Reset batch
                ids_batch = []
                embeddings_batch = []
                
        except Exception as e:
            logger.error(f"Lỗi khi mã hóa bài hát {track_id}: {e}")
            error_count += 1
            continue
    
    # Xử lý các bản ghi còn lại trong batch cuối cùng
    if ids_batch:
        milvus_collection.insert([ids_batch, embeddings_batch])
        processed_count += len(ids_batch)
    
    # 6. Flush dữ liệu xuống đĩa và Load lại
    logger.info("Đang đồng bộ dữ liệu xuống đĩa (Flush)...")
    milvus_collection.flush()
    # milvus_collection.load() # Uncomment nếu cần search ngay lập tức
    
    # Tổng kết
    logger.info("=== HOÀN TẤT QUÁ TRÌNH ===")
    logger.info(f"Tổng số bài hát quét được: {total_songs}")
    logger.info(f"Đã bỏ qua (đã có sẵn): {skipped_count}")
    logger.info(f"Đã tạo mới và lưu trữ: {processed_count}")
    logger.info(f"Số lượng lỗi: {error_count}")
    logger.info(f"Tổng số vector hiện có trong Milvus: {milvus_collection.num_entities}")
    
    # Đóng kết nối
    client.close()
    connections.disconnect("default")

if __name__ == "__main__":
    create_lyrics_embeddings()