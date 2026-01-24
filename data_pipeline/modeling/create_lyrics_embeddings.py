"""
Create Lyrics Embeddings - Content-Based Filtering
===================================================
Sử dụng NLP (Sentence Transformers) để tạo embeddings từ lyrics.
Embeddings này dùng cho Content-Based Filtering.

Workflow:
1. Load songs với lyrics từ MongoDB
2. Tạo embeddings bằng Sentence Transformers
3. Lưu vào Milvus collection riêng (hoặc cùng collection với ALS)
"""

from pymongo import MongoClient
from pymilvus import connections, Collection, FieldSchema, CollectionSchema, DataType, utility
from sentence_transformers import SentenceTransformer
import numpy as np
from tqdm import tqdm

# ============================================================
# CONFIGURATION
# ============================================================
MONGODB_URI = "mongodb://mongodb:27017"
MONGO_DB = "music_recsys"
MONGO_COLLECTION = "songs"

MILVUS_HOST = "milvus"
MILVUS_PORT = 19530
MILVUS_COLLECTION = "lyrics_embeddings"  # Collection riêng cho lyrics

# Sentence Transformer Model
# Options: 'all-MiniLM-L6-v2' (384-dim, fast), 'all-mpnet-base-v2' (768-dim, better)
EMBEDDING_MODEL = "all-MiniLM-L6-v2"
EMBEDDING_DIM = 384  # Phải khớp với model

BATCH_SIZE = 100
MAX_SONGS = None  # None = xử lý tất cả

# ============================================================
# MAIN FUNCTIONS
# ============================================================

def setup_milvus_collection(dimension):
    """
    Tạo Milvus Collection cho Lyrics Embeddings.
    """
    connections.connect(host=MILVUS_HOST, port=MILVUS_PORT)
    
    if utility.has_collection(MILVUS_COLLECTION):
        print(f"⚠️ Collection '{MILVUS_COLLECTION}' đã tồn tại. Xóa và tạo lại...")
        utility.drop_collection(MILVUS_COLLECTION)
    
    # Schema
    fields = [
        FieldSchema(name="id", dtype=DataType.VARCHAR, is_primary=True, max_length=100),
        FieldSchema(name="embedding", dtype=DataType.FLOAT_VECTOR, dim=dimension)
    ]
    schema = CollectionSchema(fields, description="Lyrics embeddings for content-based filtering")
    
    # Create collection
    collection = Collection(MILVUS_COLLECTION, schema)
    
    # Create index
    index_params = {
        "metric_type": "IP",  # Inner Product (cosine similarity khi normalized)
        "index_type": "IVF_FLAT",
        "params": {"nlist": 128}
    }
    collection.create_index("embedding", index_params)
    
    print(f"✅ Created Milvus collection: {MILVUS_COLLECTION} (dim={dimension})")
    return collection


def create_lyrics_embeddings():
    """
    Main function: Tạo embeddings từ lyrics và lưu vào Milvus.
    """
    print("=" * 60)
    print("🎵 CREATE LYRICS EMBEDDINGS (Content-Based Filtering)")
    print("=" * 60)
    
    # 1. Load NLP Model
    print(f"\n📚 Loading model: {EMBEDDING_MODEL}...")
    model = SentenceTransformer(EMBEDDING_MODEL)
    print(f"   Embedding dimension: {EMBEDDING_DIM}")
    
    # 2. Connect to MongoDB
    print("\n🔗 Connecting to MongoDB...")
    client = MongoClient(MONGODB_URI)
    db = client[MONGO_DB]
    collection = db[MONGO_COLLECTION]
    
    # 3. Query songs with lyrics
    query = {
        "lrclib_plain_lyrics": {"$exists": True, "$ne": None, "$ne": ""}
    }
    projection = {"_id": 1, "title": 1, "artist": 1, "lrclib_plain_lyrics": 1}
    
    cursor = collection.find(query, projection)
    if MAX_SONGS:
        cursor = cursor.limit(MAX_SONGS)
    
    songs = list(cursor)
    total = len(songs)
    print(f"📊 Found {total} songs with lyrics")
    
    if total == 0:
        print("❌ No songs with lyrics found! Run lyrics enrichment first.")
        return
    
    # 4. Setup Milvus
    print("\n🔗 Setting up Milvus...")
    milvus_collection = setup_milvus_collection(EMBEDDING_DIM)
    
    # 5. Create embeddings in batches
    print(f"\n🧠 Creating embeddings (batch size={BATCH_SIZE})...")
    
    ids_batch = []
    embeddings_batch = []
    processed = 0
    
    for song in tqdm(songs, desc="Processing"):
        track_id = str(song["_id"])
        lyrics = song.get("lrclib_plain_lyrics", "")
        
        # Skip empty lyrics
        if not lyrics or len(lyrics.strip()) < 50:
            continue
        
        # Truncate very long lyrics (model có limit)
        if len(lyrics) > 5000:
            lyrics = lyrics[:5000]
        
        # Create embedding
        embedding = model.encode(lyrics, normalize_embeddings=True)
        
        ids_batch.append(track_id)
        embeddings_batch.append(embedding.tolist())
        
        # Insert batch
        if len(ids_batch) >= BATCH_SIZE:
            milvus_collection.insert([ids_batch, embeddings_batch])
            processed += len(ids_batch)
            ids_batch = []
            embeddings_batch = []
    
    # Insert remaining
    if ids_batch:
        milvus_collection.insert([ids_batch, embeddings_batch])
        processed += len(ids_batch)
    
    # 6. Load collection for searching
    milvus_collection.load()
    
    print("\n" + "=" * 60)
    print(f"✅ COMPLETED!")
    print(f"   Total songs with lyrics: {total}")
    print(f"   Embeddings created: {processed}")
    print(f"   Milvus collection: {MILVUS_COLLECTION}")
    print("=" * 60)
    
    client.close()
    connections.disconnect("default")


def search_similar_by_lyrics(track_id: str, top_k: int = 10):
    """
    Tìm bài hát tương tự dựa trên lyrics embedding.
    Dùng cho Next Song recommendation.
    """
    connections.connect(host=MILVUS_HOST, port=MILVUS_PORT)
    collection = Collection(MILVUS_COLLECTION)
    collection.load()
    
    # Get embedding của bài hiện tại
    result = collection.query(
        expr=f"id == '{track_id}'",
        output_fields=["embedding"]
    )
    
    if not result:
        print(f"❌ Track {track_id} not found in lyrics embeddings")
        return []
    
    current_embedding = result[0]["embedding"]
    
    # Search similar
    search_params = {"metric_type": "IP", "params": {"nprobe": 10}}
    results = collection.search(
        data=[current_embedding],
        anns_field="embedding",
        param=search_params,
        limit=top_k + 1,  # +1 vì sẽ loại bỏ bài hiện tại
        output_fields=["id"]
    )
    
    similar_ids = [hit.id for hit in results[0] if hit.id != track_id][:top_k]
    
    connections.disconnect("default")
    return similar_ids


if __name__ == "__main__":
    create_lyrics_embeddings()
