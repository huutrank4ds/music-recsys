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

MILVUS_HOST = "milvus-standalone"
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
    Tạo hoặc load Milvus Collection cho Lyrics Embeddings.
    Hỗ trợ RESUME: Nếu collection đã có, load nó lên thay vì xóa.
    """
    connections.connect(host=MILVUS_HOST, port=MILVUS_PORT)
    
    if utility.has_collection(MILVUS_COLLECTION):
        print(f"ℹ️ Collection '{MILVUS_COLLECTION}' đã tồn tại. Sẽ tiếp tục insert (RESUME mode)...")
        collection = Collection(MILVUS_COLLECTION)
        collection.load()
        print(f"✅ Loaded existing collection. Current entities: {collection.num_entities}")
        return collection
    
    print(f"✨ Creating NEW collection '{MILVUS_COLLECTION}'...")
    
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


def get_existing_ids(collection):
    """
    Lấy danh sách ID đã tồn tại trong Milvus để skip.
    Lưu ý: Với dữ liệu lớn, query tất cả ID có thể chậm.
    """
    print("🔍 Checking existing embeddings to resume...")
    try:
        # Nếu collection rỗng
        if collection.num_entities == 0:
            return set()
            
        # Query ID only (limit max possible needed or iterate)
        # Ở đây lấy tất cả ID (nếu < 1M thì ổn)
        res = collection.query(expr="id != ''", output_fields=["id"])
        existing_ids = set([item['id'] for item in res])
        print(f"✅ Found {len(existing_ids)} existing embeddings. Will skip these.")
        return existing_ids
    except Exception as e:
        print(f"⚠️ Warning: Could not fetch existing IDs ({e}). Will try to insert all.")
        return set()


def create_lyrics_embeddings():
    """
    Main function: Tạo embeddings từ lyrics và lưu vào Milvus.
    """
    print("=" * 60)
    print("🎵 CREATE LYRICS EMBEDDINGS (Content-Based Filtering)")
    print("=" * 60)
    
    # 1. Setup Milvus first
    print("\n🔗 Connecting to Milvus...")
    try:
        milvus_collection = setup_milvus_collection(EMBEDDING_DIM)
        existing_ids = get_existing_ids(milvus_collection)
    except Exception as e:
        print(f"❌ Error connecting to Milvus: {e}")
        return

    # 2. Load NLP Model
    print(f"\n📚 Loading model: {EMBEDDING_MODEL}...")
    model = SentenceTransformer(EMBEDDING_MODEL)
    print(f"   Embedding dimension: {EMBEDDING_DIM}")
    
    # 3. Connect to MongoDB
    print("\n🔗 Connecting to MongoDB...")
    client = MongoClient(MONGODB_URI)
    db = client[MONGO_DB]
    collection = db[MONGO_COLLECTION]
    
    # 4. Query songs with lyrics
    query = {
        "lrclib_plain_lyrics": {"$exists": True, "$ne": None, "$ne": ""}
    }
    projection = {"_id": 1, "title": 1, "artist": 1, "lrclib_plain_lyrics": 1}
    
    print("🔍 Fetching songs from MongoDB...")
    cursor = collection.find(query, projection)
    if MAX_SONGS:
        cursor = cursor.limit(MAX_SONGS)
    
    songs = list(cursor)
    total = len(songs)
    print(f"📊 Found {total} songs with lyrics in MongoDB")
    
    if total == 0:
        print("❌ No songs with lyrics found! Run lyrics enrichment first.")
        return
    
    # 5. Create embeddings in batches
    print(f"\n🧠 Creating embeddings (batch size={BATCH_SIZE})...")
    
    ids_batch = []
    embeddings_batch = []
    processed = 0
    skipped = 0
    
    for song in tqdm(songs, desc="Processing"):
        track_id = str(song["_id"])
        
        # Check if already exists
        if track_id in existing_ids:
            skipped += 1
            continue
            
        lyrics = song.get("lrclib_plain_lyrics", "")
        
        # Skip empty lyrics
        if not lyrics or len(lyrics.strip()) < 50:
            continue
        
        # Truncate very long lyrics (model có limit)
        if len(lyrics) > 5000:
            lyrics = lyrics[:5000]
        
        # Create embedding
        try:
            embedding = model.encode(lyrics, normalize_embeddings=True)
            
            ids_batch.append(track_id)
            embeddings_batch.append(embedding.tolist())
            
            # Insert batch
            if len(ids_batch) >= BATCH_SIZE:
                milvus_collection.insert([ids_batch, embeddings_batch])
                processed += len(ids_batch)
                ids_batch = []
                embeddings_batch = []
        except Exception as e:
            print(f"⚠️ Error encoding song {track_id}: {e}")
            continue
    
    # Insert remaining
    if ids_batch:
        milvus_collection.insert([ids_batch, embeddings_batch])
        processed += len(ids_batch)
    
    # 6. Flush and load
    milvus_collection.flush()
    milvus_collection.load()
    
    print("\n" + "=" * 60)
    print(f"✅ COMPLETED!")
    print(f"   Total songs in DB: {total}")
    print(f"   Already existed (skipped): {skipped}")
    print(f"   Newly created & inserted: {processed}")
    print(f"   Total in Milvus: {milvus_collection.num_entities}")
    print("=" * 60)
    
    client.close()
    connections.disconnect("default")


if __name__ == "__main__":
    create_lyrics_embeddings()
