import os
import sys
from pymongo import MongoClient
from pymilvus import connections, Collection, utility

import argparse

# Add parent directory to path to import config if needed
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# Configuration
MONGO_URI = os.getenv("MONGO_URI", "mongodb://localhost:27017")
MONGO_DB = "music_recsys"
COLLECTION_SONGS = "songs"

MILVUS_HOST = "localhost" # Assuming running from local machine against docker mapped port
MILVUS_PORT = "19530"
MILVUS_ALS_COLLECTION = "music_collection"
MILVUS_LYRICS_COLLECTION = "lyrics_embeddings"

def clean_and_sync_data():
    print("=" * 60)
    print(" CLEAN & SYNC: Removing songs without lyrics")
    print("=" * 60)

    # 1. Connect to MongoDB
    print("[1/4] Connecting to MongoDB...")
    mongo_client = MongoClient(MONGO_URI)
    db = mongo_client[MONGO_DB]
    songs_col = db[COLLECTION_SONGS]

    # 2. Find IDs to delete
    # Criteria: Song has been fetched (lrclib_fetched=True) BUT no lyrics found/empty
    query = {
        "lrclib_fetched": True,
        "$or": [
            {"lrclib_not_found": True},
            {"lrclib_plain_lyrics": None},
            {"lrclib_plain_lyrics": ""}
        ]
    }
    
    # Get all IDs
    cursor = songs_col.find(query, {"_id": 1})
    ids_to_delete = [doc["_id"] for doc in cursor]
    count = len(ids_to_delete)
    
    print(f"   found {count} songs with missing lyrics.")

    if count == 0:
        print(" No data to clean. All fetched songs have lyrics.")
        return

    # 3. Confirm Action
    print(f" WARNING: This will permanently DELETE {count} songs from:")
    print("    - MongoDB (Metadata)")
    print(f"    - Milvus '{MILVUS_ALS_COLLECTION}' (ALS Vectors)")
    print(f"    - Milvus '{MILVUS_LYRICS_COLLECTION}' (Content Vectors)")

    # Check for --yes flag
    parser = argparse.ArgumentParser()
    parser.add_argument("--yes", "-y", action="store_true", help="Skip confirmation")
    args, _ = parser.parse_known_args()

    if args.yes:
        print("Creating Auto-confirmation (--yes passed).")
    else:
        # Use simple ASCII for input to avoid Unicode errors in some terminals
        confirm = input("Are you sure? (type 'yes' to proceed): ").strip().lower()
        if confirm != "yes":
            print("Operation cancelled.")
            return

    # 4. Execute Deletion
    print("\n[2/4] Deleting from MongoDB...")
    res = songs_col.delete_many({"_id": {"$in": ids_to_delete}})
    print(f"   Deleted {res.deleted_count} documents from MongoDB.")

    # 5. Delete from Milvus
    print("\n[3/4] Deleting from Milvus...")
    try:
        connections.connect(host=MILVUS_HOST, port=MILVUS_PORT)
        
        # Helper to delete from a collection
        def delete_from_milvus_col(col_name, ids):
            if utility.has_collection(col_name):
                col = Collection(col_name)
                # Milvus delete expression: "id in ['id1', 'id2']"
                # Need to chunk because expression length limit
                batch_size = 1000
                total_deleted = 0
                
                for i in range(0, len(ids), batch_size):
                    batch_ids = ids[i:i+batch_size]
                    # Format list explicitly like ['a','b']
                    id_list_str = str(batch_ids).replace("[", "").replace("]", "")
                    if not id_list_str: continue
                    
                    expr = f"id in [{id_list_str}]"
                    col.delete(expr)
                    total_deleted += len(batch_ids)
                
                print(f"   Deleted {total_deleted} entities from '{col_name}'")
            else:
                print(f"   Collection '{col_name}' not found, skipping.")

        # Execute for both collections
        delete_from_milvus_col(MILVUS_ALS_COLLECTION, ids_to_delete)
        delete_from_milvus_col(MILVUS_LYRICS_COLLECTION, ids_to_delete)

    except Exception as e:
        print(f"Error talking to Milvus: {e}")
    finally:
        connections.disconnect("default")

    print("\n" + "="*60)
    print(" STARTUP COMPLETE. System is now CONSISTENT.")

if __name__ == "__main__":
    clean_and_sync_data()
