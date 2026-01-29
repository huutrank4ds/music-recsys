import json
import os
from pymongo import MongoClient

# Config
MONGO_URI = os.getenv("MONGO_URI", "mongodb://mongodb:27017")
DB_NAME = "music_recsys"
OUTPUT_FILE = "/opt/data/songs_clean.jsonl"  # Path inside container

def main():
    print(f"Connecting to MongoDB: {MONGO_URI}")
    client = MongoClient(MONGO_URI)
    db = client[DB_NAME]
    
    print("Starting export with Cleaning & Renaming...")
    
    # Lấy toàn bộ bài hát
    cursor = db.songs.find()
    total_docs = db.songs.count_documents({})
    
    count = 0
    with open(OUTPUT_FILE, "w", encoding="utf-8") as f:
        for doc in cursor:
            # 1. Xử lý Duration (Default 300s nếu thiếu)
            duration = doc.get("duration")
            if not duration: # None, 0, or Empty
                duration = 300
            
            # 2. Xử lý Release Date (Giữ nguyên hoặc None)
            release_date = doc.get("release_date")
            release_date_str = str(release_date) if release_date else "" # Convert to string or empty
            
            # 3. Build Record & Rename Fields
            clean_record = {
                "_id": str(doc.get("_id", "")),
                
                # RENAMED FIELDS
                "track_name": doc.get("track_name") or doc.get("title", ""),
                "artist_name": doc.get("artist_name") or doc.get("artist", ""),
                
                # EXISTING FIELDS
                "artist_id": doc.get("artist_id", ""),
                "image_url": doc.get("image_url", ""),
                "url": doc.get("url", ""),
                
                # Stats as String (theo yêu cầu)
                "plays_7d": str(doc.get("plays_7d", "0")),
                "plays_cumulative": str(doc.get("plays_cumulative", "0")),
                
                "duration": str(duration), # Convert to String
                
                "release_date": doc.get("release_date"),
                
                # Lyrics
                "lrclib_plain_lyrics": doc.get("lrclib_plain_lyrics", ""),
                "lrclib_synced_lyrics": doc.get("lrclib_synced_lyrics", "")
            }
            
            # Write JSON Line
            f.write(json.dumps(clean_record, ensure_ascii=False) + "\n")
            
            count += 1
            if count % 10000 == 0:
                print(f"Exported {count}/{total_docs} songs...")

    print("="*40)
    print(f"COMPLETE! Exported {count} songs.")
    print(f"File saved to: {OUTPUT_FILE}")
    print("="*40)

if __name__ == "__main__":
    main()
