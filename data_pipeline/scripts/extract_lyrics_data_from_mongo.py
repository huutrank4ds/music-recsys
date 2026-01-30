# /opt/src/scripts/extract_lyrics_data_from_mongo.py
from utils import get_mongo_db_client
import config as cfg
from tqdm import tqdm
import json
from common.logger import get_logger
from pathlib import Path

OUTPUT_FILE_PATH = Path("/opt/data/lyrics_data/lyrics_data.jsonl")
logger = get_logger("ExtractLyricsData")

# Tạo thư mục nếu chưa tồn tại
OUTPUT_FILE_PATH.parent.mkdir(parents=True, exist_ok=True)


def get_lyrics_data():
    count_sample = 0
    client, db = get_mongo_db_client()
    collection = db[cfg.MONGO_SONGS_COLLECTION]
    query = {
        "lrclib_plain_lyrics": {"$exists": True, "$ne": None, "$ne": ""},
    }
    projection = {"_id": 1, "lrclib_plain_lyrics": 1}
    cursor = collection.find(query, projection)
    with open(OUTPUT_FILE_PATH, 'w', encoding='utf-8') as f:
        for doc in tqdm(cursor, desc="Extracting lyrics data"):
            doc["_id"] = str(doc["_id"])
            f.write(json.dumps(doc, ensure_ascii=False) + "\n")
            count_sample += 1
    logger.info(f"Total lyrics documents extracted: {count_sample}")
    logger.info(f"Lyrics data extracted to {OUTPUT_FILE_PATH}")

if __name__ == "__main__":
    get_lyrics_data()   