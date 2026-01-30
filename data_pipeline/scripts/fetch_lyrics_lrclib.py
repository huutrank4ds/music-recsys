# /opt/src/scripts/fetch_lyrics_lrclib.py
"""
Fetch lyrics từ LRCLIB API và lưu vào MongoDB.
Sử dụng asyncio và aiohttp để tăng tốc độ fetch nhiều requests song song.
"""
import asyncio
import aiohttp
import time
import os
from pymongo import MongoClient, UpdateOne #type: ignore
import config as cfg

# CONFIGURATION
LRCLIB_API = "https://lrclib.net/api"

MONGODB_URI = cfg.MONGO_URI
MONGO_DB = cfg.MONGO_DB
MONGO_COLLECTION = cfg.MONGO_SONGS_COLLECTION

# Parallel settings
MAX_CONCURRENT_REQUESTS = 20  
REQUEST_TIMEOUT = 15  
BATCH_SIZE = 500  

# Giới hạn số bài xử lý
MAX_SONGS = None  

# ASYNC LRCLIB API
async def search_lyrics_async(session: aiohttp.ClientSession, track_name: str, artist_name: str, semaphore: asyncio.Semaphore):
    """
    Async search lyrics từ LRCLIB.
    """
    if not track_name or not artist_name:
        return None
    
    async with semaphore:  
        try:
            params = {
                "track_name": track_name,
                "artist_name": artist_name
            }
            
            async with session.get(
                f"{LRCLIB_API}/search",
                params=params,
                timeout=aiohttp.ClientTimeout(total=REQUEST_TIMEOUT)
            ) as response:
                if response.status == 200:
                    results = await response.json()
                    
                    if results and len(results) > 0:
                        best_match = results[0]
                        return {
                            "lrclib_id": best_match.get("id"),
                            "lrclib_plain_lyrics": best_match.get("plainLyrics"),
                            "lrclib_synced_lyrics": best_match.get("syncedLyrics"),
                            "duration": str(best_match.get("duration") or 300),
                            "release_date": best_match.get("releaseDate") or None
                        }
                return None
                
        except asyncio.TimeoutError:
            return None
        except Exception as e:
            return None


async def process_song(session: aiohttp.ClientSession, song: dict, semaphore: asyncio.Semaphore):
    """
    Process một bài hát: fetch lyrics và trả về update operation.
    """
    track_id = song["_id"]
    track_name = song.get("track_name") or song.get("title", "")
    artist_name = song.get("artist_name") or song.get("artist", "")
    
    lyrics_data = await search_lyrics_async(session, track_name, artist_name, semaphore)
    
    if lyrics_data and lyrics_data.get("lrclib_plain_lyrics"):
        return UpdateOne(
            {"_id": track_id},
            {"$set": {**lyrics_data, "lrclib_fetched": True}}
        ), True
    else:
        return UpdateOne(
            {"_id": track_id},
            {"$set": {"lrclib_fetched": True, "lrclib_not_found": True}}
        ), False


async def fetch_lyrics_async():
    """
    Main async function: Fetch lyrics song song với nhiều requests.
    """
    print("=" * 60, flush=True)
    print("🚀 FETCH LYRICS FROM LRCLIB (ASYNC - FAST!)", flush=True)
    print(f"   Concurrent requests: {MAX_CONCURRENT_REQUESTS}", flush=True)
    print("=" * 60, flush=True)
    
    # Connect to MongoDB
    client = MongoClient(MONGODB_URI)
    db = client[MONGO_DB]
    collection = db[MONGO_COLLECTION]
    
    # Query songs without lyrics OR missing new fields (release_date)
    query = {
        "$or": [
            {"lrclib_fetched": {"$ne": True}},
            {"release_date": {"$exists": False}, "lrclib_fetched": True}
        ]
    }
    projection = {"_id": 1, "title": 1, "artist": 1, "track_name": 1, "artist_name": 1, "listen_count": 1}
    
    cursor = collection.find(query, projection).sort("listen_count", -1)
    
    if MAX_SONGS:
        cursor = cursor.limit(MAX_SONGS)
    
    songs = list(cursor)
    total = len(songs)
    print(f"Found {total} songs to fetch lyrics", flush=True)
    
    if total == 0:
        print(" All songs already processed!", flush=True)
        client.close()
        return
    
    # Setup async
    semaphore = asyncio.Semaphore(MAX_CONCURRENT_REQUESTS)
    connector = aiohttp.TCPConnector(limit=MAX_CONCURRENT_REQUESTS, limit_per_host=MAX_CONCURRENT_REQUESTS)
    
    found_count = 0
    not_found_count = 0
    processed = 0
    start_time = time.time()
    
    async with aiohttp.ClientSession(connector=connector) as session:
        # Process in batches
        for i in range(0, total, BATCH_SIZE):
            batch = songs[i:i + BATCH_SIZE]
            
            # Create tasks for this batch
            tasks = [process_song(session, song, semaphore) for song in batch]
            
            # Run all tasks concurrently
            results = await asyncio.gather(*tasks)
            
            # Collect bulk operations
            bulk_ops = []
            for update_op, has_lyrics in results:
                bulk_ops.append(update_op)
                if has_lyrics:
                    found_count += 1
                else:
                    not_found_count += 1
            
            # Bulk write to MongoDB
            if bulk_ops:
                collection.bulk_write(bulk_ops)
            
            processed += len(batch)
            elapsed = time.time() - start_time
            speed = processed / elapsed if elapsed > 0 else 0
            eta = (total - processed) / speed if speed > 0 else 0
            
            print(f"Progress: {processed}/{total} ({processed*100/total:.1f}%) | "
                  f"Found: {found_count} | Not found: {not_found_count} | "
                  f"Speed: {speed:.1f}/s | ETA: {eta/60:.1f}min", flush=True)
    
    elapsed = time.time() - start_time
    
    print("=" * 60, flush=True)
    print(f"   COMPLETED!", flush=True)
    print(f"   Total processed: {total}", flush=True)
    print(f"   Lyrics found: {found_count}", flush=True)
    print(f"   Not found: {not_found_count}", flush=True)
    print(f"   Success rate: {found_count/(found_count+not_found_count)*100:.1f}%", flush=True)
    print(f"   Time elapsed: {elapsed/60:.1f} minutes", flush=True)
    print(f"   Average speed: {total/elapsed:.1f} songs/second", flush=True)
    print("=" * 60, flush=True)
    
    client.close()


def main():
    """Entry point"""
    asyncio.run(fetch_lyrics_async())


if __name__ == "__main__":
    main()
