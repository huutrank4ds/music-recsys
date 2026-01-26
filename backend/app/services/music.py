from app.core.database import DB
from typing import List, Optional, Any
from pymongo.errors import OperationFailure #type: ignore
from common.logger import get_logger
import os

logger = get_logger("MusicService")
COLLECTION_NAME = os.getenv("COLLECTION_SONGS", "songs")

class MusicService:
    _instance = None

    def __new__(cls):
        if not cls._instance:
            cls._instance = super(MusicService, cls).__new__(cls)
        return cls._instance

    async def create_indexes(self):
        """
        Tạo Text Index cho collection songs (Giữ nguyên)
        """
        try:
            await DB.db[COLLECTION_NAME].create_index(
                [("track_name", "text"), ("artist_name", "text")],
                name="song_search_index_v2",
                weights={"track_name": 10, "artist_name": 5}
            )
            logger.info("Created Text Index for Songs (track_name, artist_name)")
        except Exception as e:
            logger.warning(f"Index creation warning: {e}")

    async def search_songs(self, query: str, limit: int, skip: int) -> Any:
        if not query:
            return {
                "data": [],
                "meta": {"has_more": False, "limit": limit, "skip": skip}
            }

        try:
            pipeline = [
                {"$match": {"$text": {"$search": query}}}, # Sử dụng $text để tìm kiếm
                {"$addFields": {"score": {"$meta": "textScore"}}}, # Thêm trường score
                {"$sort": {"score": -1}}, # Sắp xếp theo score giảm dần
                {"$skip": skip}, # Bỏ qua số lượng document
                {"$limit": limit + 1} # Lấy limit + 1 để kiểm tra has_more
            ]

            cursor = DB.db[COLLECTION_NAME].aggregate(pipeline)
            songs = await cursor.to_list(length=limit + 1)
            
            if len(songs) > limit:
                has_more = True
                songs = songs[:limit]
            else:
                has_more = False
                
            return {
                "data": songs,
                "meta": {
                    "has_more": has_more,
                    "limit": limit,
                    "skip": skip,
                    "source": "text_index"
                }
            }

        except OperationFailure as e:
            logger.error(f"MongoDB Text Search Error: {e}")
            return await self.search_songs_fallback(query, limit, skip)

    async def search_songs_fallback(self, query: str, limit: int, skip: int) -> Any:
        # Fallback dùng Regex
        filter_query = {
            "$or": [
                {"track_name": {"$regex": query, "$options": "i"}},
                {"artist_name": {"$regex": query, "$options": "i"}}
            ]
        }
        
        # --- BỎ PROJECTION ---
        # Không truyền tham số projection vào nữa => MongoDB trả về FULL DATA
        
        try:
            cursor = DB.db[COLLECTION_NAME].find(filter_query) # Lấy tất cả trường (url, duration,...)
            songs = await cursor.skip(skip).limit(limit + 1).to_list(length=limit + 1)
            
            if len(songs) > limit:
                has_more = True
                songs = songs[:limit]
            else:
                has_more = False
                
            return {
                "data": songs,
                "meta": {
                    "has_more": has_more,
                    "limit": limit,
                    "skip": skip,
                    "source": "fallback"
                }
            }
            
        except Exception as e:
            logger.error(f"Fallback Search Error: {e}")
            return {
                "data": [],
                "meta": {"has_more": False, "limit": limit, "skip": skip}
            }

music_service = MusicService()