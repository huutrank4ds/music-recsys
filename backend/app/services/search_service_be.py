from app.core.database import DB
from typing import List, Optional, Any
from pymongo.errors import OperationFailure #type: ignore
from common.logger import get_logger
import os
import config as cfg
from common.constants import SONG_SUMMARY_PROJECTION

logger = get_logger("Search Service")

class SearchService:

    async def create_indexes(self):
        """
        Tạo Text Index cho collection songs 
        """
        try:
            await DB.db[cfg.COLLECTION_SONGS].create_index(
                [("track_name", "text"), ("artist_name", "text")],
                name="song_search_index_v2",
                weights={"track_name": 10, "artist_name": 5}
            )
            logger.info("Tạo Text Index cho collection songs thành công.")
            await DB.db[cfg.COLLECTION_SONGS].create_index(
                [("plays_7d", -1)],
                name="idx_plays_7d_desc"
            )
            logger.info("Tạo Index cho plays_7d giảm dần thành công.")
            await DB.db[cfg.COLLECTION_SONGS].create_index(
                [("plays_cumulative", -1)],
                name="idx_plays_cumulative_desc"
            )
            logger.info("Tạo Index cho plays_cumulative giảm dần thành công.")
        except Exception as e:
            logger.warning(f"Index creation warning: {e}")

    async def search_songs(self, query: str, limit: int, skip: int) -> Any:
        if not query:
            return {
                "data": [],
                "meta": {"has_more": False, "limit": limit, "skip": skip}
            }

        try:
            project_stage = SONG_SUMMARY_PROJECTION.copy()
            project_stage["score"] = {"$meta": "textScore"}

            pipeline = [
                {"$match": {"$text": {"$search": query}}}, # Sử dụng $text để tìm kiếm
                {"$addFields": {"score": {"$meta": "textScore"}}}, # Thêm trường score
                {"$sort": {"score": -1}}, # Sắp xếp theo score giảm dần
                {"$skip": skip}, # Bỏ qua số lượng document
                {"$limit": limit + 1} # Lấy limit + 1 để kiểm tra has_more
            ]

            cursor = DB.db[cfg.COLLECTION_SONGS].aggregate(pipeline)
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
            logger.error(f"Tìm kiếm bằng Text Index lỗi: {e}")
            return await self.search_songs_fallback(query, limit, skip)

    async def search_songs_fallback(self, query: str, limit: int, skip: int) -> Any:
        # Fallback dùng Regex
        filter_query = {
            "$or": [
                {"track_name": {"$regex": query, "$options": "i"}},
                {"artist_name": {"$regex": query, "$options": "i"}}
            ]
        }
        
        try:
            cursor = DB.db[cfg.COLLECTION_SONGS].find(filter_query, SONG_SUMMARY_PROJECTION) # Lấy projection tóm tắt
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
            logger.error(f"Tìm kiếm dự phòng lỗi: {e}")
            return {
                "data": [],
                "meta": {"has_more": False, "limit": limit, "skip": skip}
            }

search_service = SearchService()