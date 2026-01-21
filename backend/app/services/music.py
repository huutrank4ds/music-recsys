from app.core.database import DB
from typing import List, Optional, Any
from pymongo.errors import OperationFailure #type: ignore
from common.logger import get_logger

logger = get_logger("MusicService")

class MusicService:
    _instance = None

    def __new__(cls):
        if not cls._instance:
            cls._instance = super(MusicService, cls).__new__(cls)
        return cls._instance

    async def create_indexes(self):
        """
        Hàm tiện ích để tạo Index nếu chưa có.
        Nên gọi hàm này 1 lần lúc start server (trong file main.py).
        """
        try:
            # Tạo Text Index cho trường 'title' và 'artist' để search
            await DB.db["songs"].create_index(
                [("title", "text"), ("artist", "text")],
                name="song_search_index",
                weights={"title": 10, "artist": 5} # Ưu tiên tên bài hát hơn tên ca sĩ
            )
            logger.info("Created Text Index for Songs")
        except Exception as e:
            logger.warning(f"Index creation warning: {e}")

    async def search_songs(self, query: str, limit: int = 20, skip: int = 0) -> List[dict]:
        """
        Tìm kiếm bài hát Full-text Search.
        """
        if not query:
            return []

        try:
            # 1. Query chuẩn
            filter_query = {"$text": {"$search": query}}
            projection = {
                "score": {"$meta": "textScore"}, 
                "title": 1, "artist": 1, "image_url": 1, "id": 1 # Chỉ lấy trường cần thiết
            }

            cursor = DB.db["songs"].find(filter_query, projection)
            
            # 2. Sắp xếp theo độ khớp (Score) giảm dần
            cursor = cursor.sort([("score", {"$meta": "textScore"})])
            
            # 3. Phân trang (Skip & Limit)
            songs = await cursor.skip(skip).limit(limit).to_list(length=limit)
            return songs

        except OperationFailure as e:
            # Lỗi này xảy ra nếu quên tạo Text Index
            logger.error(f"MongoDB Text Search Error: {e}")
            # Fallback: Nếu lỗi text search, dùng Regex search (chậm hơn nhưng an toàn)
            return await self.search_songs_fallback(query, limit)

    async def search_songs_fallback(self, query: str, limit: int) -> List[dict]:
        """Tìm kiếm bằng Regex (chậm hơn) khi Text Index bị lỗi"""
        filter_query = {
            "$or": [
                {"title": {"$regex": query, "$options": "i"}},
                {"artist": {"$regex": query, "$options": "i"}}
            ]
        }
        return await DB.db["songs"].find(filter_query).limit(limit).to_list(length=limit)

    async def get_song_by_id(self, song_id: Any) -> Optional[dict]:
        """
        Lấy chi tiết bài hát. Tự động xử lý ID dạng int hoặc str.
        """
        # Thử tìm với ID gốc
        song = await DB.db["songs"].find_one({"_id": song_id})
        
        # Nếu không thấy, thử ép kiểu (vì ID từ URL luôn là string)
        if not song:
            try:
                # Nếu DB lưu ID là Int (ví dụ dataset Million Song)
                int_id = int(song_id)
                song = await DB.db["songs"].find_one({"_id": int_id})
            except ValueError:
                pass # Không phải số thì thôi
        
        return song

# Instance Singleton
music_service = MusicService()