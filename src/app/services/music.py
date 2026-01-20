from app.core.database import DB
from typing import List, Optional

class MusicService:
    _instance = None

    def __new__(cls):
        if not cls._instance:
            cls._instance = super(MusicService, cls).__new__(cls)
        return cls._instance

    async def search_songs(self, query: str, limit: int = 20) -> List[dict]:
        """
        Tìm kiếm bài hát sử dụng MongoDB Full-text Search.
        Hỗ trợ tìm kiếm mờ theo cụm từ và sắp xếp theo độ liên quan.
        """
        # 1. Sử dụng toán tử $text để tận dụng Text Index
        filter_query = {"$text": {"$search": query}}
        
        # 2. Tạo một trường ảo 'score' để biết bài nào khớp với từ khóa nhất
        projection = {"score": {"$meta": "textScore"}}
        
        # 3. Thực hiện truy vấn, sắp xếp theo score cao nhất và giới hạn số lượng
        cursor = DB.db["songs"].find(filter_query, projection)
        
        # Sắp xếp theo độ liên quan (textScore)
        cursor = cursor.sort([("score", {"$meta": "textScore"})])
        
        songs = await cursor.limit(limit).to_list(limit)
        return songs

    async def get_song_by_id(self, song_id: str) -> Optional[dict]:
        """Lấy thông tin chi tiết bài hát bằng ID (khóa chính _id)"""
        return await DB.db["songs"].find_one({"_id": song_id})

# Khởi tạo instance duy nhất (Singleton)
music_service = MusicService()