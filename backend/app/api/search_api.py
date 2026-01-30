# app/api/search_api.py
from typing import Any
from fastapi import APIRouter, HTTPException #type: ignore
from app.services.search_service_be import search_service
from common.logger import get_logger

# Logger nên lấy ở ngoài để tránh khởi tạo lại nhiều lần
logger = get_logger("Search API")
router = APIRouter()

# API Tìm kiếm
@router.get("", summary="Tìm kiếm bài hát (Text Search)")
async def search_songs(query, limit, skip) -> Any:
    """
    Tìm kiếm bài hát dựa trên từ khóa.
    URL gọi sẽ là: /api/search?query=abc&limit=10&skip=0
    """
    try:
        result = await search_service.search_songs(query, int(limit), int(skip))
        return result
        
    except Exception as e:
        logger.error(f"Lỗi khi tìm kiếm bài hát: {e}")
        raise HTTPException(status_code=500, detail="Lỗi hệ thống khi tìm kiếm")
    
# API Lấy bài hát theo ID
@router.get("/{song_id}", summary="Lấy bài hát theo ID")
async def get_song_by_id(song_id: str) -> Any:
    """
    Lấy thông tin bài hát theo song_id.
    URL gọi sẽ là: /api/search/{song_id}
    """
    try:
        result = await search_service.get_song_by_id(song_id)
        if result is None:
            raise HTTPException(status_code=404, detail="Bài hát không tìm thấy")
        return result
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Lỗi khi lấy bài hát theo ID: {e}")
        raise HTTPException(status_code=500, detail="Lỗi hệ thống khi lấy bài hát")