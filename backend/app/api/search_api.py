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