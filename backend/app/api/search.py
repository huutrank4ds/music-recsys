from typing import List, Optional, Any
from fastapi import APIRouter, HTTPException, Query #type: ignore
from app.services.music import music_service
from common.logger import get_logger

# Logger nên lấy ở ngoài để tránh khởi tạo lại nhiều lần
logger = get_logger("SearchAPI")

router = APIRouter()

# API Tìm kiếm
@router.get("", summary="Tìm kiếm bài hát (Text Search)")
async def search_songs(
    # Khai báo tham số trong hàm, FastAPI tự hiểu là Query Param (?query=...)
    query: str = Query(..., min_length=1, description="Từ khóa tìm kiếm"),
    limit: int = Query(15, ge=1, le=100, description="Số lượng bài hát (Mặc định 15)"),
    skip: int = Query(0, ge=0, description="Số lượng bỏ qua (để phân trang)")
) -> Any:
    """
    Tìm kiếm bài hát dựa trên từ khóa.
    URL gọi sẽ là: /api/search?query=abc&limit=10&skip=0
    """
    try:
        # Gọi Service (Service đã trả về đúng format {data, meta} rồi nên return luôn)
        result = await music_service.search_songs(query, limit, skip)
        return result
        
    except Exception as e:
        logger.error(f"Lỗi khi tìm kiếm bài hát: {e}")
        # Trả về lỗi 500 nhưng kèm message chung chung để bảo mật
        raise HTTPException(status_code=500, detail="Lỗi hệ thống khi tìm kiếm")