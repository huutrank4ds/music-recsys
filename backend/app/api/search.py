from fastapi import APIRouter, Query, HTTPException #type: ignore
from typing import List, Optional, Any
from app.services.music import music_service
from common.logger import get_logger

router = APIRouter()
logger = get_logger("SearchAPI")

# --- 1. Helper Function: Xử lý ObjectId & Rename ---
def serialize_song(doc: dict) -> dict:
    """
    - Chuyển ObjectId -> str
    - Đổi tên '_id' thành 'id' (Frontend React thích điều này)
    """
    if not doc:
        return {}
        
    # Tạo copy để không ảnh hưởng dữ liệu gốc nếu cần dùng lại
    doc = doc.copy()
    
    if "_id" in doc:
        doc["id"] = str(doc.pop("_id")) # Cắt _id ra và gán vào id
    
    return doc

# --- 2. API Endpoints ---

@router.get("", summary="Tìm kiếm bài hát (Text Search)")
async def search_songs(
    q: str = Query(..., min_length=1, description="Từ khóa (Tên bài, Ca sĩ)"),
    limit: int = Query(default=20, ge=1, le=50, description="Số lượng kết quả"),
    skip: int = Query(default=0, ge=0, description="Số lượng bỏ qua (Phân trang)")
):
    """
    API tìm kiếm bài hát sử dụng MongoDB Text Index.
    """
    try:
        # Gọi Service
        raw_songs = await music_service.search_songs(q, limit, skip)
        
        # Serialize dữ liệu
        results = [serialize_song(song) for song in raw_songs]
        
        return {
            "status": "success",
            "query": q,
            "count": len(results),
            "data": results
        }

    except Exception as e:
        logger.error(f"Lỗi khi tìm kiếm bài hát: {e}")
        return {"status": "error", "message": str(e), "data": []}

@router.get("/{song_id}", summary="Lấy chi tiết bài hát")
async def get_song_detail(song_id: str):
    """
    Lấy thông tin chi tiết bài hát bằng ID.
    Hỗ trợ cả ID dạng chuỗi (MongoDB cũ) và số (Dataset).
    """
    try:
        song = await music_service.get_song_by_id(song_id)
        
        if not song:
            raise HTTPException(status_code=404, detail="Không tìm thấy bài hát")
            
        return {
            "status": "success",
            "data": serialize_song(song)
        }
        
    except HTTPException:
        raise # Ném lại lỗi 404
    except Exception as e:
        logger.error(f"Lỗi khi lấy chi tiết bài hát: {e}")
        raise HTTPException(status_code=500, detail="Lỗi hệ thống")