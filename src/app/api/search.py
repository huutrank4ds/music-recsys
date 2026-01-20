from fastapi import APIRouter, Query, HTTPException #type: ignore
from typing import List, Dict, Any
from app.services.music import music_service # Import service Singleton đã viết

router = APIRouter()

# --- Helper Function: Xử lý ObjectId ---
def serialize_song(doc: dict) -> dict:
    """
    Chuyển đổi ObjectId của MongoDB thành chuỗi (str)
    để FastAPI có thể trả về JSON mà không bị lỗi.
    """
    if doc and "_id" in doc:
        doc["_id"] = str(doc["_id"])
    return doc

@router.get("/search", summary="Tìm kiếm bài hát (Text Search)")
async def search_songs(
    q: str = Query(..., min_length=1, description="Từ khóa tìm kiếm (Tên bài, Ca sĩ)"),
    limit: int = Query(default=20, ge=1, le=50, description="Số lượng kết quả tối đa")
):
    """
    API tìm kiếm bài hát sử dụng MongoDB Text Index.
    - Input: Từ khóa (q)
    - Output: Danh sách bài hát được sắp xếp theo độ khớp (Score).
    """
    try:
        # 1. Gọi logic tìm kiếm từ MusicService
        raw_songs = await music_service.search_songs(q, limit)
        
        # 2. Serialize dữ liệu (Convert ObjectId -> str)
        results = [serialize_song(song) for song in raw_songs]
        
        # 3. Trả về kết quả chuẩn
        return {
            "status": "success",
            "query": q,
            "count": len(results),
            "data": results
        }

    except Exception as e:
        # Log lỗi ra console để debug
        print(f"❌ Search Error: {e}")
        # Trả về lỗi 500 cho client
        raise HTTPException(status_code=500, detail="Lỗi hệ thống khi tìm kiếm bài hát")

@router.get("/songs/{song_id}", summary="Lấy chi tiết bài hát")
async def get_song_detail(song_id: str):
    """API lấy thông tin một bài hát cụ thể theo ID"""
    try:
        song = await music_service.get_song_by_id(song_id)
        if not song:
            raise HTTPException(status_code=404, detail="Không tìm thấy bài hát")
            
        return {
            "status": "success",
            "data": serialize_song(song)
        }
    except HTTPException:
        raise
    except Exception as e:
        print(f"❌ Detail Error: {e}")
        raise HTTPException(status_code=500, detail="Lỗi hệ thống")