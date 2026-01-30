# app/api/recommemdation_api.py
from fastapi import APIRouter #type: ignore
from app.services.recommendation_service_be import recommendation_service
from common.logger import get_logger #type: ignore

router = APIRouter()
logger = get_logger("Recommendation API") 
num_default_recs = 32

@router.get("/{user_id}")
async def get_recommendations(user_id: str, limit: int = num_default_recs, refresh: bool = False):
    """
    Gợi ý trang chủ cho user (Collaborative Filtering).
    Dựa trên user profile vector từ ALS model.
    """
    try:
        personalized_recs = await recommendation_service.get_home_feed(user_id, limit=limit, refresh=refresh)

        return {
            "has_more": personalized_recs.get("has_more", False),
            "songs": personalized_recs.get("songs", []),
        }
    except Exception as e:
        logger.error(f"Lỗi khi lấy gợi ý cho user {user_id}: {str(e)}")
        return {
            "has_more": False,
            "songs": [],
        }

@router.get("/{user_id}/{current_song_id}")
async def get_next_songs(user_id: str, current_song_id: str, limit: int = num_default_recs, refresh: bool = False):
    """
    Gợi ý bài hát tiếp theo cho user dựa trên bài hát hiện tại.
    Kết hợp giữa Collaborative Filtering và Content-Based Filtering.
    """
    try:
        next_songs = await recommendation_service.get_next_songs(user_id, current_song_id, limit=limit, refresh=refresh)
        
        return {
            "has_more": next_songs.get("has_more", False),
            "next_songs": next_songs.get("next_songs", []),
        }
    except Exception as e:
        logger.error(f"Lỗi khi lấy bài hát tiếp theo cho user {user_id} từ bài {current_song_id}: {str(e)}")
        return {
            "has_more": False,
            "next_songs": [],
        }
    
@router.post("/record/{user_id}/{song_id}")
async def update_short_term_profile(user_id: str, song_id: str, decay_rate: float = 0.7):
    """
    Cập nhật Short-term Vector cho user dựa trên bài hát mới nghe.
    """
    try:
        await recommendation_service.update_short_term_profile(user_id, song_id, decay_rate)
        return {
            "status": "success", 
            "message": f"Đã cập nhật Short-term vector cho user {user_id}."
        }
    except Exception as e:
        logger.error(f"Lỗi khi cập nhật Short-term vector cho user {user_id} với bài {song_id}: {str(e)}")
        return {
            "status": "error", 
            "message": f"Lỗi khi cập nhật Short-term vector: {str(e)}"
        }