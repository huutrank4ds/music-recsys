from fastapi import APIRouter 
from app.services.recommender import RecommendationService

router = APIRouter()
recs = RecommendationService()  

<<<<<<< HEAD
@router.get("/recommendations/{user_id}/n={n}")
async def get_recommendations(user_id: str, n: int = 20):
    personalized_recs = await recs.get_personalized_recs(user_id, limit=n)
=======
@router.get("/recommendations/{user_id}")
async def get_recommendations(user_id: str):
    """
    Gợi ý trang chủ cho user (Collaborative Filtering).
    Dựa trên user profile vector từ ALS model.
    """
    personalized_recs = await recs.get_personalized_recs(user_id)
>>>>>>> 93888aabe9b70bc04a04a282dc369eb1310abdca
    
    return {
        "user_id": user_id,
        "type": "collaborative_filtering",
        "recommendations": personalized_recs,
    }

<<<<<<< HEAD
@router.get("/next-songs/{user_id}/{current_song_id}/n={n}")
async def get_next_songs(user_id: str, current_song_id: str, n: int = 20):
    next_songs = await recs.get_next_songs(user_id, current_song_id, limit=n)
=======
@router.get("/next-songs/{user_id}/{current_song_id}")
async def get_next_songs(user_id: str, current_song_id: str):
    """
    HYBRID: Gợi ý bài tiếp theo.
    Kết hợp 60% Collaborative Filtering + 40% Content-Based (Lyrics).
    """
    next_songs = await recs.get_next_songs(user_id, current_song_id)
>>>>>>> 93888aabe9b70bc04a04a282dc369eb1310abdca
    
    return {
        "user_id": user_id,
        "current_song_id": current_song_id,
        "type": "hybrid",
        "next_songs": next_songs,
    }

@router.get("/similar/{track_id}")
async def get_similar_songs(track_id: str, limit: int = 10):
    """
    Content-Based: Tìm bài có lyrics tương tự.
    Dựa trên Sentence Transformer embeddings của lyrics.
    """
    similar_songs = await recs.get_content_based_recs(track_id, limit)
    
    return {
        "track_id": track_id,
        "type": "content_based",
        "similar_songs": similar_songs,
    }

@router.get("/cold-start")
async def get_cold_start(limit: int = 20):
    """
    Cold Start: Gợi ý cho user mới.
    Trả về bài hát phổ biến nhất.
    """
    popular_songs = await recs.cold_start_recs(limit)
    
    return {
        "type": "cold_start",
        "recommendations": popular_songs,
    }