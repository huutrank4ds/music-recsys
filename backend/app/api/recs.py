from fastapi import APIRouter #type: ignore
from app.services.recommender import RecommendationService

router = APIRouter()
recs = RecommendationService('music_collection')

@router.get("/recommendations/{user_id}/n={n}")
async def get_recommendations(user_id: str, n: int = 20):
    personalized_recs = await recs.get_personalized_recs(user_id, limit=n)
    
    return {
        "personalized_recommendations": personalized_recs,
    }

@router.get("/next-songs/{user_id}/{current_song_id}/n={n}")
async def get_next_songs(user_id: str, current_song_id: str, n: int = 20):
    next_songs = await recs.get_next_songs(user_id, current_song_id, limit=n)
    
    return {
        "next_songs": next_songs,
    }