from fastapi import APIRouter #type: ignore
from services.recommender import RecommendationService

router = APIRouter()
recs = RecommendationService('music_collection')

@router.get("/recommendations/{user_id}")
async def get_recommendations(user_id: str):
    personalized_recs = await recs.get_personalized_recs(user_id)
    
    return {
        "personalized_recommendations": personalized_recs,
    }

@router.get("/next-songs/{user_id}/{current_song_id}")
async def get_next_songs(user_id: str, current_song_id: str):
    next_songs = await recs.get_next_songs(user_id, current_song_id)
    
    return {
        "next_songs": next_songs,
    }