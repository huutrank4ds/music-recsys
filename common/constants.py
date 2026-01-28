# common/constants.py
from typing import Dict, Any

SONG_SUMMARY_PROJECTION: Dict[str, Any] = {
    "_id": 1,
    "track_name": 1,
    "artist_name": 1,
    "artist_id": 1,
    "image_url": 1,
    "url": 1,
    "duration": 1,
    "plays_cumulative": 1,
    "plays_7d": 1,
    "release_date": 1, 
}

USER_SUMARY_PROJECTION: Dict[str, Any] = {
    "_id": 1,
    "username": 1,
    "image_url": 1,
}