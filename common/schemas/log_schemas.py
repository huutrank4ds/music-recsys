import time
from pydantic import BaseModel, Field #type: ignore
from typing import Literal, Optional

class UserLogRequest(BaseModel):
    user_id: str
    track_id: str
    timestamp: Optional[int] = Field(None) # Cho phép null để backend tự điền
    action: Literal["listen", "skip", "complete"]
    source: Literal["real_user", "simulation"]
    duration: int = 0
    total_duration: int = 0

def get_spark_msg(input: UserLogRequest) -> dict:
    return {
        "user_id": str(input.user_id),
        "track_id": str(input.track_id),
        "timestamp": input.timestamp or int(time.time() * 1000),
        "action": str(input.action),
        "source": str(input.source or "simulation"),
        "duration": int(input.duration),
        "total_duration": int(input.total_duration)
    }