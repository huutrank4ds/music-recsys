import time
import json
from fastapi import APIRouter, HTTPException
from pydantic import BaseModel, Field 
from typing import Literal
from confluent_kafka import Producer 
from common.logger import get_logger

logger = get_logger("Logging_API")
router = APIRouter()

### SCHEMA 
class UserLogRequest(BaseModel):
    """
    Schema cho log hành vi người dùng.
    Phải khớp với get_music_log_schema() trong common/schemas.py
    
    Actions:
    - listen: User bắt đầu nghe bài hát
    - skip: User skip bài hát (chưa nghe hết)
    - complete: User nghe hết bài hát
    - like: User like bài hát (explicit positive feedback)
    - dislike: User dislike bài hát (explicit negative feedback)
    """
    user_id: str = Field(..., description="ID người dùng")
    track_id: str = Field(..., description="ID bài hát")
    action: Literal["listen", "skip", "complete", "like", "dislike"] = Field(..., description="Loại hành vi")
    duration: int = Field(0, ge=0, description="Thời gian đã nghe (ms) - optional cho like/dislike")
    total_duration: int = Field(0, ge=0, description="Tổng thời lượng bài hát (ms) - optional cho like/dislike")

### KAFKA PRODUCER
# Singleton Kafka Producer (khởi tạo 1 lần)
_kafka_producer = None

def get_kafka_producer():
    global _kafka_producer
    if _kafka_producer is None:
        import os
        kafka_servers = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:29092")
        _kafka_producer = Producer({
            'bootstrap.servers': kafka_servers,
            'client.id': 'backend-api',
            'acks': '1',
        })
        logger.info(f"[Kafka] Producer initialized: {kafka_servers}")
    return _kafka_producer

# ================= API ENDPOINTS =================
@router.post("/event")
async def log_user_event(log: UserLogRequest):
    """
    Nhận log hành vi người dùng từ Frontend và đẩy vào Kafka.
    
    - **user_id**: ID người dùng
    - **track_id**: ID bài hát đang tương tác
    - **action**: listen | skip | complete
    - **duration**: Thời gian đã nghe (milliseconds)
    - **total_duration**: Tổng thời lượng bài hát (milliseconds)
    """
    try:
        # Chuẩn bị message
        message = {
            "user_id": log.user_id,
            "track_id": log.track_id,
            "timestamp": int(time.time() * 1000),  
            "action": log.action,
            "duration": log.duration,
            "total_duration": log.total_duration,
            "source": "real_user" 
        }
        
        # Gửi vào Kafka
        import os
        topic = os.getenv("KAFKA_TOPIC", "music_log")
        producer = get_kafka_producer()
        producer.produce(
            topic,
            value=json.dumps(message).encode('utf-8')
        )
        producer.poll(0)  
        
        logger.info(f"[Log] User={log.user_id} | Track={log.track_id} | Action={log.action}")
        
        return {
            "status": "ok",
            "message": "Event logged successfully",
            "timestamp": message["timestamp"]
        }
        
    except Exception as e:
        logger.error(f"[Log Error] {e}")
        raise HTTPException(status_code=500, detail=f"Failed to log event: {str(e)}")
