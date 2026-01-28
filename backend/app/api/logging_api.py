# app/api/logging.py
from fastapi import APIRouter, HTTPException, Depends #type: ignore
from common.schemas.log_schemas import UserLogRequest
from common.logger import get_logger

# Import đúng Service Class và Instance từ file service đã refactor
from app.services.logging_service import logging_service, LoggingService

router = APIRouter()
logger = get_logger("Logging API")

@router.post("/event")
async def log_user_event(
    log: UserLogRequest,
    service: LoggingService = Depends(lambda: logging_service)
):
    """
    API nhận log hành vi.
    User -> API -> Service (Map Schema) -> Core (Kafka Producer) -> Kafka Broker.
    """
    try:
        result = service.send_log(log)
        
        return {
            "status": "ok",
            "message": "Logged successfully",
            "data": {
                "user_id": result["user_id"],
                "timestamp": result["timestamp"]
            }
        }
    except Exception as e:
        logger.error(f"[API Log] Lỗi khi xử lý log: {e}")
        raise HTTPException(status_code=500, detail="Failed to process log")