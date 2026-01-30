import asyncio
import os
from contextlib import asynccontextmanager
from fastapi import FastAPI #type: ignore
from fastapi.middleware.cors import CORSMiddleware #type: ignore
import config as cfg

from app.core.database import DB
from app.core.kafka_client import kafka_manager 
from app.services.logging_service_be import logging_service
from app.services.recommendation_service_be import recommendation_service
from common.logger import get_logger

from app.api.recommemdation_api import router as recommendation_router
from app.api.search_api import router as search_router
from app.api.logging_api import router as logging_router
from app.api.user_api import router as user_router



logger = get_logger("Backend Main")

# Cấu hình Retry
MAX_RETRIES = 5       # Thử lại tối đa 5 lần
RETRY_DELAY = 5       # Đợi 5 giây giữa mỗi lần thử

allow_origins = [
    f"http://localhost:{cfg.FRONTEND_PORT}",
    f"http://127.0.0.1:{cfg.FRONTEND_PORT}"
]

@asynccontextmanager
async def lifespan(app: FastAPI):
    logger.info("Đang khởi động hệ thống...")
    
    # Kết nối Database
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            logger.info(f"Đang kết nối Database (Lần thử {attempt}/{MAX_RETRIES})...")
            
            # 1. Kết nối Mongo & Redis (Async)
            await DB.connect_to_mongo()
            logger.info("MongoDB đã kết nối!")
            await DB.connect_to_redis()
            logger.info("Redis đã kết nối!")
            # 2. Kết nối Milvus
            await recommendation_service.start()
            logger.info("Milvus đã kết nối!") 
            break 
            
        except Exception as e:
            logger.warning(f"Kết nối DB thất bại: {str(e)}")
            
            if attempt == MAX_RETRIES:
                logger.error(f"CRITICAL: Không thể kết nối Database sau {MAX_RETRIES} lần thử.")
                raise e
            
            logger.info(f"Chờ {RETRY_DELAY}s trước khi thử lại...")
            await asyncio.sleep(RETRY_DELAY)

    # Kết nối Kafka
    try:
        logger.info("Đang khởi tạo Kafka Service...")
        logging_service.initialize() # Tạo topic nếu chưa có
        logger.info("Kafka Service đã sẵn sàng.")
    except Exception as e:
        logger.warning(f"Kafka chưa sẵn sàng (Lỗi: {e}). Hệ thống sẽ tự động kết nối lại sau.")
    yield # --- APP RUNNING --- (App bắt đầu nhận request tại đây)

    
    logger.info("Đang tắt hệ thống...")
    try:
        kafka_manager.stop()
        logger.info("Đã đóng Kafka Producer.")
    except Exception as e:
        logger.error(f"Lỗi khi đóng Kafka: {e}")

    # Đóng kết nối Database
    try:
        if hasattr(DB, "close"):
            await DB.close() 
            logger.info("Đã đóng kết nối Database.")
    except Exception as e:
        logger.error(f"Lỗi khi đóng Database: {e}")

# Khởi tạo App
app = FastAPI(
    title="Music RecSys API",
    version="1.0.0",
    lifespan=lifespan
)

# Cấu hình CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=allow_origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Đăng ký Routers
app.include_router(recommendation_router, prefix="/api/v1/recs", tags=["Recommendation"])
app.include_router(search_router, prefix="/api/v1/search", tags=["Search"])
app.include_router(logging_router, prefix="/api/v1/logs", tags=["User Logging"])
app.include_router(user_router, prefix="/api/v1", tags=["User Management"])

@app.get("/", tags=["Health"])
async def health_check():
    return {"status": "ok", "service": "Music Backend"}