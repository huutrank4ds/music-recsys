from fastapi import FastAPI #type: ignore
from contextlib import asynccontextmanager

# Chỉ import DB và các Router
from app.core.database import DB
from app.api.recs import router as recommendation_router
from app.api.search import router as search_router
from app.api.logging import router as logging_router
from common.logger import get_logger

logger = get_logger("Backend Main")

@asynccontextmanager
async def lifespan(app: FastAPI):
    # --- STARTUP ---
    logger.info("[System] Đang khởi động hệ thống...")
    try:
        # Chỉ thực hiện kết nối cơ sở dữ liệu
        await DB.connect_to_mongo()
        await DB.connect_to_redis()
        DB.connect_to_milvus() # Kết nối Milvus là sync
        
        logger.info("[System] Database đã kết nối và sẵn sàng!")
    except Exception as e:
        logger.error(f"[Critical Error] Lỗi khi kết nối DB: {e}")
        raise e

    yield # App phục vụ request...

    # --- SHUTDOWN ---
    logger.info("[System] Đang tắt hệ thống...")
    try:
        if hasattr(DB, "close"):
            await DB.close()
            logger.info("[System] Đóng kết nối DB thành công.")
    except Exception as e:
        logger.error(f"[Warning] Lỗi đóng kết nối: {e}")

# Khởi tạo App
app = FastAPI(
    title="Music RecSys API",
    version="1.0.0",
    lifespan=lifespan
)

# Đăng ký Routers
app.include_router(recommendation_router, prefix="/api/v1/recs", tags=["Recommendation"])
app.include_router(search_router, prefix="/api/v1/search", tags=["Search"])
app.include_router(logging_router, prefix="/api/v1/logs", tags=["User Logging"])

@app.get("/", tags=["Health"])
async def health_check():
    return {"status": "ok", "service": "Music Backend"}