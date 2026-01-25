import asyncio
import os
from contextlib import asynccontextmanager
from fastapi import FastAPI #type: ignore
from fastapi.middleware.cors import CORSMiddleware #type: ignore

# Import các module nội bộ
from app.core.database import DB
from app.api.recs import router as recommendation_router
from app.api.search import router as search_router
from app.api.logging import router as logging_router
from common.logger import get_logger

logger = get_logger("Backend Main")

# Cấu hình Retry
MAX_RETRIES = 5       # Thử lại tối đa 5 lần
RETRY_DELAY = 5       # Đợi 5 giây giữa mỗi lần thử

FRONTEND_PORT = os.getenv("FRONTEND_PORT", "5173")

allow_origins = [
    f"http://localhost:{FRONTEND_PORT}",
    f"http://127.0.0.1:{FRONTEND_PORT}",
    "*", 
]

@asynccontextmanager
async def lifespan(app: FastAPI):
    # --- STARTUP LOGIC ---
    logger.info("[Backend] Đang khởi động hệ thống...")
    
    # Cơ chế Retry kết nối Database
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            logger.info(f"[Backend] Đang kết nối Database (Lần thử {attempt}/{MAX_RETRIES})...")
            
            # 1. Kết nối Mongo & Redis (Async)
            await DB.connect_to_mongo()
            await DB.connect_to_redis()
            
            # 2. Kết nối Milvus (Sync hoặc Async tùy driver, nhưng cứ để trong try/catch)
            DB.connect_to_milvus() 

            logger.info("[Backend] Database đã kết nối và sẵn sàng!")
            break  # Nếu thành công thì thoát vòng lặp ngay
            
        except Exception as e:
            logger.warning(f"[Backend] Kết nối thất bại: {str(e)}")
            
            # Nếu đã là lần thử cuối cùng mà vẫn lỗi -> Raise lỗi để sập App (vì không có DB không chạy được)
            if attempt == MAX_RETRIES:
                logger.error(f"[Backend] Không thể kết nối Database sau {MAX_RETRIES} lần thử.")
                raise e
            
            # Nếu chưa hết lượt -> Đợi X giây rồi thử lại
            logger.info(f"[Backend] Chờ {RETRY_DELAY}s trước khi thử lại...")
            await asyncio.sleep(RETRY_DELAY)

    yield # --- APP RUNNING --- (Tại đây App bắt đầu nhận request)

    # --- SHUTDOWN LOGIC ---
    logger.info("[Backend] Đang tắt hệ thống...")
    try:
        if hasattr(DB, "close"):
            # Lưu ý: DB.close() có thể là async hoặc sync tùy code của bạn. 
            # Nếu là async thì dùng await, nếu sync thì bỏ await.
            # Ở đây mình giả định là async cho an toàn.
            await DB.close() 
            logger.info("[Backend] Đóng kết nối DB thành công.")
    except Exception as e:
        logger.error(f"[Backend] Lỗi khi đóng kết nối: {e}")

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

@app.get("/", tags=["Health"])
async def health_check():
    return {"status": "ok", "service": "Music Backend"}