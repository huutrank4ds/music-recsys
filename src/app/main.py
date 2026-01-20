from fastapi import FastAPI #type: ignore
from contextlib import asynccontextmanager
from app.core.database import DB # Import đối tượng DB duy nhất
from pymilvus import Collection, utility #type: ignore
import asyncio
from api.recs import router as recommendation_router
from api.search import router as search_router 


@asynccontextmanager
async def lifespan(app: FastAPI):
    # --- GIAI ĐOẠN STARTUP (KHỞI CHẠY) ---
    print("🚀 Đang khởi tạo các kết nối dịch vụ...")
    
    try:
        # 1. Kết nối MongoDB
        await DB.connect_to_mongo()
        
        # 2. Kết nối Redis
        await DB.connect_to_redis()
        
        # 3. Kết nối Milvus
        DB.connect_to_milvus()

        # 4. Kiểm tra và nạp dữ liệu Milvus lên RAM
        collection_name = "music_collection"
        # Đợi một chút để Milvus ổn định sau khi connect
        if utility.has_collection(collection_name):
            col = Collection(collection_name)
            col.load()
            print(f"✅ Milvus Collection '{collection_name}' đã được nạp vào RAM")
        else:
            print(f"⚠️ Cảnh báo: Collection '{collection_name}' chưa tồn tại. Hãy chạy ETL trước.")

    except Exception as e:
        print(f"❌ Lỗi nghiêm trọng khi khởi động hệ thống: {e}")
        # Tùy chọn: Có thể dừng App nếu các kết nối bắt buộc thất bại
        # raise e

    yield # Tại điểm này, ứng dụng bắt đầu nhận các yêu cầu API

    # --- GIAI ĐOẠN SHUTDOWN (TẮT MÁY) ---
    print("🛑 Đang đóng các kết nối dịch vụ...")
    try:
        # Giải phóng RAM Milvus
        if utility.has_collection(collection_name):
            col = Collection(collection_name)
            col.release()
            print("✅ Đã giải phóng RAM Milvus")
        
        # Đóng kết nối Redis (nếu thư viện hỗ trợ)
        if DB.redis:
            await DB.redis.close()
            
    except Exception as e:
        print(f"⚠️ Lỗi khi đóng dịch vụ: {e}")

# Khởi tạo FastAPI với lifespan
app = FastAPI(
    title="Big Data Music Recommendation System",
    lifespan=lifespan
)

# Ví dụ gọi trong lúc khởi động FastAPI
@app.on_event("startup")
async def startup_db_client():
    # Đảm bảo index luôn tồn tại để MusicService không bị lỗi
    await DB.db["songs"].create_index([("title", "text"), ("artist", "text")])

# Đăng ký Router (Ví dụ)
# from app.api.endpoints import recommendation_router
# app.include_router(recommendation_router, prefix="/api/v1")

@app.get("/")
async def health_check():
    return {
        "status": "online",
        "database": {
            "mongodb": "connected" if DB.client else "disconnected",
            "redis": "connected" if DB.redis else "disconnected"
        }
    }

app.include_router(recommendation_router, prefix="/api/v1")
app.include_router(search_router, prefix="/api/v1")

