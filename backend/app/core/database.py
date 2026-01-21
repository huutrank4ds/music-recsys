from motor.motor_asyncio import AsyncIOMotorClient, AsyncIOMotorDatabase #type: ignore
from pymilvus import connections #type: ignore
import redis.asyncio as redis #type: ignore
import os

class Database:
    # Chỉ khai báo kiểu dữ liệu, không gán = None ở đây để tránh Pylance hiểu lầm
    _client: AsyncIOMotorClient
    _db: AsyncIOMotorDatabase
    _redis: redis.Redis

    def __init__(self):
        # Gán giá trị thực tế trong init hoặc để mặc định
        self._client = None
        self._db = None
        self._redis = None

    @property
    def client(self) -> AsyncIOMotorClient:
        return self._client

    @property
    def db(self) -> AsyncIOMotorDatabase:
        return self._db

    @property
    def redis(self) -> redis.Redis:
        return self._redis

    async def connect_to_mongo(self):
        url = os.getenv("MONGO_URL", "mongodb://mongodb:27017")
        client = AsyncIOMotorClient(url)
        self._client = client
        self._db = client["music_recsys"]
        await self._client.admin.command('ping')
        print("✅ Connected to MongoDB")

    async def connect_to_redis(self):
        url = os.getenv("REDIS_URL", "redis://redis:6379")
        self._redis = redis.from_url(url, decode_responses=False)
        await self._redis.ping()
        print("✅ Connected to Redis")

    def connect_to_milvus(self):
        connections.connect(
            alias="default", 
            host=os.getenv("MILVUS_HOST", "milvus"), 
            port=os.getenv("MILVUS_PORT", "19530")
        )
        print("✅ Milvus connection initialized")

    async def close(self):
        if self._client:
            self._client.close()
        if self._redis:
            return await self._redis.close()
        return None
    
# Singleton Instance
DB = Database()