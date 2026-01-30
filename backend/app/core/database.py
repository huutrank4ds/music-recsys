# app/core/database.py
from motor.motor_asyncio import AsyncIOMotorClient, AsyncIOMotorDatabase #type: ignore
from pymilvus import connections #type: ignore
import redis.asyncio as redis #type: ignore
from common.logger import get_logger
from config import MONGO_URI, MONGO_DB, REDIS_URL, MILVUS_URI
import os

NAME_TASK = "Database Core"
logger = get_logger(NAME_TASK)

class Database:
    # khai báo kiểu dữ liệu
    _client: AsyncIOMotorClient
    _db: AsyncIOMotorDatabase
    _redis: redis.Redis

    def __init__(self):
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
        client = AsyncIOMotorClient(MONGO_URI)
        self._client = client
        self._db = client[MONGO_DB]
        await self._client.admin.command('ping')
        logger.info("Connected to MongoDB!")

    async def connect_to_redis(self):
        self._redis = redis.from_url(REDIS_URL, decode_responses=False)
        await self._redis.ping()
        logger.info("Connected to Redis")

    def connect_to_milvus(self):
        connections.connect(
            alias="default", 
            uri=MILVUS_URI
        )
        logger.info("Milvus connection initialized")

    async def close(self):
        if self._client:
            self._client.close()
        if self._redis:
            return await self._redis.close()
        return None
    
# Singleton Instance
DB = Database()