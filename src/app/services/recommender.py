import numpy as np
import json
from pymilvus import Collection #type: ignore
from app.core.database import DB
import random
from typing import Dict

class RecommendationService:
    # Dictionary để lưu trữ các instance dựa trên collection_name
    _instances: Dict[str, 'RecommendationService'] = {}
    
    # Gợi ý cho Pylance biết instance sẽ có thuộc tính collection
    collection: Collection 
    collection_name: str

    def __new__(cls, collection_name: str = "music_collection"):
        if collection_name not in cls._instances:
            # Tạo instance mới
            instance = super(RecommendationService, cls).__new__(cls)
            
            # Khởi tạo giá trị cho instance
            instance.collection = Collection(collection_name)
            instance.collection_name = collection_name
            
            cls._instances[collection_name] = instance
            print(f"🚀 Created new Service Instance for: {collection_name}")
            
        return cls._instances[collection_name]

    async def get_personalized_recs(self, user_id: str, limit: int = 20):
        # 1. Lấy Long-term vector (từ Redis hoặc Mongo)
        long_key = f"user:long:{user_id}"
        v_long_raw = await DB.redis.get(long_key)
        
        if v_long_raw:
            v_long = np.array(json.loads(v_long_raw))
        else:
            user_data = await DB.db["users"].find_one({"user_id": user_id})
            if not user_data:
                return await DB.db["songs"].find().limit(limit).to_list(limit)
            v_long = np.array(user_data["latent_vector"])
            await DB.redis.setex(long_key, 7200, json.dumps(v_long.tolist()))

        # 2. Lấy Short-term vector (Lịch sử nghe trong ngày)
        short_key = f"user:short:{user_id}"
        v_short_raw = await DB.redis.get(short_key)
        
        if v_short_raw:
            v_short = np.array(json.loads(v_short_raw))
            v_home = 0.6 * v_long + 0.4 * v_short
        else:
            v_home = v_long

        # 3. Search Milvus với vector đã "biến thiên"
        search_params = {"metric_type": "IP", "params": {"nprobe": 10}}
        results = self.collection.search(
                data=[v_home.tolist()],
                anns_field="embedding",
                param=search_params,
                limit=limit * 2,  # Lấy nhiều hơn để chọn lọc ngẫu nhiên
                output_fields=["id"]
            )

        # 2. Lấy toàn bộ ID từ tất cả bài đó
        all_song_ids = [hit.id for hit in results[0]]
        selected_ids = random.sample(all_song_ids, min(len(all_song_ids), limit))
        final_recs = await DB.db["songs"].find({"_id": {"$in": selected_ids}}).to_list(limit)
        return final_recs

    async def get_next_songs(self, user_id: str, current_song_id: str, limit: int = 10):
        """
        Gợi ý bài tiếp theo: Kết hợp Short-term (50/50) và Long-term (70/30).
        """       
        # Lấy vector bài hiện tại
        res = self.collection.query(expr=f"id == {current_song_id}", output_fields=["embedding"])
        if not res: return []
        v_current = np.array(res[0]["embedding"])

        # Lấy các vector thành phần từ Redis
        short_key = f"user:short:{user_id}"
        long_key = f"user:long:{user_id}"
        
        v_short_raw = await DB.redis.get(short_key)
        v_long_raw = await DB.redis.get(long_key)

        # Xử lý Short-term (EMA)
        v_short_old = np.array(json.loads(v_short_raw)) if v_short_raw else v_current
        v_short_new = 0.5 * v_current + 0.5 * v_short_old
        
        # Cập nhật lại bộ nhớ ngắn hạn vào Redis
        await DB.redis.setex(short_key, 1800, json.dumps(v_short_new.tolist()))

        # Xử lý Long-term
        if v_long_raw:
            v_long = np.array(json.loads(v_long_raw))
        else:
            user_data = await DB.db["users"].find_one({"user_id": user_id})
            if not user_data:
                v_long = np.zeros_like(v_current)
            v_long = np.array(user_data["latent_vector"]) if user_data else np.zeros_like(v_current)

        # Công thức tổng hợp 70% ngắn hạn + 30% dài hạn
        v_target = 0.7 * v_short_new + 0.3 * v_long

        # Search Milvus
        search_params = {"metric_type": "IP", "params": {"nprobe": 10}}
        results = self.collection.search(
            data=[v_target.tolist()],
            anns_field="embedding",
            param=search_params,
            limit=limit + 1,
            output_fields=["id"]
        )

        song_ids = [hit.id for hit in results[0] if hit.id != current_song_id][:limit]
        return await DB.db["songs"].find({"_id": {"$in": song_ids}}).to_list(limit)