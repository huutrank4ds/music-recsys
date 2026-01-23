import json
import random
import asyncio
import numpy as np
from typing import List, Any
from pymilvus import Collection, utility, MilvusException #type: ignore
from app.core.database import DB
from common.logger import get_logger

logger = get_logger("RecommendationService")
long_key_prefix = f"user:long:"
short_key_prefix = f"user:short:"
time_to_live_seconds = 3600 * 2  # 2 hours

class RecommendationService:
    def __init__(self, collection_name: str = "music_collection"):
        """
        Khởi tạo Service. 
        """
        self.collection_name = collection_name
        self._collection = None

    @property
    def collection(self) -> Collection:
        """
        Lazy Loading: Chỉ thực sự kết nối và load Collection khi có request đầu tiên gọi đến.
        """
        if self._collection is None:
            # Kiểm tra xem Collection có tồn tại trong Milvus chưa
            if not utility.has_collection(self.collection_name):
                # Bạn có thể raise lỗi hoặc tự động tạo collection nếu muốn
                raise ValueError(f"Critical Error: Collection '{self.collection_name}' chưa tồn tại trong Milvus!")
            
            # Kết nối
            self._collection = Collection(self.collection_name)
            
            # QUAN TRỌNG: Phải load vào RAM thì mới search nhanh được
            try:
                self._collection.load()
                logger.info(f"[Milvus] Tải collection '{self.collection_name}' vào bộ nhớ.")
            except MilvusException as e:
                logger.warning(f"[Milvus] Lỗi khi tải collection '{self.collection_name}' vào bộ nhớ: {e}")
                
        return self._collection

    async def _search_milvus(self, vector: List[float], top_k: int, exclude_id: Any = None) -> List[int]:
        """
        Hàm phụ trợ để chạy lệnh Search của Milvus trong Thread riêng (Non-blocking).
        """
        search_params = {"metric_type": "IP", "params": {"nprobe": 10}}
        
        # Chạy hàm sync của Milvus trong thread pool để không chặn FastAPI
        results = await asyncio.to_thread(
            self.collection.search,
            data=[vector],
            anns_field="embedding",
            param=search_params,
            limit=top_k,
            output_fields=["id"]
        )

        if not results:
            return []

        # Lấy danh sách ID (loại bỏ bài cần exclude nếu có)
        ids = [hit.id for hit in results[0] if str(hit.id) != str(exclude_id)]
        return ids
    
    async def cold_start_recs(self, limit: int = 20):
        """
        Gợi ý cho user mới chưa có lịch sử (Cold Start).
        Lấy ngẫu nhiên từ tập bài hát phổ biến.
        """
        popular_songs = await DB.db["songs"].find().sort("plays_7d", -1).limit(limit*2).to_list(length=limit*2)
        
        if not popular_songs:
            return []
        
        # Random sample từ top limit bài phổ biến
        selected_songs = random.sample(popular_songs, min(len(popular_songs), limit))
        return selected_songs

    async def get_personalized_recs(self, user_id: str, limit: int = 20):
        """
        Gợi ý trang chủ: Kết hợp Long-term (User Profile) + Short-term (Session).
        """
        # Lấy long-term vector (sở thích dài hạn)
        long_key = f"{long_key_prefix}{user_id}"
        v_long_raw = await DB.redis.get(long_key)
        
        if v_long_raw:
            v_long = np.array(json.loads(v_long_raw))
        else:
            # Nếu cache trong Redis không có, lấy từ MongoDB
            user_data = await DB.db["users"].find_one({"user_id": user_id})
            
            # Nếu user mới tinh chưa có vector -> Trả về bài hát ngẫu nhiên
            if not user_data or "latent_vector" not in user_data:
                logger.info(f"User mới tinh chưa có vector: {user_id}")
                return await self.cold_start_recs(limit)
            else:
                v_long = np.array(user_data["latent_vector"])
            # Cache lại vào Redis
            await DB.redis.setex(long_key, time_to_live_seconds, json.dumps(v_long.tolist()))

        # Lấy short-term vector (sở thích ngắn hạn trong session)
        short_key = f"{short_key_prefix}{user_id}"
        v_short_raw = await DB.redis.get(short_key)
        
        if v_short_raw:
            v_short = np.array(json.loads(v_short_raw))
            v_home = 0.6 * v_long + 0.4 * v_short
        else:
            v_home = v_long

        # Search trong Milvus với vector tổng hợp
        candidate_ids = await self._search_milvus(v_home.tolist(), top_k=limit * 2)
        if not candidate_ids:
            return []

        # Random sample để danh sách gợi ý mỗi lần F5 trông khác đi một chút
        selected_ids = random.sample(candidate_ids, min(len(candidate_ids), limit))
        
        # Lấy thông tin bài hát từ MongoDB
        final_recs = await DB.db["songs"].find({"_id": {"$in": selected_ids}}).to_list(limit)
        return final_recs

    async def get_next_songs(self, user_id: str, current_song_id: Any, limit: int = 10):
        """
        Gợi ý bài tiếp theo (Next Song): Dựa chủ yếu vào bài đang nghe + lịch sử vừa qua.
        """
        # Lấy vector của bài đang nghe
        expr = f"id == {current_song_id}" 
        res = await asyncio.to_thread(
            self.collection.query,
            expr=expr,
            output_fields=["embedding"]
        )
        
        if not res: 
            return await self.get_personalized_recs(user_id, limit)
            
        v_current = np.array(res[0]["embedding"])

        # Cập nhật Short-term vector
        short_key = f"{short_key_prefix}{user_id}"
        v_short_raw = await DB.redis.get(short_key)
        
        if v_short_raw:
            v_short_old = np.array(json.loads(v_short_raw))
        else:
            v_short_old = v_current # Nếu chưa có session, bài hiện tại là khởi đầu

        # Cập nhật: 50% vector bài mới + 50% lịch sử cũ
        v_short_new = 0.5 * v_current + 0.5 * v_short_old
        
        # Lưu lại vào Redis
        await DB.redis.setex(short_key, time_to_live_seconds/4, json.dumps(v_short_new.tolist()))

        # Tổng hợp với Long-term vector
        long_key = f"{long_key_prefix}{user_id}"
        v_long_raw = await DB.redis.get(long_key)
        
        if v_long_raw:
             v_long = np.array(json.loads(v_long_raw))
        else:
             v_long = np.zeros_like(v_current) # User mới thì coi như long-term bằng 0

        # Công thức Next Song: 70% Gu hiện tại + 30% Gu gốc
        v_target = 0.7 * v_short_new + 0.3 * v_long

        # Search trong Milvus với vector tổng hợp
        # Lấy limit + 1 vì chắc chắn sẽ lọc bỏ bài hiện tại
        candidate_ids = await self._search_milvus(
            v_target.tolist(), 
            top_k=limit + 1, 
            exclude_id=current_song_id
        )
        
        # Chỉ lấy đúng số lượng limit
        final_ids = candidate_ids[:limit]
        
        return await DB.db["songs"].find({"_id": {"$in": final_ids}}).to_list(limit)

# --- Singleton Instance ---
recommender = RecommendationService()