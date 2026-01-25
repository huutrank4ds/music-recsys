import json
import random
import asyncio
import numpy as np
from typing import List, Any, Optional
from app.core.database import DB
from common.logger import get_logger
from common.milvus_schemas import get_milvus_song_embedding_schema, index_params_milvus, search_params_milvus, get_milvus_content_embedding_schema
from pymilvus import Collection, utility #type: ignore

logger = get_logger("RecommendationService")
long_key_prefix = f"user:long:"
short_key_prefix = f"user:short:"
time_to_live_seconds = 3600 * 2  # 2 hours

# Collection names
ALS_COLLECTION = "music_collection"  # ALS item embeddings (Collaborative Filtering)
CONTENT_COLLECTION = "lyrics_embeddings"  # Lyrics embeddings (Content-Based)


class RecommendationService:
    def __init__(self, als_collection_name: str = ALS_COLLECTION, content_collection_name: str = CONTENT_COLLECTION):
        self.als_collection_name = als_collection_name
        self.content_collection_name = content_collection_name
        self._als_collection = None
        self._content_collection = None
        
        self.search_params = search_params_milvus()
        self._milvus_available = True 
        self.init_milvus_collection()
        

    @property
    def als_collection(self) -> Collection | None:
        """
        PROPERTY: Được gọi mỗi khi cần search.
        Logic:
        1. Nếu đã có (từ hàm init) -> Trả về ngay (Siêu nhanh).
        2. Nếu chưa có (do init lỗi) -> Thử kết nối lại (Lazy Load / Retry).
        3. Nếu thử lại vẫn lỗi -> Trả về None (Kích hoạt Fallback Mongo).
        """
        if self._als_collection is not None: # Đã init thành công
            return self._als_collection
        if not self._milvus_available: # Init thất bại trước đó
            return None
        try:
            # Kiểm tra nhẹ
            if not utility.has_collection(self.als_collection_name):
                logger.warning(f"[Milvus Runtime] Collection chưa tồn tại. Dùng Fallback.")
                self._milvus_available = False 
                return None
            
            self._als_collection = Collection(self.als_collection_name)
            self._als_collection.load()
            logger.info(f"[Milvus Runtime] Đã kết nối lại thành công!")
            return self._als_collection

        except Exception as e:
            logger.error(f"[Milvus Runtime] Vẫn không kết nối được: {e}")
            self._milvus_available = False
            return None
        
    @property
    def content_collection(self) -> Collection | None:
        """
        PROPERTY: Collection lyrics embeddings.
        Logic tương tự als_collection.
        """
        if self._content_collection is not None:
            return self._content_collection
        if not self._milvus_available:
            return None
        try:
            if not utility.has_collection(self.content_collection_name):
                logger.warning(f"[Milvus Runtime] Lyrics Collection chưa tồn tại. Dùng Fallback.")
                self._milvus_available = False 
                return None
            
            self._content_collection = Collection(self.content_collection_name)
            self._content_collection.load()
            logger.info(f"[Milvus Runtime] Đã kết nối lại Lyrics Collection thành công!")
            return self._content_collection
        except Exception as e:
            logger.error(f"[Milvus Runtime] Vẫn không kết nối được Lyrics Collection: {e}")
            self._milvus_available = False
            return None
    
    def init_milvus_collection(self):
        """
        Hàm này được gọi 1 lần duy nhất khi Server khởi động.
        Nhiệm vụ: Đảm bảo Collection và Index luôn tồn tại.
        """
        try:
            if not utility.has_collection(self.als_collection_name):
                logger.info(f"Creating new collection: {self.als_collection_name}")
                schema = get_milvus_song_embedding_schema()
                self._als_collection = Collection(self.als_collection_name, schema)
                index_params = index_params_milvus()
                self._als_collection.create_index(field_name="embedding", index_params=index_params)
                logger.info("Index created successfully.")
            
            else:
                self._als_collection = Collection(self.als_collection_name)
            self._als_collection.load() # Load vào memory
            logger.info(f"Collection '{self.als_collection_name}' loaded into memory.")

            if not utility.has_collection(self.content_collection_name):
                logger.info(f"Creating new collection: {self.content_collection_name}")
                schema = get_milvus_content_embedding_schema()
                self._content_collection = Collection(self.content_collection_name, schema)
                index_params = index_params_milvus()
                self._content_collection.create_index(field_name="embedding", index_params=index_params)
                logger.info("Content Index created successfully.")
            else:
                self._content_collection = Collection(self.content_collection_name)
            self._content_collection.load() # Load vào memory
            logger.info(f"Collection '{self.content_collection_name}' loaded into memory.")

        except Exception as e:
            logger.error(f"Failed to initialize Milvus: {e}")

    async def _search_milvus(self, collection: Collection, vector: List[float], top_k: int, exclude_ids: Optional[List[Any]] = None) -> List[int]:
        """
        Hàm search an toàn. Nếu Milvus chưa sẵn sàng, trả về list rỗng.
        """
        # Nếu collection bị None, trả về rỗng ngay
        if collection is None:
            return []
        
        expr = None
        if exclude_ids and len(exclude_ids) > 0:
            expr = f"id not in {exclude_ids}"
        
        try:
            results = await asyncio.to_thread(
                collection.search,
                data=[vector],
                anns_field="embedding",
                param=self.search_params,
                limit=top_k,
                expr=expr,
                output_fields=["id"]
            )

            if not results:
                return []

            ids = [hit.id for hit in results[0]]
            return ids
            
        except Exception as e:
            logger.error(f"[Milvus] Lỗi khi tìm kiếm: {e}")
            return []
        
    async def _get_lyrics_similar(self, track_id: str, top_k: int = 10, exclude_ids: Optional[str] = None) -> List[int]:
        """
        Tìm bài hát tương tự dựa trên lyrics (Content-Based Filtering)
        """
        if self.content_collection is None:
            return []
        
        try:
            # Get embedding của bài hiện tại
            result = await asyncio.to_thread(
                self.content_collection.query,
                expr=f"id == '{track_id}'",
                output_fields=["embedding"]
            )
            
            if not result:
                return []
            
            current_embedding = result[0]["embedding"]
            
            # Search similar
            similar_ids = await self._search_milvus(
                self.content_collection,
                current_embedding,
                top_k=top_k + 1,
                exclude_ids=[track_id]
            )
            
            return similar_ids[:top_k]  
            
        except Exception as e:
            logger.warning(f"Content-based search error: {e}")
            return []


    async def get_fallback_songs(self, limit: int = 20, exclude_ids: Optional[List[Any]] = None) -> List[dict]:
        """
        SYSTEM FALLBACK: Khi Milvus hỏng hoặc chưa có dữ liệu.
        Lấy bài hát có lượt nghe cao nhất trong 7 ngày qua (plays_7d).
        """
        query = {}
        if exclude_ids and len(exclude_ids) > 0:
            query["_id"] = {"$nin": exclude_ids}
            
        # Lấy top thịnh hành từ MongoDB
        cursor = DB.db["songs"].find(query).sort("plays_7d", -1).limit(limit)
        return await cursor.to_list(length=limit)

    async def get_personalized_recs(self, user_id: str, limit: int = 20, exclude_ids: Optional[List[Any]] = None):
        """
        Gợi ý trang chủ.
        """
        # Nếu Milvus không khả dụng, dùng fallback
        if self.als_collection is None:
            return await self.get_fallback_songs(limit, exclude_ids=exclude_ids)

        try:
            # Lấy vector Long term từ Redis / MongoDB
            long_key = f"{long_key_prefix}{user_id}"
            v_long_raw = await DB.redis.get(long_key)
            
            if v_long_raw:
                # Lấy từ Redis
                v_long = np.array(json.loads(v_long_raw)) 
            else:
                # Lấy từ MongoDB
                user_data = await DB.db["users"].find_one({"_id": user_id})
                if not user_data or "latent_vector" not in user_data:
                    logger.info(f"User {user_id}: No latent vector found. Using Fallback.")
                    return await self.get_fallback_songs(limit, exclude_ids=exclude_ids)
                else:
                    v_long = np.array(user_data["latent_vector"])
                # Lưu lại vào Redis để lần sau nhanh hơn
                await DB.redis.setex(long_key, time_to_live_seconds, json.dumps(v_long.tolist()))

            short_key = f"{short_key_prefix}{user_id}"
            v_short_raw = await DB.redis.get(short_key)
            
            if v_short_raw:
                v_short = np.array(json.loads(v_short_raw))
                v_home = 0.6 * v_long + 0.4 * v_short
            else:
                v_home = v_long

            # Tìm kiếm trong Milvus
            candidate_ids = await self._search_milvus(self.als_collection, v_home.tolist(), top_k=limit * 2, exclude_ids=exclude_ids)
            
            # Nếu kết quả rỗng, dùng fallback
            if not candidate_ids:
                logger.warning(f"User {user_id}: Milvus search returned empty. Using Fallback.")
                return await self.get_fallback_songs(limit, exclude_ids=exclude_ids)

            selected_ids = random.sample(candidate_ids, min(len(candidate_ids), limit))
            final_recs = await DB.db["songs"].find({"_id": {"$in": selected_ids}}).to_list(limit)
            
            # Nếu MongoDB không tìm thấy đủ bài, bù thêm từ fallback
            if len(final_recs) < limit:
                needed = limit - len(final_recs)
                extras = await self.get_fallback_songs(
                    needed, 
                    exclude_ids=(exclude_ids or []) + selected_ids
                )
                final_recs.extend(extras)

            return final_recs
            
        except Exception as e:
            logger.error(f"Error in Personal Recs: {e}")
            return await self.get_fallback_songs(limit, exclude_ids=exclude_ids)

    async def get_next_songs(self, user_id: str, current_song_id: Any, limit: int = 10, exclude_ids: Optional[List[Any]] = None):
        """
        Gợi ý bài tiếp theo.
        """
        if not exclude_ids:
            exclude_ids = [current_song_id]
        else:
            exclude_ids = exclude_ids + [current_song_id]
        # Nếu Milvus không khả dụng, dùng fallback
        if self.als_collection is None:
            return await self.get_fallback_songs(limit, exclude_ids=exclude_ids)

        try:
            # Lấy vector bài hiện tại từ Milvus
            expr = f"id == {current_song_id}" 
            # Dùng asyncio.to_thread để tránh block nếu Milvus query lâu
            res = await asyncio.to_thread(
                self.als_collection.query,
                expr=expr,
                output_fields=["embedding"]
            )
            
            # Nếu bài này chưa có vector trong Milvus, dùng fallback
            if not res: 
                logger.info(f"Song {current_song_id} vector not found. Using Fallback.")
                return await self.get_fallback_songs(limit, exclude_ids=exclude_ids)
                
            v_current = np.array(res[0]["embedding"])

            # Cập nhật vector Short-term
            short_key = f"{short_key_prefix}{user_id}"
            v_short_raw = await DB.redis.get(short_key)
            
            if v_short_raw:
                v_short_old = np.array(json.loads(v_short_raw))
            else:
                v_short_old = v_current

            v_short_new = 0.5 * v_current + 0.5 * v_short_old
            await DB.redis.setex(short_key, time_to_live_seconds/4, json.dumps(v_short_new.tolist()))

            long_key = f"{long_key_prefix}{user_id}"
            v_long_raw = await DB.redis.get(long_key)
            
            if v_long_raw:
                 v_long = np.array(json.loads(v_long_raw))
            else:
                 v_long = np.zeros_like(v_current)

            v_target = 0.7 * v_short_new + 0.3 * v_long

            # Tìm bài tương tự
            candidate_ids = await self._search_milvus(
                self.als_collection,
                v_target.tolist(), 
                top_k=limit + 1, 
                exclude_ids=exclude_ids
            )
            
            # Nếu không tìm thấy ai giống -> Fallback
            if not candidate_ids:
                return await self.get_fallback_songs(limit, exclude_ids=exclude_ids)
            
            final_ids = candidate_ids[:limit]
            
            # Lấy chi tiết bài hát từ MongoDB
            songs = await DB.db["songs"].find({"_id": {"$in": final_ids}}).to_list(limit)
            
            # Safety check: Nếu số lượng bài trả về quá ít, bù thêm bài Trending
            if len(songs) < limit:
                needed = limit - len(songs)
                extras = await self.get_fallback_songs(needed, exclude_ids=exclude_ids)
                songs.extend(extras)
                
            return songs

        except Exception as e:
            logger.error(f"Error in Next Song: {e}")
            # Lưới an toàn cuối cùng
            return await self.get_fallback_songs(limit, exclude_ids=exclude_ids)

    async def get_content_based_recs(self, track_id: str, limit: int = 10):
        """
        Pure Content-Based Recommendation.
        Tìm bài có lyrics tương tự nhất.
        """
        similar_ids = await self._get_lyrics_similar(track_id, top_k=limit)
        
        if not similar_ids:
            return []
        
        return await DB.db["songs"].find({"_id": {"$in": similar_ids}}).to_list(limit)


# --- Singleton Instance ---
recommender = RecommendationService()