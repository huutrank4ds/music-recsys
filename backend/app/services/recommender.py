import json
import random
import asyncio
import numpy as np
from typing import List, Any, Optional
from pymilvus import Collection, utility, MilvusException #type: ignore
from app.core.database import DB
from common.logger import get_logger

logger = get_logger("RecommendationService")
long_key_prefix = f"user:long:"
short_key_prefix = f"user:short:"
time_to_live_seconds = 3600 * 2  # 2 hours

# Collection names
ALS_COLLECTION = "music_collection"  # ALS item embeddings (Collaborative Filtering)
LYRICS_COLLECTION = "lyrics_embeddings"  # Lyrics embeddings (Content-Based)


class RecommendationService:
    def __init__(self, als_collection: str = ALS_COLLECTION, lyrics_collection: str = LYRICS_COLLECTION):
        """
        Khởi tạo Service với 2 collections:
        - ALS Collection: Collaborative Filtering
        - Lyrics Collection: Content-Based Filtering
        """
        self.als_collection_name = als_collection
        self.lyrics_collection_name = lyrics_collection
        self._als_collection = None
        self._lyrics_collection = None

    @property
    def als_collection(self) -> Collection:
        """Lazy load ALS collection (Collaborative Filtering)"""
        if self._als_collection is None:
            if utility.has_collection(self.als_collection_name):
                self._als_collection = Collection(self.als_collection_name)
                try:
                    self._als_collection.load()
                    logger.info(f"[Milvus] Loaded ALS collection: {self.als_collection_name}")
                except MilvusException as e:
                    logger.warning(f"[Milvus] Error loading ALS collection: {e}")
            else:
                logger.warning(f"[Milvus] ALS collection '{self.als_collection_name}' not found!")
        return self._als_collection

    @property
    def lyrics_collection(self) -> Optional[Collection]:
        """Lazy load Lyrics collection (Content-Based Filtering)"""
        if self._lyrics_collection is None:
            if utility.has_collection(self.lyrics_collection_name):
                self._lyrics_collection = Collection(self.lyrics_collection_name)
                try:
                    self._lyrics_collection.load()
                    logger.info(f"[Milvus] Loaded Lyrics collection: {self.lyrics_collection_name}")
                except MilvusException as e:
                    logger.warning(f"[Milvus] Error loading Lyrics collection: {e}")
            else:
                logger.info(f"[Milvus] Lyrics collection '{self.lyrics_collection_name}' not available (Content-Based disabled)")
        return self._lyrics_collection

    # Keep backward compatibility
    @property
    def collection(self) -> Collection:
        return self.als_collection

    async def _search_milvus(self, collection: Collection, vector: List[float], top_k: int, exclude_id: Any = None) -> List[str]:
        """
        Search trong Milvus collection (async wrapper)
        """
        if collection is None:
            return []
            
        search_params = {"metric_type": "IP", "params": {"nprobe": 10}}
        
        results = await asyncio.to_thread(
            collection.search,
            data=[vector],
            anns_field="embedding",
            param=search_params,
            limit=top_k,
            output_fields=["id"]
        )

        if not results:
            return []

        ids = [str(hit.id) for hit in results[0] if str(hit.id) != str(exclude_id)]
        return ids

    async def _get_lyrics_similar(self, track_id: str, top_k: int = 10) -> List[str]:
        """
        Tìm bài hát tương tự dựa trên lyrics (Content-Based Filtering)
        """
        if self.lyrics_collection is None:
            return []
        
        try:
            # Get embedding của bài hiện tại
            result = await asyncio.to_thread(
                self.lyrics_collection.query,
                expr=f"id == '{track_id}'",
                output_fields=["embedding"]
            )
            
            if not result:
                return []
            
            current_embedding = result[0]["embedding"]
            
            # Search similar
            similar_ids = await self._search_milvus(
                self.lyrics_collection,
                current_embedding,
                top_k=top_k + 1,
                exclude_id=track_id
            )
            
            return similar_ids[:top_k]
            
        except Exception as e:
            logger.warning(f"Content-based search error: {e}")
            return []

    async def cold_start_recs(self, limit: int = 20):
        """
        Gợi ý cho user mới chưa có lịch sử (Cold Start).
        Lấy ngẫu nhiên từ tập bài hát phổ biến.
        """
        popular_songs = await DB.db["songs"].find().sort("listen_count", -1).limit(limit*2).to_list(length=limit*2)
        
        if not popular_songs:
            return []
        
        selected_songs = random.sample(popular_songs, min(len(popular_songs), limit))
        return selected_songs

    async def get_personalized_recs(self, user_id: str, limit: int = 20):
        """
        Gợi ý trang chủ: Kết hợp Long-term (User Profile) + Short-term (Session).
        Sử dụng Collaborative Filtering (ALS).
        """
        # Lấy long-term vector
        long_key = f"{long_key_prefix}{user_id}"
        v_long_raw = await DB.redis.get(long_key)
        
        if v_long_raw:
            v_long = np.array(json.loads(v_long_raw))
        else:
            user_data = await DB.db["users"].find_one({"user_id": user_id})
            
            if not user_data or "latent_vector" not in user_data:
                logger.info(f"User mới chưa có vector: {user_id}")
                return await self.cold_start_recs(limit)
            else:
                v_long = np.array(user_data["latent_vector"])
            await DB.redis.setex(long_key, time_to_live_seconds, json.dumps(v_long.tolist()))

        # Lấy short-term vector
        short_key = f"{short_key_prefix}{user_id}"
        v_short_raw = await DB.redis.get(short_key)
        
        if v_short_raw:
            v_short = np.array(json.loads(v_short_raw))
            v_home = 0.6 * v_long + 0.4 * v_short
        else:
            v_home = v_long

        # Search trong ALS collection
        candidate_ids = await self._search_milvus(self.als_collection, v_home.tolist(), top_k=limit * 2)
        if not candidate_ids:
            return []

        selected_ids = random.sample(candidate_ids, min(len(candidate_ids), limit))
        
        final_recs = await DB.db["songs"].find({"_id": {"$in": selected_ids}}).to_list(limit)
        return final_recs

    async def get_next_songs(self, user_id: str, current_song_id: Any, limit: int = 10):
        """
        🎯 HYBRID RECOMMENDATION: Gợi ý bài tiếp theo
        
        Kết hợp:
        - 60% Collaborative Filtering (ALS): Người dùng tương tự thích gì
        - 40% Content-Based (Lyrics): Bài có nội dung tương tự
        """
        # ========================================
        # PHẦN 1: COLLABORATIVE FILTERING (ALS)
        # ========================================
        als_candidates = []
        
        try:
            # Lấy vector của bài đang nghe từ ALS collection
            expr = f"id == {current_song_id}" if str(current_song_id).isdigit() else f"id == '{current_song_id}'"
            res = await asyncio.to_thread(
                self.als_collection.query,
                expr=expr,
                output_fields=["embedding"]
            )
            
            if res:
                v_current = np.array(res[0]["embedding"])
                
                # Cập nhật Short-term vector
                short_key = f"{short_key_prefix}{user_id}"
                v_short_raw = await DB.redis.get(short_key)
                
                if v_short_raw:
                    v_short_old = np.array(json.loads(v_short_raw))
                else:
                    v_short_old = v_current

                v_short_new = 0.5 * v_current + 0.5 * v_short_old
                await DB.redis.setex(short_key, time_to_live_seconds/4, json.dumps(v_short_new.tolist()))

                # Lấy Long-term vector
                long_key = f"{long_key_prefix}{user_id}"
                v_long_raw = await DB.redis.get(long_key)
                
                if v_long_raw:
                    v_long = np.array(json.loads(v_long_raw))
                else:
                    v_long = np.zeros_like(v_current)

                # Công thức: 70% Gu hiện tại + 30% Gu gốc
                v_target = 0.7 * v_short_new + 0.3 * v_long

                # Search ALS candidates
                als_candidates = await self._search_milvus(
                    self.als_collection,
                    v_target.tolist(),
                    top_k=limit + 5,
                    exclude_id=current_song_id
                )
        except Exception as e:
            logger.warning(f"ALS search error: {e}")

        # ========================================
        # PHẦN 2: CONTENT-BASED (LYRICS)
        # ========================================
        content_candidates = await self._get_lyrics_similar(str(current_song_id), top_k=limit + 5)

        # ========================================
        # PHẦN 3: HYBRID - KẾT HỢP 2 NGUỒN
        # ========================================
        if als_candidates and content_candidates:
            # Hybrid: 60% ALS + 40% Content-Based
            als_count = int(limit * 0.6)
            content_count = limit - als_count
            
            final_ids = als_candidates[:als_count] + content_candidates[:content_count]
            # Shuffle để không bị phân biệt rõ ràng
            random.shuffle(final_ids)
            # Loại trùng
            final_ids = list(dict.fromkeys(final_ids))[:limit]
            
            logger.info(f"Hybrid Recs: {als_count} ALS + {content_count} Content-Based")
            
        elif als_candidates:
            # Fallback to ALS only
            final_ids = als_candidates[:limit]
            logger.info(f"ALS-only Recs (Content-Based unavailable)")
            
        elif content_candidates:
            # Fallback to Content-Based only
            final_ids = content_candidates[:limit]
            logger.info(f"Content-Based only Recs (ALS unavailable)")
            
        else:
            # Fallback to personalized recs
            return await self.get_personalized_recs(user_id, limit)

        # Lấy thông tin bài hát từ MongoDB
        return await DB.db["songs"].find({"_id": {"$in": final_ids}}).to_list(limit)

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