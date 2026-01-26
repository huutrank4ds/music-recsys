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

# Constants
LONG_KEY_PREFIX = "user:long:"
SHORT_KEY_PREFIX = "user:short:"
KEY_TTL = 3600 * 4  # 4 hours

FEED_KEY_PREFIX = "user:feed:"
SESSION_KEY_PREFIX = "user:session:"
SESSION_TTL = 3600  # 1 hour

PLAYLIST_KEY_PREFIX = "user:playlist:"
PLAYLIST_SESSION_KEY_PREFIX = "user:playlist_session:"
PLAYLIST_TTL = 3600  # 1 hour

# Collection names
ALS_COLLECTION = "music_collection"
CONTENT_COLLECTION = "lyrics_embeddings"

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
        if self._als_collection is not None: return self._als_collection
        if not self._milvus_available: return None
        try:
            if not utility.has_collection(self.als_collection_name):
                logger.warning(f"[Milvus] ALS Collection not found. Fallback.")
                self._milvus_available = False; return None
            self._als_collection = Collection(self.als_collection_name)
            self._als_collection.load()
            return self._als_collection
        except Exception as e:
            logger.error(f"[Milvus] Connection failed: {e}")
            self._milvus_available = False; return None

    @property
    def content_collection(self) -> Collection | None:
        if self._content_collection is not None: return self._content_collection
        if not self._milvus_available: return None
        try:
            if not utility.has_collection(self.content_collection_name):
                self._milvus_available = False; return None
            self._content_collection = Collection(self.content_collection_name)
            self._content_collection.load()
            return self._content_collection
        except Exception:
            self._milvus_available = False; return None

    def init_milvus_collection(self):
        try:
            if not utility.has_collection(self.als_collection_name):
                logger.info(f"Creating {self.als_collection_name}")
                schema = get_milvus_song_embedding_schema()
                self._als_collection = Collection(self.als_collection_name, schema)
                self._als_collection.create_index(field_name="embedding", index_params=index_params_milvus())
            else:
                self._als_collection = Collection(self.als_collection_name)
                self._als_collection.load()

            if not utility.has_collection(self.content_collection_name):
                logger.info(f"Creating {self.content_collection_name}")
                schema = get_milvus_content_embedding_schema()
                self._content_collection = Collection(self.content_collection_name, schema)
                self._content_collection.create_index(field_name="embedding", index_params=index_params_milvus())
            else:
                self._content_collection = Collection(self.content_collection_name)
                self._content_collection.load()
        except Exception as e:
            logger.error(f"Milvus Init Failed: {e}")

    async def _get_long_vector(self, user_id: str) -> Optional[np.ndarray]:
        long_key = f"{LONG_KEY_PREFIX}{user_id}"
        v_long_raw = await DB.redis.get(long_key)
        if v_long_raw:
            v_long = np.array(json.loads(v_long_raw))
            return v_long
        else:
            user_data = await DB.db["users"].find_one({"_id": user_id})
            if not user_data or "latent_vector" not in user_data:
                return None
            v_long = np.array(user_data["latent_vector"])
            await DB.redis.setex(long_key, int(KEY_TTL), json.dumps(v_long.tolist()))
            return v_long
        
    async def _get_short_vector(self, user_id: str, default: Optional[np.ndarray] = None) -> Optional[np.ndarray]:
        short_key = f"{SHORT_KEY_PREFIX}{user_id}"
        v_short_raw = await DB.redis.get(short_key)
        if v_short_raw:
            v_short = np.array(json.loads(v_short_raw))
            return v_short
        else:
            return default
        
    async def _get_embedding_vector(self, collection: Collection, song_id: str) -> Optional[np.ndarray]:
        if collection is None: return None
        try:
            query_result = await asyncio.to_thread(
                collection.query,
                expr=f"id == '{song_id}'",
                output_fields=["embedding"]
            )
            if not query_result:
                return None
            embedding = np.array(query_result[0]["embedding"])
            return embedding
        except Exception as e:
            logger.error(f"[Milvus] Lỗi lấy embedding cho bài hát {song_id}: {e}")
            return None

    # Hàm tìm kiếm chung
    async def _search_milvus(self, collection: Collection, vector: List[float], top_k: int, exclude_ids: Optional[List[Any]] = None) -> dict:
        if collection is None: return {}
        
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
            if not results: return {}
            score_map = {hit.id: hit.distance for hit in results[0]}
            return score_map
        except Exception as e:
            logger.error(f"[Milvus] Lỗi tìm kiếm theo ALS: {e}")
            return {}

    async def _get_lyrics_similar(self, vector: List[float], top_k: int = 10, exclude_ids: Optional[List[Any]] = None) -> dict:
        if self.content_collection is None: return {}

        expr = None
        if exclude_ids and len(exclude_ids) > 0:
            expr = f"id not in {exclude_ids}"
        try:
            results = await asyncio.to_thread(
                self.content_collection.search,
                data=[vector],
                anns_field="embedding",
                param=self.search_params,
                limit=top_k,
                expr=expr,
                output_fields=["id"]
            )
            if not results: return {}
            score_map = {hit.id: hit.distance for hit in results[0]}
            return score_map
        except Exception as e:
            logger.error(f"[Milvus] Lỗi tìm kiếm theo lyrics: {e}")
            return {}
        

    async def get_fallback_songs_ids(self, limit: int = 20, exclude_ids: Optional[List[Any]] = None) -> List[str]:
        """
        SYSTEM FALLBACK: Lấy ngẫu nhiên IDs từ nhóm Top Trending.
        Trả về: List[str] (Danh sách các ID)
        """
        query = {}
        if exclude_ids:
            query["_id"] = {"$nin": exclude_ids}
            
        pool_limit = int(limit * 2)
        cursor = DB.db["songs"].find(query, {"_id": 1}).sort("plays_7d", -1).limit(pool_limit)
        candidates = await cursor.to_list(length=pool_limit)
        if not candidates:
            return []
        if len(candidates) > limit:
            selected_docs = random.sample(candidates, limit)
        else:
            random.shuffle(candidates)
            selected_docs = candidates

        return [doc["_id"] for doc in selected_docs]

    # Hàm tạo các id bài hát gợi ý cá nhân hóa
    async def _generate_personalized_ids(self, user_id: str, limit: int, exclude_ids: Optional[List[Any]] = None) -> List[str]:
        if self.als_collection is None: return []
        try:
            v_long = await self._get_long_vector(user_id)
            if v_long is None:  
                return []
            v_short = await self._get_short_vector(user_id, default=v_long)
            if v_short is None:
                v_home = v_long
            else:
                v_home = 0.7 * v_long + 0.3 * v_short
            pool_limit = limit * 2
            pool_score_map = await self._search_milvus(self.als_collection, v_home.tolist(), top_k=pool_limit, exclude_ids=exclude_ids)
            pool_ids = list(pool_score_map.keys())
            if not pool_ids:
                return []
            if len(pool_ids) > limit:
                final_ids = random.sample(pool_ids, limit)
            else:
                final_ids = pool_ids
            return final_ids
        
        except Exception as e:
            logger.error(f"Gen ID Error: {e}")
            return []

    # Lấy gợi ý cá nhân hóa cho trang chủ
    async def get_home_feed(self, user_id: str, limit: int = 20, refresh: bool = False) -> dict:
        feed_key = f"{FEED_KEY_PREFIX}{user_id}"
        session_key = f"{SESSION_KEY_PREFIX}{user_id}"
        pool_limit = int(limit * 3)
        has_more = True

        if refresh:
            await DB.redis.delete(feed_key, session_key)

        current_len = await DB.redis.llen(feed_key)
        if current_len < limit:
            seen_ids_bytes = await DB.redis.smembers(session_key)
            seen_ids = [x.decode('utf-8') for x in seen_ids_bytes] if seen_ids_bytes else []

            new_batch_ids = await self._generate_personalized_ids(
                user_id, 
                limit=pool_limit, 
                exclude_ids=seen_ids
            )
            if not new_batch_ids:
                new_batch_ids = await self.get_fallback_songs_ids(limit=pool_limit, exclude_ids=seen_ids)
            if not new_batch_ids:
                has_more = False
            else:
                await DB.redis.rpush(feed_key, *new_batch_ids)
                await DB.redis.expire(feed_key, int(KEY_TTL))
                
                await DB.redis.sadd(session_key, *new_batch_ids)
                await DB.redis.expire(session_key, int(SESSION_TTL))

        # Lấy ID từ Redis
        song_ids_bytes = await DB.redis.lpop(feed_key, count=limit)
        song_ids = [x.decode('utf-8') for x in song_ids_bytes]
        remaining = await DB.redis.llen(feed_key)
        has_more = True if remaining > 0 else (len(song_ids) >= limit)

        # Không cần sắp xếp ở trang chủ
        songs = await DB.db["songs"].find({"_id": {"$in": song_ids}}).to_list(length=limit)
        return {
            "songs": songs,
            "has_more": has_more
        }
        
    def _merge_score_maps(self, als_scores: dict, content_scores: dict, limit: int, weight_als: float = 0.5) -> List[str]:
        """
        Trộn điểm và trả về danh sách ID đã sắp xếp từ cao xuống thấp.
        """
        merged_map = {}
        all_ids = set(als_scores.keys()) | set(content_scores.keys())

        for key in all_ids:
            # Lấy điểm (mặc định là 0 nếu không tồn tại)
            s_als = als_scores.get(key, 0.0)
            s_content = content_scores.get(key, 0.0)

            weight_content = 1.0 - weight_als
            merged_score = (s_als * weight_als) + (s_content * weight_content)
            merged_map[key] = merged_score
        
        # Sắp xếp theo điểm từ cao xuống thấp
        sorted_items = sorted(merged_map.items(), key=lambda item: item[1], reverse=True)
        sorted_ids = [item[0] for item in sorted_items][:limit]
        return sorted_ids

    # Get next songs based on current song
    async def get_next_songs(self, user_id: str, current_song_id: str, limit: int = 10, refresh: bool = False) -> dict:
        playlist_key = f"{PLAYLIST_KEY_PREFIX}{user_id}"
        session_key = f"{PLAYLIST_SESSION_KEY_PREFIX}{user_id}"
        pool_limit = int(limit * 3)

        if refresh:
            await DB.redis.delete(playlist_key, session_key)

        current_len = await DB.redis.llen(playlist_key)
        if current_len < limit:
            seen_ids_bytes = await DB.redis.smembers(session_key)
            seen_ids = [x.decode('utf-8') for x in seen_ids_bytes] if seen_ids_bytes else []
            search_exclude_ids = list(seen_ids)
            if current_song_id:
                search_exclude_ids.append(current_song_id)
            
            v_long = await self._get_long_vector(user_id)
            if v_long is None:
                als_score_map = {}
            else:
                als_score_map = await self._search_milvus(
                    self.als_collection,
                    vector=v_long.tolist(),
                    top_k=pool_limit,
                    exclude_ids=search_exclude_ids 
                )
            
            current_song_embedding = await self._get_embedding_vector(self.content_collection, current_song_id)
            if current_song_embedding is None:
                content_score_map = {}
            else:
                v_short = await self._get_short_vector(user_id, default=current_song_embedding)
                try:
                    if v_short is not None and v_short.shape == current_song_embedding.shape:
                        v_session = 0.5 * v_short + 0.5 * current_song_embedding
                    else:
                        v_session = current_song_embedding
                except Exception:
                    v_session = current_song_embedding

                content_score_map = await self._get_lyrics_similar(
                    vector=v_session.tolist(),
                    top_k=pool_limit,
                    exclude_ids=search_exclude_ids
                )

            merged_ids = self._merge_score_maps(als_score_map, content_score_map, limit=pool_limit, weight_als=0.3)
            if not merged_ids:
                merged_ids = await self.get_fallback_songs_ids(limit=pool_limit, exclude_ids=search_exclude_ids)
            if not merged_ids:
                has_more = False
            else:
                await DB.redis.rpush(playlist_key, *merged_ids)
                await DB.redis.expire(playlist_key, int(PLAYLIST_TTL))
                await DB.redis.sadd(session_key, *merged_ids)
                await DB.redis.expire(session_key, int(PLAYLIST_TTL))

        song_ids_bytes = await DB.redis.lpop(playlist_key, count=limit)
        
        if not song_ids_bytes:
            return {"songs": [], "has_more": False}
        song_ids = [x.decode('utf-8') for x in song_ids_bytes]
        remaining = await DB.redis.llen(playlist_key)
        has_more = True if remaining > 0 else (len(song_ids) >= limit)

        docs = await DB.db["songs"].find({"_id": {"$in": song_ids}}).to_list(length=limit)
        docs_map = {d["_id"]: d for d in docs}
        songs = [docs_map[sid] for sid in song_ids if sid in docs_map]
        return {
            "next_songs": songs,
            "has_more": has_more
        }
            

# Singleton
recommender = RecommendationService()