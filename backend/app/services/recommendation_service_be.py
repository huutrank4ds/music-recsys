import json
import random
import asyncio
import numpy as np
from typing import List, Any, Optional, Tuple

from app.core.database import DB
from common.logger import get_logger
from common.schemas.milvus_schemas import get_milvus_song_embedding_schema, index_params_milvus, search_params_milvus, get_milvus_content_embedding_schema
from pymilvus import Collection, utility #type: ignore
import config as cfg
from app.core.database import DB
import heapq
from common.constants import SONG_SUMMARY_PROJECTION

NAME_TASK = "Recommendation Service"
logger = get_logger(NAME_TASK)

class RecommendationService:
    def __init__(self, als_collection_name: str = cfg.ALS_MILVUS_COLLECTION, content_collection_name: str = cfg.CONTENT_MILVUS_COLLECTION):
        self.als_collection_name = als_collection_name
        self.content_collection_name = content_collection_name
        self._als_collection = None
        self._content_collection = None
        
        self.search_params = search_params_milvus()
        self._milvus_available = True 

    @property
    def als_collection(self) -> Collection | None:
        return self._als_collection

    @property
    def content_collection(self) -> Collection | None:
        return self._content_collection
        
    async def start(self, retries: int = 5, sleep_seconds: int = 5):
        for attempt in range(1, retries + 1):
            try:
                logger.info(f"Khởi động Milvus (Lần thử {attempt}/{retries})...")
                DB.connect_to_milvus()
                self.ensure_collections_ready()
                self._milvus_available = True
                logger.info("Milvus khởi động thành công!")
                return
            except Exception as e:
                logger.error(f"Lỗi khởi động Milvus: {e}")
                if attempt == retries:
                    logger.critical("Không thể khởi động Milvus sau nhiều lần thử.")
                    self._milvus_available = False
                    raise e
                logger.info(f"Chờ {sleep_seconds} giây trước khi thử lại...")
                await asyncio.sleep(sleep_seconds)

    def ensure_collections_ready(self):
        """
        Đảm bảo cả 2 collection (ALS & Content) đều tồn tại và được load lên RAM.
        """
        # Xử lý ALS Collection
        self._als_collection = self._setup_one_collection(
            name=self.als_collection_name,
            schema_func=get_milvus_song_embedding_schema
        )

        # Xử lý Content Collection
        self._content_collection = self._setup_one_collection(
            name=self.content_collection_name,
            schema_func=get_milvus_content_embedding_schema
        )

    def _setup_one_collection(self, name: str, schema_func) -> Collection:
        """Helper function: Tạo/Load 1 collection cụ thể"""
        if not utility.has_collection(name):
            logger.info(f"Đang tạo mới Collection: {name}")
            schema = schema_func()
            col = Collection(name, schema)
            # Tạo Index ngay sau khi tạo collection
            col.create_index(field_name="embedding", index_params=index_params_milvus())
            logger.info(f"Đã tạo xong: {name}")
        else:
            col = Collection(name)
            logger.info(f"Tìm thấy Collection cũ: {name}")

        # Luôn luôn load lên RAM
        col.load()
        logger.info(f"Đã Load lên RAM: {name}")
        return col

    # Các hàm trợ giúp để lấy vector từ Redis / DB
    async def _get_long_vector(self, user_id: str) -> Optional[np.ndarray]:
        long_key = f"{cfg.LONG_KEY_PREFIX}{user_id}"
        v_long_bytes = await DB.redis.get(long_key)
        if v_long_bytes:
            v_long = np.frombuffer(v_long_bytes, dtype=np.float32)
        else:
            user_data = await DB.db["users"].find_one({"_id": user_id})
            if not user_data or "latent_vector" not in user_data:
                return None
            v_long = np.array(user_data["latent_vector"], dtype=np.float32)
            await DB.redis.setex(long_key, int(cfg.KEY_VECTOR_TTL), v_long.tobytes())
        return v_long
        
    async def _get_short_vector(self, user_id: str, default: Optional[np.ndarray] = None) -> Optional[np.ndarray]:
        short_key = f"{cfg.SHORT_KEY_PREFIX}{user_id}"
        v_short_bytes = await DB.redis.get(short_key)
        if v_short_bytes:
            v_short = np.frombuffer(v_short_bytes, dtype=np.float32)
            return v_short
        else:
            return default
        
    async def _get_session_vector(self, user_id: str, weightht_long: float = 0.7) -> Tuple:
        
        # Khởi tạo giá trị mặc định
        v_long = None
        v_short = None
        vector_to_search = None

        # Nếu chưa kết nối DB thì trả về 3 cái None luôn
        if self.als_collection is None:
            return v_long, v_short, vector_to_search

        try:
            # Lấy vector thành phần
            v_long = await self._get_long_vector(user_id)
            v_short = await self._get_short_vector(user_id, default=None) 

            # Logic Trọng số (Vector Weighting Strategy)
            if v_long is not None and v_short is not None:
                if v_long.shape == v_short.shape:
                    vector_to_search = weightht_long * v_long + (1 - weightht_long) * v_short
                    logger.info(f"User {user_id}: Gợi ý kết hợp Long ({weightht_long}) kết hợp Short.")
                else:
                    # Nếu lệch shape (hiếm gặp), ưu tiên Long
                    vector_to_search = v_long
                    logger.warning(f"User {user_id}: Vector không cùng kích thước.")

            elif v_long is not None:
                vector_to_search = v_long
                logger.info(f"User {user_id}: Gợi ý Long Term.")

            elif v_short is not None:
                vector_to_search = v_short
                logger.info(f"User {user_id}: Gợi ý Short Term.")
            
            else:
                vector_to_search = None
                logger.info(f"User {user_id}: Cold Start (Không có vector).")

        except Exception as e:
            logger.error(f"Lỗi tính toán Vector cho user {user_id}: {e}")
            vector_to_search = None

        return v_long, v_short, vector_to_search
        
    async def _get_embedding_vector(self, collection: Collection, song_id: str, key_prefix: str, ttl: int) -> Optional[np.ndarray]:
        if collection is None: return None
        # Tạo key dựa trên prefix được truyền vào
        cache_key = f"{key_prefix}{song_id}" 
        # Tìm trong Redis trước
        try:
            cached_bytes = await DB.redis.get(cache_key)
            if cached_bytes:
                return np.frombuffer(cached_bytes, dtype=np.float32)
        except Exception as e:
            logger.warning(f"Redis Error: {e}")

        # Nếu không có trong Redis, truy vấn Milvus
        try:
            query_result = await asyncio.to_thread(
                collection.query,
                expr=f"id == '{song_id}'",
                output_fields=["embedding"]
            )
            
            if not query_result: return None
            embedding = np.array(query_result[0]["embedding"], dtype=np.float32)
            # Lưu vào Redis để lần sau dùng nhanh hơn
            await DB.redis.setex(cache_key, ttl, embedding.tobytes())
            return embedding  
        except Exception as e:
            logger.error(f"[Milvus] Lỗi lấy embedding {song_id}: {e}")
            return None

    # Hàm tìm kiếm chung
    async def _search_milvus(self, collection: Collection, vector: List[float], top_k: int, exclude_ids: Optional[List[Any]] = None) -> dict:
        if collection is None: return {}
        exclude_set = set(exclude_ids) if exclude_ids else set()

        # Tính toán số lượng tìm kiếm cần thiết và giới hạn tìm kiếm tối đa
        buffer_size = len(exclude_set)
        MAX_SEARCH_LIMIT = 500 
        search_limit = min(top_k + buffer_size + 20, MAX_SEARCH_LIMIT)
        try:
            # Thực hiện tìm kiếm trong Milvus
            results = await asyncio.to_thread(
                collection.search,
                data=[vector],
                anns_field="embedding",
                param=self.search_params,
                limit=search_limit, 
                expr=None,
                output_fields=["id"]
            )
            if not results: return {}
            score_map = {}
            count = 0
            for hit in results[0]:
                if count >= top_k: 
                    break 
                if hit.id in exclude_set:
                    continue
                score_map[hit.id] = hit.distance
                count += 1
            if count < top_k:
                logger.debug(f"Tìm được {count}/{top_k} bài sau khi lọc.")
            return score_map
        except Exception as e:
            logger.error(f"Lỗi tìm kiếm: {e}")
            return {}

    async def get_fallback_songs_ids(self, limit: int = 20, exclude_ids: Optional[List[Any]] = None) -> List[str]:
        # Tối ưu exclusion
        exclude_set = set(exclude_ids) if exclude_ids else set()
        should_use_mongo_filter = len(exclude_set) < 1000 
        
        query = {}
        if should_use_mongo_filter and exclude_ids:
            query["_id"] = {"$nin": exclude_ids}

        # Tính toán pool size
        if should_use_mongo_filter:
            pool_limit = max(limit + 40, int(limit * 1.5))
        else:
            pool_limit = min(limit + len(exclude_set) + 50, 500)

        # Lấy thêm trường "plays_7d" để tính toán trọng số
        cursor = DB.db["songs"].find(query, {"_id": 1, "plays_7d": 1}).sort("plays_7d", -1).limit(pool_limit)
        docs = await cursor.to_list(length=pool_limit)
        
        # Lọc trừ các ID không mong muốn
        candidates = []
        for doc in docs:
            if not should_use_mongo_filter:
                if doc["_id"] in exclude_set:
                    continue
            # Append nguyên cả object dict, KHÔNG append string ID
            candidates.append(doc) 

        if not candidates: return []

        # Lấy ngẫu nhiên
        if len(candidates) > limit:
            try:
                # Lúc này doc là Dict nên .get() hoạt động bình thường
                weights = np.array([np.log(doc.get("plays_7d", 0) + 2.0) for doc in candidates])
                
                # Tránh chia cho 0 nếu tổng weight = 0
                weight_sum = weights.sum()
                if weight_sum > 0:
                    probs = weights / weight_sum
                else:
                    probs = None # Random đều

                selected_indices = np.random.choice(
                    len(candidates), 
                    size=limit, 
                    replace=False, 
                    p=probs
                )
                selected_docs = [candidates[i] for i in selected_indices]
                
                random.shuffle(selected_docs)
                # Lấy ID ra để trả về
                return [doc["_id"] for doc in selected_docs]
                
            except Exception as e:
                logger.error(f"Lỗi tính toán Numpy, fallback về random thường: {e}")
                # Fallback an toàn
                fallback_sample = random.sample(candidates, limit)
                return [doc["_id"] for doc in fallback_sample]
        else:
            random.shuffle(candidates)
            # Lấy ID ra để trả về
            return [doc["_id"] for doc in candidates]

    # Hàm tạo các id bài hát gợi ý cá nhân hóa
    async def _generate_personalized_ids(self, user_id: str, limit: int, exclude_ids: Optional[List[Any]] = None) -> List[str]:
        use_milvus = True
        final_ids = []
        vector_to_search = None
        milvus_quota = limit 

        if self.als_collection is not None:
            try:
                v_long, v_short, vector_to_search = await self._get_session_vector(user_id)
                if vector_to_search is None:
                    use_milvus = False
                else:
                    if v_long is not None:
                        milvus_quota = int(limit)
                    else:
                        milvus_quota = int(limit*0.5)
            except Exception as e:
                logger.error(f"Lỗi tính toán Vector cho user {user_id}: {e}")
                use_milvus = False

        # Giai đoạn tìm kiếm chính với Milvus
        if use_milvus and vector_to_search is not None:
            try:
                # Tính toán pool size dựa trên QUOTA chứ không phải LIMIT gốc
                multiplier = 3.0 if milvus_quota <= 50 else 1.2
                pool_size = int(milvus_quota * multiplier)
                pool_size = max(pool_size, 20) # Tối thiểu vẫn phải lấy đủ dùng

                score_map = await self._search_milvus(
                    self.als_collection, 
                    vector_to_search.tolist(), 
                    top_k=pool_size, 
                    exclude_ids=exclude_ids
                )
                
                candidates = list(score_map.keys())
                
                # Chỉ lấy đúng số lượng Quota cho phép (ví dụ 10 bài)
                if candidates:
                    if len(candidates) > milvus_quota:
                        random.shuffle(candidates)
                        final_ids = candidates[:milvus_quota]
                    else:
                        final_ids = candidates
                        random.shuffle(final_ids)
                        
            except Exception as e:
                logger.error(f"Lỗi Search Milvus: {e}")
                final_ids = []

        # Nếu không đủ, bổ sung từ fallback
        missing_count = limit - len(final_ids)
        
        if missing_count > 0:
            current_exclude = (exclude_ids or []) + final_ids
            fallback_ids = await self.get_fallback_songs_ids(
                limit=missing_count, 
                exclude_ids=current_exclude
            )
            final_ids.extend(fallback_ids)
        return final_ids[:limit]

    # Lấy gợi ý cá nhân hóa cho trang chủ
    async def get_home_feed(self, user_id: str, limit: int = 20, refresh: bool = False) -> dict:
        feed_key = f"{cfg.FEED_KEY_PREFIX}{user_id}"
        session_key = f"{cfg.SESSION_FEED_KEY_PREFIX}{user_id}"
        
        # Xử lý Refresh
        if refresh:
            await DB.redis.delete(feed_key, session_key)

        # Kiểm tra trạng thái hiện tại
        async with DB.redis.pipeline() as pipe:
            pipe.scard(session_key) # Lấy tổng đã load
            pipe.llen(feed_key)     # Lấy hàng tồn kho
            total_generated, current_in_queue = await pipe.execute()
        logger.info(f"User {user_id}: Feed hiện có {current_in_queue} bài, đã tạo tổng {total_generated} bài.")
        
        # Logic bơm hàng (refill) thông minh
        # Chỉ bơm khi kho sắp hết và chưa chạm trần
        if current_in_queue < cfg.FEED_MIN_THRESHOLD and total_generated < cfg.MAX_FEED_SESSION:
            quota_left = cfg.MAX_FEED_SESSION - total_generated
            fetch_size = min(cfg.FEED_BATCH_SIZE, quota_left)
            if fetch_size > 0:
                seen_ids_bytes = await DB.redis.smembers(session_key)
                seen_ids = [x.decode('utf-8') for x in seen_ids_bytes] if seen_ids_bytes else []

                new_batch_ids = await self._generate_personalized_ids(
                    user_id, 
                    limit=fetch_size, 
                    exclude_ids=seen_ids
                )
                
                if new_batch_ids:
                    await DB.redis.rpush(feed_key, *new_batch_ids)
                    await DB.redis.expire(feed_key, int(cfg.FEED_TTL))
                    
                    await DB.redis.sadd(session_key, *new_batch_ids)
                    await DB.redis.expire(session_key, int(cfg.FEED_TTL))
                    
                    logger.info(f"Bơm thêm {len(new_batch_ids)} bài cho feed user {user_id}.")
                    addcount = len(new_batch_ids)
                    current_in_queue += addcount
                    total_generated += addcount

        # Lấy bài hát từ kho
        song_ids_bytes = await DB.redis.lpop(feed_key, count=limit)
        
        songs_ordered = []
        popcount = 0

        if song_ids_bytes:
            popcount = len(song_ids_bytes)
            song_ids = [x.decode('utf-8') for x in song_ids_bytes]

            cursor = DB.db["songs"].find({"_id": {"$in": song_ids}}, SONG_SUMMARY_PROJECTION)
            songs_unordered = await cursor.to_list(length=len(song_ids))
            song_map = {doc["_id"]: doc for doc in songs_unordered}
            
            for sid in song_ids:
                if sid in song_map:
                    songs_ordered.append(song_map[sid])
        
        remaining_local = current_in_queue - popcount
        # Xác định has_more
        if not songs_ordered:
            has_more = False
        else:
            if remaining_local > 0:
                has_more = True
            else:
                has_more = (total_generated < cfg.MAX_FEED_SESSION)

        return {
            "songs": songs_ordered,
            "has_more": has_more
        }
        
    def _normalize_scores(self, scores: dict) -> dict:
        """Chuẩn hóa điểm về khoảng [0, 1] dùng Min-Max Scaling"""
        if not scores: return {}
        
        values = list(scores.values())
        min_val = min(values)
        max_val = max(values)
        
        # Nếu max == min (tất cả điểm bằng nhau), trả về 1.0 hết hoặc 0.0
        if max_val == min_val:
            return {k: 1.0 for k in scores}
            
        # Công thức: (x - min) / (max - min)
        range_val = max_val - min_val
        return {k: (v - min_val) / range_val for k, v in scores.items()}

    def _merge_score_maps(self, als_scores: dict, content_scores: dict, limit: Optional[int] = None, weight_als: float = 0.5) -> List[str]:
        
        # Chuẩn hóa điểm số
        norm_als = self._normalize_scores(als_scores)
        norm_content = self._normalize_scores(content_scores)
        
        merged_map = {}
        all_ids = set(norm_als.keys()) | set(norm_content.keys())
        
        weight_content = 1.0 - weight_als

        # Tính điểm tổng hợp
        for key in all_ids:
            s_als = norm_als.get(key, 0.0)
            s_content = norm_content.get(key, 0.0)
            merged_map[key] = (s_als * weight_als) + (s_content * weight_content)
        
        # Lấy top K bài hát dựa trên điểm tổng hợp
        if limit is None:
            return sorted(merged_map.keys(), key=lambda k: merged_map[k], reverse=True)
        return heapq.nlargest(limit, merged_map, key=lambda k: merged_map[k])
    
    async def _generate_recommendation_next_songs_ids(self, user_id: str, current_song_id: str, limit: int, exclude_ids: Optional[List[Any]] = None) -> List[str]:
        # Tính toán pool size
        multiplier = 3.0 if limit <= 50 else 1.5
        pool_limit = int(limit * multiplier)
        pool_limit = max(pool_limit, cfg.PLAYLIST_MIN_THRESHOLD) # Tối thiểu vẫn phải lấy đủ dùng

        search_exclude_ids = (exclude_ids or []).append(current_song_id)

        async def _task_search_als():
            try:
                v_long, v_short, v_session = await self._get_session_vector(user_id)
                if v_session is not None:
                    return await self._search_milvus(
                        self.als_collection,
                        vector=v_session.tolist(),
                        top_k=pool_limit,
                        exclude_ids=search_exclude_ids
                    )
            except Exception as e:
                logger.error(f"Lỗi tìm kiếm ALS: {e}")
            return {}
        
        async def _task_search_content():
            try:
                curr_emb = await self._get_embedding_vector(
                    self.content_collection, current_song_id, 
                    cfg.CONTENT_VECTOR_KEY_PREFIX, cfg.CONTENT_VECTOR_TTL
                )
                if curr_emb is None: return {}

                return await self._search_milvus(
                    self.content_collection,
                    vector=curr_emb.tolist(),
                    top_k=pool_limit,
                    exclude_ids=search_exclude_ids
                )
            except Exception as e:
                logger.error(f"Lỗi tìm kiếm Content: {e}")
            return {}
        als_map, content_map = await asyncio.gather(_task_search_als(), _task_search_content())
        merged_ids = self._merge_score_maps(als_map, content_map, limit=limit, weight_als=0.3)
        return merged_ids

    # Get next songs based on current song
    async def get_next_songs(self, user_id: str, current_song_id: str, limit: int = 10, refresh: bool = False) -> dict:
        playlist_key = f"{cfg.PLAYLIST_KEY_PREFIX}{user_id}"
        session_key = f"{cfg.SESSION_PLAYLIST_KEY_PREFIX}{user_id}"

        if refresh:
            await DB.redis.delete(playlist_key, session_key)

        async with DB.redis.pipeline() as pipe:
            pipe.scard(session_key)
            pipe.llen(playlist_key)
            total_generated, current_in_queue = await pipe.execute()

        # Lấy thêm bài khi số bài còn lại chạm ngưỡng tối thiểu và số lượng tổng chưa chạm trần
        if current_in_queue < cfg.PLAYLIST_MIN_THRESHOLD and total_generated < cfg.MAX_PLAYLIST_SESSION:
            quota_left = cfg.MAX_PLAYLIST_SESSION - total_generated
            fetch_size = min(cfg.PLAYLIST_BATCH_SIZE, quota_left)

            if fetch_size > 0:
                seen_ids_bytes = await DB.redis.smembers(session_key)
                seen_ids = [x.decode('utf-8') for x in seen_ids_bytes] if seen_ids_bytes else []
                
                merged_ids = await self._generate_recommendation_next_songs_ids(
                    user_id, current_song_id, limit=fetch_size, exclude_ids=seen_ids
                )
                # Fallback nếu search rỗng
                if not merged_ids:
                    merged_ids = await self.get_fallback_songs_ids(limit=fetch_size, exclude_ids=seen_ids)
                if merged_ids:
                    await DB.redis.rpush(playlist_key, *merged_ids)
                    await DB.redis.expire(playlist_key, int(cfg.PLAYLIST_TTL))
                    await DB.redis.sadd(session_key, *merged_ids)
                    await DB.redis.expire(session_key, int(cfg.PLAYLIST_TTL))
                    
                    current_in_queue += len(merged_ids)
                    total_generated += len(merged_ids)
            
        # LẤY HÀNG TRẢ KHÁCH
        limit_final = min(limit, cfg.MAX_PLAYLIST_SESSION)
        song_ids_bytes = await DB.redis.lpop(playlist_key, count=limit_final)
        songs_ordered = []
        popped_count = 0

        if song_ids_bytes:
            popped_count = len(song_ids_bytes)
            song_ids = [x.decode('utf-8') for x in song_ids_bytes]
            
            cursor = DB.db["songs"].find({"_id": {"$in": song_ids}}, SONG_SUMMARY_PROJECTION)
            docs = await cursor.to_list(length=popped_count)
            docs_map = {d["_id"]: d for d in docs}
            
            for sid in song_ids:
                if sid in docs_map:
                    songs_ordered.append(docs_map[sid])

        # Xác định has_more
        remaining_local = current_in_queue - popped_count
        
        if not songs_ordered:
            has_more = False
        else:
            if remaining_local > 0:
                has_more = True
            else:
                # Nếu kho cạn, chỉ has_more khi chưa chạm trần
                has_more = (total_generated < cfg.MAX_PLAYLIST_SESSION)
        return {
            "next_songs": songs_ordered,
            "has_more": has_more
        }
            

# Singleton
recommendation_service = RecommendationService()