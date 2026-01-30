# app/services/user_service.py
from datetime import datetime, timezone
from typing import List, Optional
from pymongo.errors import OperationFailure, DuplicateKeyError  #type: ignore
import uuid

from app.core.database import DB
from common.logger import get_logger
import config as cfg
from common.constants import USER_SUMARY_PROJECTION

logger = get_logger("UserService")

class UserService:
    @staticmethod
    async def get_users(num_users: int) -> List[dict]:
        """
        Lấy danh sách tất cả người dùng với các trường tóm tắt.
        """
        try:
            cursor = DB.db[cfg.MONGO_USERS_COLLECTION].find({}, USER_SUMARY_PROJECTION) #type: ignore
            users = await cursor.to_list(length=num_users)
                
            return users
        except OperationFailure as e:
            logger.error(f"Lỗi truy vấn MongoDB: {e}")
            return []
    
    @staticmethod
    async def get_user_by_id(user_id: str) -> Optional[dict]:
        """
        Lấy thông tin người dùng theo user_id với các trường tóm tắt.
        """
        try:
            user = await DB.db[cfg.MONGO_USERS_COLLECTION].find_one( #type: ignore
                {"_id": user_id}
            )
                
            return user
        except OperationFailure as e:
            logger.error(f"Lỗi truy vấn MongoDB: {e}")
            return None
        except Exception as e:
            logger.error(f"Lỗi không xác định khi lấy user: {e}")
            return None
        
    @staticmethod
    async def create_new_user(name: str) -> Optional[dict]:
        """
        Tạo người dùng mới chỉ với tên, các trường khác tự sinh.
        Trả về: Dict user đã tạo (có _id dạng string) hoặc None nếu lỗi.
        """
        if not name or len(name.strip()) == 0:
            return None
        max_retries = 3
        for attempt in range(max_retries):
            new_id = str(uuid.uuid4()) # Sinh ID mới
            new_user_data = {
                "_id": new_id,
                "username": name.strip(),
                "signup_date": datetime.now(timezone.utc),
                "image_url": None,
                "latent_vector": None
            }
            try:
                await DB.db[cfg.MONGO_USERS_COLLECTION].insert_one(new_user_data) #type: ignore
                
                # Nếu chạy đến đây nghĩa là thành công, thoát vòng lặp
                logger.info(f"Tạo user thành công: {new_id}")
                return await UserService.get_user_by_id(new_id) 

            except DuplicateKeyError:
                logger.warning(f"Xung đột ID: {new_id}. Đang thử lại lần {attempt + 1}...")
                continue
            
            except Exception as e:
                # Các lỗi khác (mất mạng, DB sập...) thì return None luôn
                logger.error(f"Lỗi hệ thống khi tạo user: {e}")
                return None
        
        # Nếu thử 3 lần mà vẫn trùng ID
        logger.error("Không thể tạo User sau 3 lần thử.")
        return None