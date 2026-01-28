from datetime import datetime, timezone
from app.core.database import DB
from typing import List, Optional, Any
from pymongo.errors import OperationFailure #type: ignore
from bson import ObjectId, errors as bson_errors #type: ignore
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
            cursor = DB.db[cfg.COLLECTION_USERS].find({}, USER_SUMARY_PROJECTION)
            users = await cursor.to_list(length=num_users)
            
            # [FIX] Convert ObjectId sang string cho từng user để trả về JSON không lỗi
            for user in users:
                user["_id"] = str(user["_id"])
                
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
            # [FIX] Kiểm tra xem user_id có đúng định dạng ObjectId không
            if not ObjectId.is_valid(user_id):
                logger.warning(f"User ID không hợp lệ: {user_id}")
                return None

            user = await DB.db[cfg.COLLECTION_USERS].find_one(
                {"_id": ObjectId(user_id)}, # [FIX] Convert string -> ObjectId để query
                USER_SUMARY_PROJECTION
            )
            
            # [FIX] Convert ObjectId -> string để trả về
            if user:
                user["_id"] = str(user["_id"])
                
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
        # Chuẩn bị dữ liệu người dùng mới
        new_user_data = {
            "username": name,
            "signup_date": datetime.now(timezone.utc),
            "image_url": None,
            "latent_vector": None
        }

        try:
            # Insert vào MongoDB
            result = await DB.db[cfg.COLLECTION_USERS].insert_one(new_user_data)
            # [FIX] Gán lại _id dạng string vào object trả về
            new_user_data["_id"] = str(result.inserted_id)
            # Xử lý datetime thành string
            new_user_data["signup_date"] = new_user_data["signup_date"].isoformat()
            logger.info(f"Người dùng mới đã được tạo: {new_user_data['_id']}")
            return new_user_data

        except OperationFailure as e:
            logger.error(f"Lỗi khi tạo người dùng mới: {e}")
            return None