from typing import Any
from fastapi import APIRouter, HTTPException #type: ignore
from app.services.user_service import UserService
from common.logger import get_logger
from pydantic import BaseModel #type: ignore

logger = get_logger("User API")
router = APIRouter()

class UserCreateRequest(BaseModel):
    name: str

@router.get("/users", summary="Tìm kiếm bài hát (Text Search)")
async def get_users(num_users: int = 5) -> Any:
    """
    Lấy danh sách người dùng với giới hạn số lượng.
    URL gọi sẽ là: /api/users?num_users=20
    """
    try:
        users = await UserService.get_users(num_users)
        return {"users": users}
        
    except Exception as e:
        logger.error(f"Lỗi khi lấy danh sách người dùng: {e}")
        raise HTTPException(status_code=500, detail="Lỗi hệ thống khi lấy danh sách người dùng")
    
@router.get("/user/{user_id}", summary="Lấy thông tin người dùng theo ID")
async def get_user_by_id(user_id: str) -> Any:
    """
    Lấy thông tin người dùng theo user_id.
    URL gọi sẽ là: /api/user/{user_id}
    """
    try:
        user = await UserService.get_user_by_id(user_id)
        if user is None:
            raise HTTPException(status_code=404, detail="Người dùng không tồn tại")
        return {"user": user}
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Lỗi khi lấy thông tin người dùng: {e}")
        raise HTTPException(status_code=500, detail="Lỗi hệ thống khi lấy thông tin người dùng")
    
@router.post("/new-user", summary="Tạo người dùng mới")
async def create_new_user(payload: UserCreateRequest) -> Any:
    """
    Tạo người dùng mới với tên.
    URL gọi sẽ là: /api/new-user
    Body: { "name": "Tên người dùng" }
    """
    try:
        new_user = await UserService.create_new_user(payload.name)
        if new_user is None:
            raise HTTPException(status_code=500, detail="Không thể tạo người dùng mới")
        return {"user": new_user}
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Lỗi khi tạo người dùng mới: {e}")
        raise HTTPException(status_code=500, detail="Lỗi hệ thống khi tạo người dùng mới")