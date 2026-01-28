import logging
import sys

def get_logger(name):
    """
    Tạo logger chuẩn cho ứng dụng.
    Output: Log ra màn hình console (stdout) với định dạng dễ đọc.
    """
    logger = logging.getLogger(name)
    logger.setLevel(logging.INFO)
    
    # Kiểm tra xem logger đã có handler chưa để tránh duplicate log
    if not logger.handlers:
        handler = logging.StreamHandler(sys.stdout)
        # Định dạng: [Thời gian] - [Tên Module] - [Level] - [Nội dung]
        formatter = logging.Formatter('%(asctime)s - %(levelname)s - [%(name)s] %(message)s')
        handler.setFormatter(formatter)
        logger.addHandler(handler)
        
    return logger