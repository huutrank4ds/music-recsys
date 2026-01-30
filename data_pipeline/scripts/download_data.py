from datasets import load_dataset
from pathlib import Path
import config as cfg
from common.logger import get_logger

# Sử dụng logger chuẩn của hệ thống
logger = get_logger("Download Dataset")

# Lấy đường dẫn từ config nếu có, nếu không dùng mặc định
OUTPUT_DIR = Path(cfg.RAW_DATA_DIR) #type: ignore
DATASET_NAME = "matthewfranglen/lastfm-1k"

def download_and_save():
    logger.info(f"Bắt đầu tải dataset từ HuggingFace: {DATASET_NAME}")
    
    try:
        # Load dataset (chế độ mặc định của HuggingFace là stream/cache thông minh)
        dataset = load_dataset(DATASET_NAME)
        
        # Tạo thư mục output
        OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
        
        for split_name in dataset.keys(): #type: ignore
            output_file = OUTPUT_DIR / f"{split_name}.parquet"
            logger.info(f"Đang xử lý split: {split_name} (Dự kiến lưu tại: {output_file})")
            
            # Sử dụng to_parquet trực tiếp của dataset để tiết kiệm RAM
            dataset[split_name].to_parquet(output_file) #type: ignore
            
            logger.info(f"Đã lưu thành công split: {split_name}")
            
        logger.info(f"HOÀN TẤT! Toàn bộ dữ liệu đã được lưu tại: {OUTPUT_DIR}")
        
    except Exception as e:
        logger.error(f"Lỗi trong quá trình tải hoặc lưu dữ liệu: {e}")

if __name__ == "__main__":
    download_and_save()