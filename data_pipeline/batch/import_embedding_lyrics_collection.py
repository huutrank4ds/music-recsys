# /opt/src/batch/import_embedding_lyrics_collection.py
import time
from pymilvus import ( #type: ignore
    connections, 
    utility, 
    Collection, 
    FieldSchema, 
    CollectionSchema, 
    DataType
) 
import config as cfg
from common.logger import get_logger
from common.schemas.milvus_schemas import index_params_milvus, get_milvus_content_embedding_schema

logger = get_logger("ImportLyricsEmbeddings")

def connect_to_milvus():
    """Thiết lập kết nối tới Milvus server"""
    try:
        connections.connect(
            alias="default", 
            uri=cfg.MILVUS_URI
        )
        logger.info(f"Kết nối thành công tới Milvus tại {cfg.MILVUS_URI}")
    except Exception as e:
        logger.error(f"Không thể kết nối tới Milvus: {e}")
        raise

def setup_collection(collection_name):
    """Xóa collection cũ và tạo mới với Schema + Index chuẩn"""
    # 1. Kiểm tra và xóa nếu đã tồn tại để tránh trùng lặp dữ liệu
    if utility.has_collection(collection_name):
        logger.info(f"Collection '{collection_name}' đã tồn tại. Đang xóa để khởi tạo lại (Re-indexing)...")
        utility.drop_collection(collection_name)

    # 2. Định nghĩa Schema (Cấu trúc dữ liệu)
    schema = get_milvus_content_embedding_schema()

    # 3. Tạo Collection
    collection = Collection(name=collection_name, schema=schema)
    logger.info(f"Đã tạo mới Collection: {collection_name}")

    # 4. Cấu hình Index (Sử dụng IVF_FLAT cho tốc độ truy vấn cao)
    index_params = index_params_milvus
    collection.create_index(field_name="embedding", index_params=index_params)
    logger.info("Đã khởi tạo Index thành công.")
    return collection

def run_bulk_insert(file_name, collection_name):
    """Thực hiện quá trình Bulk Insert từ MinIO vào Milvus"""
    try:
        logger.info(f"Đang gửi lệnh Bulk Insert cho file: {file_name} vào collection: {collection_name}")
        # Milvus tìm trong bucket mặc định (thường là 'a-bucket')
        task_id = utility.do_bulk_insert(
            collection_name=collection_name,
            files=[file_name]
        )
        logger.info(f"Đã tạo tác vụ nạp liệu. Task ID: {task_id}")
        return task_id
    except Exception as e:
        logger.error(f"Lỗi khi gửi lệnh Bulk Insert: {e}")
        return None

def monitor_insert_task(task_id, collection_name):
    """Theo dõi trạng thái của task cho đến khi hoàn thành"""
    if task_id is None:
        return False

    logger.info("Bắt đầu theo dõi trạng thái tác vụ...")
    while True:
        state = utility.get_bulk_insert_state(task_id=task_id)
        
        if state.state_name == "Completed":
            logger.info("Quá trình nạp liệu hoàn tất thành công!")
            
            # Sau khi nạp xong, load vào RAM để sẵn sàng Search
            collection = Collection(collection_name)
            collection.load()
            logger.info(f"Số lượng bản ghi hiện có trong Milvus: {collection.num_entities}")
            return True
            
        elif state.state_name == "Failed":
            logger.error(f"Quá trình nạp liệu thất bại. Lý do: {state.failed_reason}")
            return False
            
        else:
            logger.info(f"Trạng thái: {state.state_name} ({state.progress}%)")
        
        time.sleep(5)

def main():
    """Hàm điều phối chính"""
    try:
        connect_to_milvus()
        
        col_name = cfg.MILVUS_CONTENT_COLLECTION
        # File này phải được upload lên MinIO trước
        file_to_import = "embeddings_lyrics.parquet" 
        
        # Bước 1: Dọn dẹp và thiết lập cấu trúc
        setup_collection(col_name)
        
        # Bước 2: Kích hoạt nạp dữ liệu từ Data Lake
        task_id = run_bulk_insert(file_to_import, col_name)
        
        # Bước 3: Theo dõi tiến độ
        if task_id:
            success = monitor_insert_task(task_id, col_name)
            if success:
                logger.info("Pipeline nhập liệu kết thúc tốt đẹp.")
            else:
                logger.warning("Pipeline kết thúc với lỗi nạp liệu.")
                
    except Exception as e:
        logger.critical(f"Lỗi hệ thống: {e}")
    finally:
        connections.disconnect("default")

if __name__ == "__main__":
    main()