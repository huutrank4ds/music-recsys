import pandas as pd
import json
import time
from pathlib import Path
from kafka import KafkaProducer
from datetime import datetime

# --- CẤU HÌNH ---
KAFKA_TOPIC = "music_stream"
KAFKA_BOOTSTRAP_SERVERS = ['kafka:9092'] 
DATA_FOLDER = '/opt/data'  # Thư mục chứa 3 file Parquet

def create_producer():
    try:
        producer = KafkaProducer(
            bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
            value_serializer=lambda x: json.dumps(x).encode('utf-8')
        )
        print("Kết nối Kafka thành công!")
        return producer
    except Exception as e:
        print(f"Không thể kết nối Kafka: {e}")
        return None

def load_and_merge_parquet():
    """
    Hàm này tìm tất cả file .parquet, đọc và gộp lại thành 1 DataFrame duy nhất
    """
    # Tìm tất cả file có đuôi .parquet trong thư mục data
    parquet_files = list(Path(DATA_FOLDER).rglob('*.parquet'))
    
    if not parquet_files:
        print(f"Không tìm thấy file Parquet nào trong {DATA_FOLDER}")
        return None

    print(f"Tìm thấy {len(parquet_files)} file Parquet. Đang đọc và gộp...")
    
    # Đọc từng file và đưa vào list
    df_list = []
    for file in parquet_files:
        try:
            # Dùng pyarrow engine để đọc cho nhanh
            df_part = pd.read_parquet(file, engine='pyarrow')
            df_list.append(df_part)
            print(f"-> Đã đọc xong: {Path(file).name} ({len(df_part)} dòng)")
        except Exception as e:
            print(f"Lỗi đọc file {file}: {e}")

    # Gộp lại thành 1 DataFrame to
    if df_list:
        full_df = pd.concat(df_list, ignore_index=True)
        return full_df
    return None

def process_and_send():
    producer = create_producer()
    if not producer: return

    # 1. Đọc và Gộp dữ liệu
    df = load_and_merge_parquet()
    if df is None: return

    # 2. Kiểm tra tên cột (Để map cho đúng)
    # Dữ liệu Hugging Face thường có cột: user_id, timestamp, artist_name, track_name...
    print(f"Các cột có trong dữ liệu: {list(df.columns)}")

    # 3. Xử lý Timestamp và Sắp xếp
    # Cần đảm bảo cột thời gian tên là 'timestamp'. Nếu tên khác phải đổi.
    if 'timestamp' not in df.columns:
        print("Không thấy cột 'timestamp'. Hãy kiểm tra lại tên cột in ở trên.")
        # Ví dụ nếu nó tên là 'time_played' thì bỏ comment dòng dưới:
        # df.rename(columns={'time_played': 'timestamp'}, inplace=True)
        return

    print("Đang sắp xếp dữ liệu theo thời gian...")
    df['timestamp'] = pd.to_datetime(df['timestamp'])
    df = df.sort_values(by='timestamp')

    print(f"Bắt đầu bắn {len(df)} dòng dữ liệu vào Kafka...")
    # 4. Loop và Bắn
    for index, row in df.iterrows(): #type: ignore
        message = row.to_dict()
        
        # --- TIME TRAVEL (Giả lập realtime) ---
        # Chuyển timestamp object thành string để gửi JSON không bị lỗi
        # Nếu muốn hiển thị giờ hiện tại:
        # message['original_time'] = str(message['timestamp'])
        # message['timestamp'] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        
        # Nếu muốn giữ nguyên giờ gốc để test logic:
        message['timestamp'] = str(message['timestamp'])

        producer.send(KAFKA_TOPIC, value=message)
        
        if index % 1000 == 0:
            # Lấy tên bài hát, phòng trường hợp tên cột khác nhau
            track = message.get('track_name', message.get('track', 'Unknown Track'))
            print(f"Sent [{index}]: {track}")
            
        # Tốc độ bắn (0.01s = 100 tin/giây)
        time.sleep(0.01)

    print("🎉 Đã gửi xong toàn bộ dữ liệu!")

if __name__ == "__main__":
    time.sleep(5) # Chờ xíu cho chắc
    process_and_send()