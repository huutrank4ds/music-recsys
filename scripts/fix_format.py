import pyarrow as pa
import pyarrow.parquet as pq
from pathlib import Path
import shutil

SOURCE_DIR = Path('/opt/data/raw')
DEST_DIR = Path('/opt/data/data_clean/')

def rebirth_parquet():
    print("BẮT ĐẦU QUY TRÌNH TÁI SINH DỮ LIỆU (PANDAS CHUNKING)")
    
    if DEST_DIR.exists(): shutil.rmtree(DEST_DIR)
    DEST_DIR.mkdir(parents=True, exist_ok=True)

    files = [f for f in SOURCE_DIR.glob("*.parquet") if f.is_file() and not f.name.startswith("_")]

    for file_path in files:
        print(f" Xử lý: {file_path.name}")
        dest_path = DEST_DIR / file_path.name
        
        try:
            # 1. Mở file Parquet bằng PyArrow để lấy Schema trước
            pf = pq.ParquetFile(file_path)
            
            # Tạo Writer mới
            first_chunk = True
            
            # 2. Đọc từng chunk bằng Pandas (Batch size an toàn)
            for df_chunk in pf.iter_batches(batch_size=50000):
                df = df_chunk.to_pandas()
                
                # --- CHUYỂN ĐỔI TIMESTAMP MẠNH TAY ---
                if 'timestamp' in df.columns:
                    # Ép về datetime64[us] (Microseconds)
                    df['timestamp'] = df['timestamp'].astype('datetime64[us]')

                # 3. Ghi ra file mới
                # Cách tốt nhất với PyArrow Table để ghi append
                table = pa.Table.from_pandas(df)
                
                if first_chunk:
                    writer = pq.ParquetWriter(dest_path, table.schema, coerce_timestamps='us', allow_truncated_timestamps=True)
                    first_chunk = False
                
                writer.write_table(table)
            
            if not first_chunk:
                writer.close()
                print(f"  Đã tái sinh thành công: {dest_path}")

        except Exception as e:
            print(f"  Lỗi: {e}")

if __name__ == "__main__":
    rebirth_parquet()