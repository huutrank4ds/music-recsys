"""
Download LastFM-1K dataset từ HuggingFace
Dataset: https://huggingface.co/datasets/matthewfranglen/lastfm-1k
"""
from datasets import load_dataset
import pandas as pd
from pathlib import Path
import pyarrow as pa
import pyarrow.parquet as pq

# Cấu hình
OUTPUT_DIR = Path("/opt/data/raw")
DATASET_NAME = "matthewfranglen/lastfm-1k"

def download_and_save():
    print(f"Đang download dataset từ HuggingFace: {DATASET_NAME}...")
    
    # Load dataset
    dataset = load_dataset(DATASET_NAME)
    
    # Tạo thư mục output
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    
    # Lưu từng split
    for split_name in dataset.keys():
        print(f"\nXử lý split: {split_name}")
        df = dataset[split_name].to_pandas()
        
        print(f"  - Số dòng: {len(df):,}")
        print(f"  - Columns: {list(df.columns)}")
        
        # Lưu ra parquet
        output_file = OUTPUT_DIR / f"{split_name}.parquet"
        df.to_parquet(output_file, index=False)
        print(f"  - Đã lưu: {output_file}")
    
    print(f"\nHOÀN THÀNH! Data đã được lưu tại: {OUTPUT_DIR}")

if __name__ == "__main__":
    download_and_save()
