#!/bin/bash
# daily_workflow.sh
# Đường dẫn log để theo dõi
LOG_FILE="/home/user/pipeline.log" 

log() {
    echo "$(date '+%Y-%m-%d %H:%M:%S') - $1" | tee -a $LOG_FILE
}

log "🔄 BẮT ĐẦU QUY TRÌNH TRAIN MODEL HÀNG NGÀY"

# --- BƯỚC 1: TẮT JOB MINIO ---
log "1. Đang tắt Job Stream to MinIO..."
# Lệnh pkill -f tìm process theo tên file python và kill nó bên trong container
docker exec spark-master pkill -f "stream_to_minio.py"

# Đợi 10s để Spark dọn dẹp và trả Worker
sleep 15
log "   -> Đã tắt Job MinIO. Worker 1 đã trống."

# --- BƯỚC 2: CHẠY TRAIN ALS ---
log "2. Đang chạy Training ALS Model..."
# Lưu ý: Không dùng -d (detach) ở đây. Chúng ta muốn script đợi train xong mới đi tiếp.
docker exec spark-master spark-submit \
  --master spark://spark-master:7077 \
  --driver-memory 1g \
  --executor-memory 1g \
  --total-executor-cores 1 \
  /opt/src/training/train_als.py >> $LOG_FILE 2>&1

# Kiểm tra xem train có thành công không
if [ $? -eq 0 ]; then
    log "   -> Training thành công!"
else
    log "❌ Training thất bại! Kiểm tra log."
    # Dù thất bại vẫn phải bật lại MinIO để không mất log
fi

# --- BƯỚC 3: BẬT LẠI JOB MINIO ---
log "3. Đang khởi động lại Job Stream to MinIO..."
docker exec -d spark-master spark-submit \
  --master spark://spark-master:7077 \
  --driver-memory 512m \
  --executor-memory 800m \
  --total-executor-cores 1 \
  --conf "spark.kafka.consumer.cache.enabled=false" \
  /opt/src/jobs/stream_to_minio.py

log "✅ QUY TRÌNH HOÀN TẤT. Hệ thống đã trở lại trạng thái Streaming Full."