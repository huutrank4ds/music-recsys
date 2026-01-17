#!/bin/bash

echo "🚀 [Pipeline] Khởi động hệ thống..."

# ---------------------------------------------------------------
# BƯỚC 1: CHẠY PRODUCER (BACKGROUND)
# ---------------------------------------------------------------
echo "🎵 [1/3] Khởi động Producer (Background)..."
# Chạy Producer ngầm để script không bị chặn lại ở đây
python3 -u /opt/src/pipelines/ingestion/producer.py &

# Lưu lại PID của Producer để dùng cho lệnh wait ở cuối
PRODUCER_PID=$!
echo "✅ Producer đã chạy với PID: $PRODUCER_PID"

# ---------------------------------------------------------------
# BƯỚC 2: CHỜ TOPIC ĐƯỢC TẠO
# ---------------------------------------------------------------
echo "⏳ [2/3] Đợi 10s để Producer tạo Topic và ổn định..."
sleep 10

# ---------------------------------------------------------------
# BƯỚC 3: CHẠY SPARK STREAMING (BACKGROUND)
# ---------------------------------------------------------------
echo "🔥 [3/3] Khởi động Spark Streaming (Background)..."
# Chạy Spark ngầm để ta có thể xuống bước 4
/opt/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  --deploy-mode client \
  --total-executor-cores 1 \
  --executor-memory 512m \

# ---------------------------------------------------------------
# BƯỚC 4: "KHÓA" SCRIPT LẠI BẰNG PRODUCER
# ---------------------------------------------------------------
echo "🔒 Hệ thống đã sẵn sàng! Container sẽ sống theo Producer..."

# Lệnh wait này sẽ treo script ở đây mãi mãi cho đến khi Producer chết.
# Nếu Spark Streaming chết (do ta kill bảo trì), lệnh wait này KHÔNG bị ảnh hưởng.
# Container vẫn sống!
wait $PRODUCER_PID