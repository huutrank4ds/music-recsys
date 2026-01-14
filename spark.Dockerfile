# 1. Base Image
FROM apache/spark:3.5.0-python3

# Chuyển sang root để cài cắm
USER root

# 2. Cài các công cụ cơ bản (Giữ gcc/python3-dev để phòng hờ, nhưng BỎ librdkafka-dev)
# Lý do: librdkafka-dev trong apt quá cũ, gây xung đột.
RUN set -ex; \
    apt-get update && \
    apt-get install -y gcc g++ python3-dev && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

WORKDIR /opt/spark/work-dir
COPY requirements.txt .

# 3. QUAN TRỌNG NHẤT: Nâng cấp pip, setuptools và wheel
# Pip cũ trong image Spark không nhận diện được file .whl mới của confluent-kafka
# nên nó mới cố đâm đầu vào việc build từ source và gặp lỗi.
RUN pip install --upgrade pip setuptools wheel

# 4. Cài đặt thư viện (Lúc này nó sẽ tải file .whl thay vì .tar.gz)
RUN pip install --no-cache-dir -r requirements.txt

# Trả lại quyền cho user Spark
USER 185