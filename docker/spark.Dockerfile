# 1. Base Image
FROM apache/spark:3.5.0-python3

USER root

# 2. Cài đặt thêm curl để tải JAR
RUN set -ex; \
    apt-get update && \
    apt-get install -y gcc g++ python3-dev curl && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

# 3. Nâng cấp pip (Quan trọng cho confluent-kafka)
RUN pip install --upgrade pip setuptools wheel

# 4. DOWNLOAD SPARK JARS (Tải thẳng vào thư mục jars của Spark)
# Lưu ý: Phải tải đúng version tương thích
WORKDIR /opt/spark/jars

# --- A. Hadoop AWS & Dependencies (Cho MinIO/S3) ---
# Hadoop 3.3.4 đi kèm với aws-java-sdk-bundle 1.12.262
RUN curl -O https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.4/hadoop-aws-3.3.4.jar \
    && curl -O https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.12.262/aws-java-sdk-bundle-1.12.262.jar

# --- B. Kafka Spark & Dependencies ---
# Spark 3.5.0 tương thích Kafka Client 3.4.1
RUN curl -O https://repo1.maven.org/maven2/org/apache/spark/spark-sql-kafka-0-10_2.12/3.5.0/spark-sql-kafka-0-10_2.12-3.5.0.jar \
    && curl -O https://repo1.maven.org/maven2/org/apache/spark/spark-token-provider-kafka-0-10_2.12/3.5.0/spark-token-provider-kafka-0-10_2.12-3.5.0.jar \
    && curl -O https://repo1.maven.org/maven2/org/apache/kafka/kafka-clients/3.4.1/kafka-clients-3.4.1.jar \
    && curl -O https://repo1.maven.org/maven2/org/apache/commons/commons-pool2/2.11.1/commons-pool2-2.11.1.jar

# --- C. MongoDB Spark Connector ---
RUN curl -O https://repo1.maven.org/maven2/org/mongodb/spark/mongo-spark-connector_2.12/10.3.0/mongo-spark-connector_2.12-10.3.0.jar \
    && curl -O https://repo1.maven.org/maven2/org/mongodb/mongodb-driver-sync/4.11.1/mongodb-driver-sync-4.11.1.jar \
    && curl -O https://repo1.maven.org/maven2/org/mongodb/bson/4.11.1/bson-4.11.1.jar \
    && curl -O https://repo1.maven.org/maven2/org/mongodb/mongodb-driver-core/4.11.1/mongodb-driver-core-4.11.1.jar

# 5. Cài đặt thư viện Python
WORKDIR /opt/spark/work-dir
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# 6. Permissions (Trả quyền lại cho user spark)
RUN chown -R 185:185 /opt/spark/jars
USER 185