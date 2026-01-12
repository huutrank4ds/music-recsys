# Sử dụng image gốc của Apache Spark
FROM apache/spark:3.5.0-python3

# Chuyển sang quyền root để cài thư viện
USER root

# Thiết lập thư mục làm việc (Optional nhưng nên có)
WORKDIR /opt/spark/work-dir

# --- PHẦN QUAN TRỌNG: COPY VÀ CÀI ĐẶT ---
# 1. Copy file requirements từ máy thật vào trong image
COPY requirements.txt .

# 2. Chạy pip install theo danh sách trong file
RUN pip install --no-cache-dir -r requirements.txt

# ---
# (Tùy chọn) Nếu bạn muốn mặc định user là spark (185) thì uncomment dòng dưới
# Nhưng với môi trường Dev/Đồ án thì để root cho đỡ lỗi permission volume
# USER 185