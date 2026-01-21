# docker/backend.Dockerfile

FROM python:3.10-slim

# 1. Đặt thư mục làm việc là /app (Gốc của Container)
WORKDIR /app

RUN apt-get update && apt-get install -y build-essential && rm -rf /var/lib/apt/lists/*

# 2. Copy file requirements từ máy thật vào
COPY backend/requirements.txt ./requirements.txt

RUN pip install --no-cache-dir -r requirements.txt

# 3. Copy thư mục app vào trong /app/app
COPY backend/app ./app

# 4. Thiết lập PYTHONPATH
# Đặt là /app để Python hiểu folder 'app' và 'common' là các module
ENV PYTHONPATH=/app

EXPOSE 8000

# 5. Lệnh chạy ứng dụng
CMD ["python", "-m", "uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8000", "--reload"]