FROM python:3.9-alpine

# Cài đặt Docker CLI và Bash
RUN apk add --no-cache docker-cli bash curl

# Cài thư viện
RUN pip install schedule

WORKDIR /opt/src

# --- THAY ĐỔI Ở ĐÂY ---
# Entrypoint trỏ vào file mới trong thư mục orchestration
CMD ["python", "-u", "/opt/src/orchestration/manager.py"]