# Sử dụng Node.js v20 (nhẹ, phiên bản Alpine)
FROM node:20-alpine

# Thiết lập thư mục làm việc trong container
WORKDIR /app

# Copy file package để cài thư viện trước (tận dụng Docker cache)
COPY package*.json ./

# Cài đặt dependencies
RUN npm install

# Copy toàn bộ source code vào
COPY . .

# Mở port 5173 (Port mặc định của Vite)
EXPOSE 5173

# Chạy server development với cờ --host để truy cập được từ bên ngoài
CMD ["npm", "run", "dev", "--", "--host"]