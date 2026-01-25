import { defineConfig } from 'vite'
import react from '@vitejs/plugin-react'

// https://vite.dev/config/
export default defineConfig({
  plugins: [react()],
  server: {
    host: true,       // Cho phép Docker map port ra ngoài
    port: 5173,       // Port cố định
    watch: {
      usePolling: true // Bắt buộc cho Windows: Dùng chế độ thăm dò file để hot reload chạy chuẩn
    }
  }
})
