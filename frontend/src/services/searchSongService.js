// src/services/searchSongService.js

// Lấy port từ biến môi trường hoặc mặc định 8000
const PORT = import.meta.env.VITE_WEB_APP_PORT || 8000; 
const API_URL = `http://localhost:${PORT}/api/v1/search`;

export const searchSongsService = async (query, limit = 15, skip = 0) => {
    try {
        // Tạo URLSearchParams để xử lý query string an toàn (tự động encode ký tự đặc biệt)
        const params = new URLSearchParams({
            query: query,
            limit: limit,
            skip: skip
        });

        const response = await fetch(`${API_URL}?${params.toString()}`, {
            method: 'GET',
            headers: {
                'Content-Type': 'application/json',
            },
        });

        if (!response.ok) {
            throw new Error(`HTTP error! status: ${response.status}`);
        }

        const responseJson = await response.json();
        console.log("Kết quả tìm kiếm từ service:", responseJson);
        // Trả về object chứa data và meta
        return {
            data: responseJson.data || [],
            meta: responseJson.meta || {}
        };

    } catch (error) {
        console.error("Lỗi service tìm kiếm:", error);
        // Trả về cấu trúc rỗng để không làm crash UI
        return { data: [], meta: { has_more: false } };
    }
};