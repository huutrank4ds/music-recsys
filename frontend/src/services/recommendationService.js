// src/services/recommendationService.js
const PORT = import.meta.env.WEB_APP_PORT || 8000;
const API_URL = `http://localhost:${PORT}/api/v1/recs`; // Sử dụng biến môi trường cho cổng

export const fetchRecommendations = async (userId, limit=32, refresh=false) => {
  try {
    // Gọi API thực tế
    const response = await fetch(`${API_URL}/${userId}?limit=${limit}&refresh=${refresh}`, {
        method: 'GET',
    });
    
    if (!response.ok) {
      throw new Error('Network response was not ok');
    }

    const data_dict = await response.json();
    return {
      songs: data_dict.songs || [],
      has_more: data_dict.has_more || false
    }
    
  } catch (error) {
    console.error("Lỗi khi gọi API gợi ý:", error);
    return {
      songs: [],
      has_more: false
    };
  }
};

export const fetchNextSongs = async (userId, currentSongId, limit=32, refresh=false) => {
  try {
    const response = await fetch(`${API_URL}/${userId}/${currentSongId}?limit=${limit}&refresh=${refresh}`, {
        method: 'GET',
    });
    
    if (!response.ok) {
      throw new Error('Network response was not ok');
    }

    const data_dict = await response.json();
    return {
      next_songs: data_dict.next_songs || [],
      has_more: data_dict.has_more || false
    };
  } catch (error) {
    console.error("Lỗi khi gọi API bài tiếp theo:", error);
    return { next_songs: [], has_more: false };
  }
};

export const recordUserListen = async (userId, songId) => {
  try {
    const result = await fetch(`${API_URL}/record/${userId}/${songId}`, {
      method: 'POST',
    });
    if (result.status == "success") {
      console.log("Ghi nhận bài nghe thành công.");
    }
  } catch (error) {
    console.error("Lỗi khi ghi nhận bài nghe:", error);
  }
};