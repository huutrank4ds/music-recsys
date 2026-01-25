// src/services/recommendationService.js
import { songs as defaultSongs } from '../data/mockData';

const PORT = import.meta.env.WEB_APP_PORT || 8000;
const API_URL = `http://localhost:${PORT}/api/v1/recs`; // Sử dụng biến môi trường cho cổng

export const fetchRecommendations = async (userId, n=100) => {
  try {
    // Gọi API thực tế
    const response = await fetch(`${API_URL}/recommendations/${userId}/n=${n}`, {
        method: 'GET',
    });
    
    if (!response.ok) {
      throw new Error('Network response was not ok');
    }

    const data_dict = await response.json();
    const data = data_dict.personalized_recommendations;
    if (!data || data.length === 0) {
      console.warn("API trả về rỗng, dùng dữ liệu mẫu.");
      return defaultSongs;
    }

    return data;
  } catch (error) {
    console.error("Lỗi khi gọi API gợi ý:", error);
    return defaultSongs;
  }
};

export const fetchNextSongs = async (userId, currentSongId, n=100) => {
  try {
    const response = await fetch(`${API_URL}/next-songs/${userId}/${currentSongId}/n=${n}`, {
        method: 'GET',
    });
    
    if (!response.ok) {
      throw new Error('Network response was not ok');
    }

    const data_dict = await response.json();
    const data = data_dict.next_songs;
    if (!data || data.length === 0) {
      console.warn("API trả về rỗng, dùng dữ liệu mẫu.");
      return defaultSongs;
    }

    return data;
  } catch (error) {
    console.error("Lỗi khi gọi API bài tiếp theo:", error);
    return defaultSongs;
  }
};