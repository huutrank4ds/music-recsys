//src/services/loggingService.js
const PORT = import.meta.env.WEB_APP_PORT || 8000;
const API_URL = `http://localhost:${PORT}/api/v1/logs`; // Sử dụng biến môi trường cho cổng

export const logUserEvent = async (logData) => {
  try {
    const response = await fetch(`${API_URL}/event`, {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
        },
        body: JSON.stringify(logData),
    });
    if (!response.ok) {
        throw new Error('Network response was not ok');
    }
    const result = await response.json();
    return result;
  } catch (error) {
    console.error("Lỗi khi gửi log người dùng:", error);
    return { success: false, message: error.message };
  }
};