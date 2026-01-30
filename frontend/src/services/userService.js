const PORT = import.meta.env.VITE_WEB_APP_PORT || 8000; 
const API_URL = `http://localhost:${PORT}/api/v1`;

export const fetchUsers = async (num_users = 5) => {
    try {
        const params = new URLSearchParams({
            num_users: num_users
        });
        const response = await fetch(`${API_URL}/users?${params.toString()}`, {
            method: 'GET',
            headers: {
                'Content-Type': 'application/json',
            },
        });
        if (!response.ok) {
            throw new Error(`HTTP error! status: ${response.status}`);
        }
        const responseJson = await response.json();
        return responseJson.users || [];
    } catch (error) {
        console.error("Lỗi service lấy người dùng:", error);
        return [];
    }
}

export const createUser = async (name) => {
  try {
    const response = await fetch(`${API_URL}/new-user`, {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
      },
      body: JSON.stringify({ name }), 
    });

    if (!response.ok) {
      const errorData = await response.json().catch(() => ({}));
      throw new Error(errorData.detail || 'Không thể tạo người dùng');
    }

    const newUser = await response.json();
    return newUser.user;
  } catch (error) {
    console.error("Lỗi createUser:", error);
    throw error;
  }
};