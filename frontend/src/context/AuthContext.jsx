import { createContext, useContext, useState, useEffect } from 'react';
import { fetchUsers, createUser } from '../services/userService';

const AuthContext = createContext();
const USER_KEY = 'music_app_user';

export const useAuth = () => {
  const context = useContext(AuthContext);
  if (!context) {
    throw new Error('useAuth must be used within an AuthProvider');
  }
  return context;
};

export const AuthProvider = ({ children }) => {
  const [user, setUser] = useState(null);
  const [availableUsers, setAvailableUsers] = useState([]);
  const [userError, setUserError] = useState(null);
  const [isCheckingAuth, setIsCheckingAuth] = useState(true);

  useEffect(() => {
    const initAuth = async () => {
      setIsCheckingAuth(true);
      try {
        const storedUserJson = localStorage.getItem(USER_KEY);
        const storedUser = storedUserJson ? JSON.parse(storedUserJson) : null;

        const data = await fetchUsers();
        const userList = Array.isArray(data) ? data : (data.data || []);
        setAvailableUsers(userList);

        if (storedUser) {
          const foundUser = userList.find(u => u._id === storedUser._id);
          
          if (foundUser) {
            setUser(foundUser);
            localStorage.setItem(USER_KEY, JSON.stringify(foundUser)); 
          } else {
            console.warn("User trong LocalStorage không tồn tại trên hệ thống. Xóa key.");
            localStorage.removeItem(USER_KEY);
            setUser(null);
          }
        } else {
          setUser(null);
        }

      } catch (err) {
        console.error("Lỗi xác thực khởi tạo:", err);
        setUser(null); 
      } finally {
        setIsCheckingAuth(false);
      }
    };
    initAuth();
  }, []);

  const login = (userData) => {
    localStorage.setItem(USER_KEY, JSON.stringify(userData));
    setUser(userData);
  };

  const logout = () => {
    localStorage.removeItem(USER_KEY);
    setUser(null);
  };

  const createNewProfile = async (username) => {
    try {
      const newUser = await createUser(username);
      setAvailableUsers(prev => [...prev, newUser]); 
      return { success: true }; 
    } catch (err) {
      return { success: false, message: err.message };
    }
  };

  return (
    <AuthContext.Provider value={{ user, login, logout, availableUsers, isCheckingAuth, userError, createNewProfile }}>
      {children}
    </AuthContext.Provider>
  );
};