import React, { useState, useEffect } from 'react';
import { useAuth } from '../context/AuthContext';
import { fetchUsers, createUser } from '../services/userService';
import { User, Loader2, AlertCircle, Plus, X } from 'lucide-react';

const UserSelection = () => {
  const { login } = useAuth();
  
  // --- STATE DỮ LIỆU ---
  const [users, setUsers] = useState([]);
  const [isLoading, setIsLoading] = useState(true);
  const [error, setError] = useState(null);

  // --- STATE MODAL TẠO USER ---
  const [showModal, setShowModal] = useState(false);
  const [newUserName, setNewUserName] = useState('');
  const [isCreating, setIsCreating] = useState(false); // Loading khi đang tạo

  // 1. Tải danh sách user khi trang vừa mở
  useEffect(() => {
    const loadUsers = async () => {
      try {
        const data = await fetchUsers();
        // Xử lý trường hợp backend trả về { data: [...] } hoặc mảng trực tiếp [...]
        const userList = Array.isArray(data) ? data : (data.data || []);
        setUsers(userList);
      } catch (err) {
        setError("Không thể kết nối đến máy chủ.");
      } finally {
        setIsLoading(false);
      }
    };
    loadUsers();
  }, []);

  // 2. Xử lý khi chọn User để đăng nhập
  const handleSelectUser = (user) => {
    login(user); // AuthContext sẽ lưu user và chuyển hướng sang Dashboard
  };

  // 3. Xử lý khi bấm nút "Create" trong Modal
  const handleCreateUser = async (e) => {
    e.preventDefault(); // Ngăn reload trang
    if (!newUserName.trim()) return;

    setIsCreating(true);
    try {
      // Gọi API tạo user
      const newUser = await createUser(newUserName);
      
      // Thành công: Thêm user mới vào danh sách hiển thị ngay lập tức
      // (Không cần gọi lại API fetchAllUsers -> Tối ưu hiệu năng)
      setUsers(prev => [...prev, newUser]);
      
      // Reset form và đóng modal
      setNewUserName('');
      setShowModal(false);
    } catch (err) {
      alert("Lỗi khi tạo profile: " + err.message);
    } finally {
      setIsCreating(false);
    }
  };

  return (
    <div className="min-h-screen bg-gray-900 flex items-center justify-center p-4 relative">
      <div className="max-w-4xl w-full">
        {/* Header */}
        <div className="text-center mb-12">
          <h1 className="text-4xl font-bold text-transparent bg-clip-text bg-gradient-to-r from-primary-400 to-purple-500 mb-4">
            Who is listening?
          </h1>
          <p className="text-gray-400">Chọn profile để bắt đầu trải nghiệm nhạc cá nhân hóa</p>
        </div>

        {/* --- TRẠNG THÁI LOADING & ERROR --- */}
        {isLoading && (
          <div className="flex flex-col items-center justify-center h-64">
            <Loader2 className="w-12 h-12 text-primary-500 animate-spin mb-4" />
            <p className="text-gray-500">Đang tải danh sách...</p>
          </div>
        )}

        {error && !isLoading && (
          <div className="flex flex-col items-center text-red-400 mb-8">
            <AlertCircle className="w-12 h-12 mb-2" />
            <p>{error}</p>
            <button onClick={() => window.location.reload()} className="mt-4 underline">Thử lại</button>
          </div>
        )}

        {/* --- DANH SÁCH USER --- */}
        {!isLoading && !error && (
          <div className="grid grid-cols-2 md:grid-cols-3 lg:grid-cols-4 gap-8 justify-items-center animate-fade-in">
            
            {/* Render từng User */}
            {users.map((user) => (
              <div
                key={user._id || user.id} // Ưu tiên _id từ Mongo
                onClick={() => handleSelectUser(user)}
                className="group flex flex-col items-center cursor-pointer"
              >
                {/* Avatar Container */}
                <div className="w-32 h-32 md:w-40 md:h-40 rounded-full overflow-hidden mb-4 border-4 border-transparent group-hover:border-primary-500 transition-all duration-300 transform group-hover:scale-105 shadow-lg bg-gray-800 flex items-center justify-center relative">
                  
                  {/* Logic Avatar: Ưu tiên ảnh, nếu lỗi hoặc không có thì hiện Icon */}
                  {user.image_url ? (
                    <img 
                      src={user.image_url} 
                      alt={user.username} 
                      className="w-full h-full object-cover"
                      onError={(e) => {
                        e.target.style.display = 'none'; // Ẩn ảnh lỗi
                        e.target.nextSibling.style.display = 'flex'; // Hiện icon fallback
                      }}
                    />
                  ) : null}
                  
                  {/* Icon Fallback */}
                  <div className={`absolute inset-0 flex items-center justify-center bg-gray-700 text-gray-400 ${user.image_url ? 'hidden' : 'flex'}`}>
                     <User className="w-16 h-16" />
                  </div>
                  
                  {/* Overlay hover */}
                  <div className="absolute inset-0 bg-black bg-opacity-0 group-hover:bg-opacity-20 transition-all duration-300"></div>
                </div>

                <span className="text-lg text-gray-300 font-medium group-hover:text-white transition-colors duration-300 text-center px-2 truncate w-full">
                  {user.username}
                </span>
              </div>
            ))}
            
            {/* Nút ADD PROFILE */}
            <div 
              onClick={() => setShowModal(true)}
              className="group flex flex-col items-center cursor-pointer"
            >
                <div className="w-32 h-32 md:w-40 md:h-40 rounded-full border-2 border-dashed border-gray-600 flex items-center justify-center mb-4 group-hover:border-gray-400 group-hover:bg-gray-800 transition-all duration-300">
                    <Plus className="w-12 h-12 text-gray-500 group-hover:text-gray-300" />
                </div>
                <span className="text-lg text-gray-500 group-hover:text-gray-300">Add Profile</span>
            </div>

          </div>
        )}
      </div>

      {/* --- MODAL (POPUP) TẠO USER --- */}
      {showModal && (
        <div className="fixed inset-0 z-50 flex items-center justify-center bg-black bg-opacity-80 backdrop-blur-sm animate-in fade-in duration-200">
          <div className="bg-gray-800 border border-gray-700 rounded-xl p-8 w-full max-w-md shadow-2xl transform scale-100 transition-transform">
            
            {/* Modal Header */}
            <div className="flex justify-between items-center mb-6">
              <h2 className="text-2xl font-bold text-white">Create Profile</h2>
              <button 
                onClick={() => setShowModal(false)}
                className="text-gray-400 hover:text-white transition-colors"
              >
                <X className="w-6 h-6" />
              </button>
            </div>

            {/* Form */}
            <form onSubmit={handleCreateUser} className="space-y-6">
              <div>
                <label className="block text-sm font-medium text-gray-400 mb-2">Tên hiển thị</label>
                <input
                  type="text"
                  value={newUserName}
                  onChange={(e) => setNewUserName(e.target.value)}
                  placeholder="Nhập tên của bạn"
                  autoFocus
                  className="w-full px-4 py-3 bg-gray-900 border border-gray-600 rounded-lg text-white placeholder-gray-500 focus:outline-none focus:border-primary-500 focus:ring-1 focus:ring-primary-500 transition-colors"
                />
              </div>

              <div className="flex gap-4">
                <button
                  type="button"
                  onClick={() => setShowModal(false)}
                  className="flex-1 px-4 py-3 border border-gray-600 text-gray-300 rounded-lg hover:bg-gray-700 transition-colors font-medium"
                >
                  Hủy
                </button>
                <button
                  type="submit"
                  disabled={!newUserName.trim() || isCreating}
                  className="flex-1 px-4 py-3 bg-primary-600 text-white rounded-lg hover:bg-primary-700 transition-colors font-medium disabled:opacity-50 disabled:cursor-not-allowed flex items-center justify-center"
                >
                  {isCreating ? <Loader2 className="w-5 h-5 animate-spin" /> : 'Tạo mới'}
                </button>
              </div>
            </form>

          </div>
        </div>
      )}
    </div>
  );
};

export default UserSelection;