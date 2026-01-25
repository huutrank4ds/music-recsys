import { useMusic } from '../context/MusicContext';
import { users } from '../data/mockData';
import { User } from 'lucide-react';

const UserSelection = () => {
  const { login } = useMusic();

  const handleUserSelect = (user) => {
    login(user);
  };

  return (
    <div className="min-h-screen flex items-center justify-center bg-gradient-to-br from-gray-900 via-purple-900 to-gray-900 p-8">
      <div className="max-w-4xl w-full">
        <div className="text-center mb-12">
          <h1 className="text-5xl font-bold mb-4 bg-gradient-to-r from-primary-400 to-accent-500 bg-clip-text text-transparent">
            Who is listening?
          </h1>
          <p className="text-gray-400 text-lg">Select your profile to continue</p>
        </div>

        <div className="grid grid-cols-2 md:grid-cols-3 gap-8">
          {users.map((user) => (
            <div
              key={user.id}
              onClick={() => handleUserSelect(user)}
              className="flex flex-col items-center cursor-pointer group"
            >
              <div className="relative mb-4 transition-transform duration-300 group-hover:scale-110">
                <div className="w-32 h-32 rounded-full overflow-hidden border-4 border-gray-700 group-hover:border-primary-500 transition-colors duration-300 shadow-lg group-hover:shadow-primary-500/50">
                  {user.name === "New User" ? (
                    <div className="w-full h-full bg-gray-700 flex items-center justify-center">
                      <User className="w-16 h-16 text-gray-400" />
                    </div>
                  ) : (
                    <img
                      src={user.avatar}
                      alt={user.name}
                      className="w-full h-full object-cover"
                    />
                  )}
                </div>
              </div>
              <h3 className="text-xl font-semibold text-white group-hover:text-primary-400 transition-colors">
                {user.name}
              </h3>
            </div>
          ))}
        </div>
      </div>
    </div>
  );
};

export default UserSelection;
