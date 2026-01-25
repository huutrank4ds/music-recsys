import { useState } from 'react';
import { useMusic } from '../context/MusicContext';
import { Search, LogOut } from 'lucide-react';
import SongCard from '../components/SongCard';
import SidebarPlayer from '../components/SidebarPlayer';
import EmptySidebarPlayer from '../components/EmptySidebarPlayer';

const Dashboard = () => {
  const { currentUser, logout, viewList, isLoading, currentSong } = useMusic();
  const [searchQuery, setSearchQuery] = useState('');

  const filteredSongs = viewList.filter(
    (song) =>
      song.track_name.toLowerCase().includes(searchQuery.toLowerCase()) ||
      song.artist_name.toLowerCase().includes(searchQuery.toLowerCase())
  );

  return (
    <div className="min-h-screen bg-gradient-to-br from-gray-900 via-gray-800 to-gray-900">
      {/* Main Content Container */}
      <div className="flex">
        {/* Left Content Area */}
        <div className="flex-1 mr-0 lg:mr-80">
          {/* Header */}
          <header className="sticky top-0 z-10 bg-gray-900/95 backdrop-blur-sm border-b border-gray-800">
            <div className="px-6 py-4">
              <div className="flex items-center justify-between mb-4">
                <h1 className="text-3xl font-bold bg-gradient-to-r from-primary-400 to-accent-500 bg-clip-text text-transparent">
                  Welcome back, {currentUser?.name}!
                </h1>
                <button
                  onClick={logout}
                  className="flex items-center gap-2 px-4 py-2 bg-gray-800 hover:bg-gray-700 rounded-lg transition-colors"
                >
                  <LogOut className="w-5 h-5" />
                  <span>Logout</span>
                </button>
              </div>

              {/* Search Bar */}
              <div className="relative">
                <Search className="absolute left-3 top-1/2 transform -translate-y-1/2 text-gray-400 w-5 h-5" />
                <input
                  type="text"
                  placeholder="Search for songs or artists..."
                  value={searchQuery}
                  onChange={(e) => setSearchQuery(e.target.value)}
                  className="w-full pl-10 pr-4 py-3 bg-gray-800 text-white rounded-lg focus:outline-none focus:ring-2 focus:ring-primary-500 transition-all"
                />
              </div>
            </div>
          </header>

          {/* Main Content */}
          <main className="p-6">
            <div className="mb-6">
              <h2 className="text-2xl font-bold text-white mb-2">
                {searchQuery ? 'Search Results' : 'Recommended for You'}
              </h2>
              <p className="text-gray-400">
                {filteredSongs.length} {filteredSongs.length === 1 ? 'song' : 'songs'}
              </p>
            </div>

            {/* Songs Grid */}
            {isLoading ? (
              <div className="w-full h-96 flex flex-col items-center justify-center">
                <div className="relative w-16 h-16">
                  <div className="absolute inset-0 border-4 border-gray-700/50 rounded-full"></div>
                  <div className="absolute inset-0 border-4 border-t-primary-500 border-r-accent-500 border-b-transparent border-l-transparent rounded-full animate-spin"></div>
                  <div className="absolute inset-1 border-4 border-t-transparent border-r-transparent border-b-white/20 border-l-transparent rounded-full animate-spin-reverse"></div>
                </div>
                <p className="mt-4 text-base font-medium text-gray-400 animate-pulse">
                  Đang phân tích gu âm nhạc của bạn...
                </p>
              </div>
            ) : (
              <>
                {viewList.length > 0 ? (
                  <div className="grid grid-cols-2 md:grid-cols-3 lg:grid-cols-4 gap-6">
                    {viewList.map((song, idx) => (
                      <SongCard key={song._id} song={song} />
                    ))}
                  </div>
                ) : (
                  <div className="text-center py-20">
                    <p className="text-gray-400 text-xl">No songs found</p>
                    <p className="text-gray-500 mt-2">Try a different search term</p>
                  </div>
                )}
            </>
            )}
          </main>
        </div>

        {/* Sidebar Player (Fixed Right) */}
        {currentSong ?
         <SidebarPlayer /> :
         <EmptySidebarPlayer />}
      </div>
    </div>
  );
};

export default Dashboard;
