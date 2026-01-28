// src/pages/Dashboard.jsx
import { useState, useRef } from 'react';
import { useMusic } from '../context/MusicContext';
import { useAuth } from '../context/AuthContext'; 
import { Search, LogOut, ArrowLeft, Loader2 } from 'lucide-react';

// Components
import SongCard from '../components/SongCard';
import SongListItem from '../components/SongListItem';
import RightSidebar from '../components/RightSidebar';
import SearchResultList from '../components/SearchResultList';
import ViewGrid from '../components/ViewGrid';

// Services
import { searchSongsService } from '../services/searchSongService';

const Dashboard = () => {
  // --- 2. Lấy Auth State từ useAuth ---
  const { user: currentUser, logout } = useAuth(); 

  // --- 3. Lấy Music State từ useMusic (Bỏ currentUser và logout ở đây đi) ---
  const { 
    viewList, 
    isLoading: isContextLoading, 
    currentSong, 
    fetchHomeFeed, 
    viewHasMore,
    cleanupMusicSession
  } = useMusic();
  
  // --- LOCAL STATE ---
  const [mode, setMode] = useState('home'); 
  const [searchQuery, setSearchQuery] = useState('');
  const [searchResults, setSearchResults] = useState([]);
  const [isSearching, setIsSearching] = useState(false);
  
  const [searchPage, setSearchPage] = useState(0);
  const [searchHasMore, setSearchHasMore] = useState(true);
  const searchLimit = 15;
  const mainContentRef = useRef(null); 

  // --- API SEARCH ---
  const fetchSearchResults = async (query, skip_val, isNewSearch = false) => {
    if (!query.trim()) return;
    setIsSearching(true);
    try {
      const { data, meta } = await searchSongsService(query, searchLimit, skip_val);
      if (isNewSearch) {
        setSearchResults(data);
      } else {
        setSearchResults(prev => [...prev, ...data]);
      }
      setSearchHasMore(meta?.has_more ?? false);
    } catch (error) {
      console.error("Lỗi search:", error);
    } finally {
      setIsSearching(false);
    }
  };

  // --- EVENT HANDLERS ---
  const handleSearchSubmit = (e) => {
    e.preventDefault();
    if (!searchQuery.trim()) return;
    setMode('search');
    setSearchPage(0);
    setSearchHasMore(true);
    fetchSearchResults(searchQuery, 0, true);
  };

  const handleBackToHome = () => {
    setMode('home');
    setSearchQuery('');
    setSearchResults([]);
    setSearchHasMore(true);
  };

  const handleScroll = (e) => {
    const { scrollTop, scrollHeight, clientHeight } = e.target;
    const isNearBottom = scrollHeight - scrollTop <= clientHeight + 50;
    
    if (!isNearBottom) return;

    if (mode === 'search') {
      if (isSearching || !searchHasMore) return;
      const nextSkip = (searchPage + 1) * searchLimit;
      setSearchPage(prev => prev + 1);
      fetchSearchResults(searchQuery, nextSkip, false);
    } 
    else { 
      if (isContextLoading || !viewHasMore) return;
      fetchHomeFeed(false);
    }
  };

  const handleSafeLogout = async () => {
    try {
      await cleanupMusicSession(); 
    } catch (error) {
      console.error("Lỗi gửi log khi logout:", error);
    } finally {
      logout();
    }
  };

  return (
    <div className="h-screen bg-gradient-to-br from-gray-900 via-gray-800 to-gray-900 flex overflow-hidden">
      
      {/* MAIN CONTENT 
          Nếu có currentSong -> Margin Right 80 (320px) để chừa chỗ cho Sidebar
      */}
      <div className={`flex-1 flex flex-col min-w-0 transition-all duration-300 ${currentSong ? 'lg:mr-80' : ''}`}>
        
        {/* Header */}
        <header className="flex-none bg-gray-900/95 backdrop-blur-sm border-b border-gray-800 z-20">
          <div className="px-6 py-4">
            <div className="flex items-center justify-between mb-4">
              <h1 className="text-2xl md:text-3xl font-bold bg-gradient-to-r from-primary-400 to-accent-500 bg-clip-text text-transparent truncate">
                {mode === 'search' ? 'Search Results' : `Welcome, ${currentUser?.name || 'Guest'}!`}
              </h1>
              
              {/* Nút Logout gọi hàm từ useAuth */}
              <button onClick={handleSafeLogout} className="flex items-center gap-2 px-3 py-2 bg-gray-800 hover:bg-gray-700 rounded-lg transition-colors text-sm text-gray-300">
                <LogOut className="w-4 h-4" />
                <span className="hidden sm:inline">Logout</span>
              </button>
            </div>

            {/* Search Bar */}
            <form onSubmit={handleSearchSubmit} className="relative flex items-center gap-3">
              {mode === 'search' && (
                <button 
                  type="button"
                  onClick={handleBackToHome}
                  className="p-3 bg-gray-800 hover:bg-gray-700 rounded-lg text-gray-300 transition-colors"
                >
                  <ArrowLeft className="w-5 h-5" />
                </button>
              )}
              <div className="relative flex-1">
                <Search className="absolute left-3 top-1/2 transform -translate-y-1/2 text-gray-400 w-5 h-5" />
                <input
                  type="text"
                  placeholder="Search songs, artists..."
                  value={searchQuery}
                  onChange={(e) => setSearchQuery(e.target.value)}
                  className="w-full pl-10 pr-4 py-3 bg-gray-800 text-white rounded-lg focus:outline-none focus:ring-2 focus:ring-primary-500 border border-transparent focus:border-primary-500"
                />
              </div>
            </form>
          </div>
        </header>

        {/* Scrollable Content */}
        <main 
          ref={mainContentRef}
          onScroll={handleScroll} 
          className="space-y-2 overflow-y-auto pr-1 [&::-webkit-scrollbar]:w-1.5 [&::-webkit-scrollbar-track]:bg-transparent [&::-webkit-scrollbar-thumb]:bg-gray-700 [&::-webkit-scrollbar-thumb]:rounded-full hover:[&::-webkit-scrollbar-thumb]:bg-gray-600"
        >
          {/* VIEW 1: HOME */}
          {mode === 'home' && (
            <ViewGrid viewList={viewList} isContextLoading={isContextLoading} />
          )}

          {/* VIEW 2: SEARCH */}
          {mode === 'search' && (
            <SearchResultList 
              query={searchQuery} 
              results={searchResults}
            />
          )}
        </main>
      </div>

      {/* Sidebar Component */}
      <RightSidebar />

    </div>
  );
};

export default Dashboard;