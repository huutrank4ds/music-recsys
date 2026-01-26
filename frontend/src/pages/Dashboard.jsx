// src/pages/Dashboard.jsx
import { useState, useRef } from 'react';
import { useMusic } from '../context/MusicContext';
import { Search, LogOut, ArrowLeft, Loader2 } from 'lucide-react';

// Components
import SongCard from '../components/SongCard';
import SongListItem from '../components/SongListItem';
import SidebarPlayer from '../components/SidebarPlayer';
import EmptySidebarPlayer from '../components/EmptySidebarPlayer';

// Services
import { searchSongsService } from '../services/searchSongService';

const Dashboard = () => {
  const { currentUser, logout, viewList, isLoading: isContextLoading, currentSong, fetchHomeFeed, viewHasMore } = useMusic();
  
  // Quản lý tìm kiếm
  const [mode, setMode] = useState('home'); 
  const [searchQuery, setSearchQuery] = useState('');
  const [searchResults, setSearchResults] = useState([]);
  const [isSearching, setIsSearching] = useState(false);
  
  // Quản lý cuộn trang
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
      console.log("Load more home feed...");
      fetchHomeFeed(false);
    }
  };

  return (
    <div className="h-screen bg-gradient-to-br from-gray-900 via-gray-800 to-gray-900 flex overflow-hidden">
      {/* Main Content Area */}
      <div className="flex-1 flex flex-col mr-0 lg:mr-80 min-w-0">
        
        {/* Header */}
        <header className="flex-none bg-gray-900/95 backdrop-blur-sm border-b border-gray-800 z-20">
          <div className="px-6 py-4">
            <div className="flex items-center justify-between mb-4">
              <h1 className="text-2xl md:text-3xl font-bold bg-gradient-to-r from-primary-400 to-accent-500 bg-clip-text text-transparent truncate">
                {mode === 'search' ? 'Search Results' : `Welcome, ${currentUser?.name}!`}
              </h1>
              <button onClick={logout} className="flex items-center gap-2 px-3 py-2 bg-gray-800 hover:bg-gray-700 rounded-lg transition-colors text-sm text-gray-300">
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

        <main 
          ref={mainContentRef}
          onScroll={handleScroll} 
          className="space-y-2 overflow-y-auto pr-1 [&::-webkit-scrollbar]:w-1.5 [&::-webkit-scrollbar-track]:bg-transparent [&::-webkit-scrollbar-thumb]:bg-gray-700 [&::-webkit-scrollbar-thumb]:rounded-full hover:[&::-webkit-scrollbar-thumb]:bg-gray-600"
        >
          {/* VIEW 1: HOME */}
          {mode === 'home' && (
            <>
              <div className="mb-6 mt-4 ml-4">
                <h2 className="text-xl font-bold text-white mb-2">Recommended for You</h2>
              </div>

              {/* Grid hiển thị */}
              <div className="grid grid-cols-2 md:grid-cols-3 xl:grid-cols-4 gap-6 pb-20">
                  {viewList.map((song) => (
                    <SongCard key={song._id} song={song} />
                  ))}
              </div>

              {/* Loading Indicator ở đáy khi cuộn */}
              {isContextLoading && viewList.length > 0 && (
                <div className="py-4 flex justify-center w-full">
                   <Loader2 className="w-8 h-8 animate-spin text-primary-500" />
                </div>
              )}
              
              {/* State rỗng hoặc loading lần đầu */}
              {isContextLoading && viewList.length === 0 && <LoadingSpinner text="Analyzing your taste..." />}
              {!isContextLoading && viewList.length === 0 && <EmptyState message="No recommendations available." />}
            </>
          )}

          {/* VIEW 2: SEARCH */}
          {mode === 'search' && (
            <div className="flex flex-col gap-2 pb-20">
               <p className="text-gray-400 mb-4 mt-4 ml-4">Found results for "{searchQuery}"</p>
               {searchResults.map((song, idx) => (
                 <SongListItem key={`${song._id}-${idx}`} song={song} index={idx} />
               ))}
               
               {isSearching && (
                 <div className="py-4 flex justify-center"><Loader2 className="w-6 h-6 animate-spin text-primary-500" /></div>
               )}
               
               {!searchHasMore && searchResults.length > 0 && (
                 <p className="text-center text-gray-500 py-6 text-sm">~ End of results ~</p>
               )}
               
               {!isSearching && searchResults.length === 0 && (
                 <EmptyState message={`No results found for "${searchQuery}"`} />
               )}
            </div>
          )}
        </main>
      </div>

      {/* Sidebar Player */}
      {currentSong ? <SidebarPlayer /> : <EmptySidebarPlayer />}
    </div>
  );
};

// ... Sub-components giữ nguyên ...
const LoadingSpinner = ({ text }) => (
  <div className="w-full h-64 flex flex-col items-center justify-center">
    <Loader2 className="w-10 h-10 animate-spin text-primary-500 mb-4" />
    <p className="text-gray-400 animate-pulse">{text}</p>
  </div>
);

const EmptyState = ({ message }) => (
  <div className="text-center py-20 bg-gray-800/30 rounded-xl border border-dashed border-gray-700">
    <p className="text-gray-400 text-lg">{message}</p>
  </div>
);

export default Dashboard;