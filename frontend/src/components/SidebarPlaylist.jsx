import React, { memo } from 'react';
import { useMusic } from '../context/MusicContext';
import { Loader2 } from 'lucide-react';

const SidebarSkeleton = () => (
  <div className="flex items-center gap-2 p-2 rounded-lg animate-pulse">
    <div className="w-10 h-10 bg-gray-700 rounded" />
    <div className="flex-1 min-w-0 space-y-2">
      <div className="h-3 bg-gray-700 rounded w-3/4" />
      <div className="h-2 bg-gray-700 rounded w-1/2" />
    </div>
  </div>
);

const SidebarPlaylist = memo(() => {
  const {
    playlist,
    currentIndex,
    isSidebarLoading,
    sidebarHasMore,
    fetchRecommendNextSongs,
    playFromPlaylist,
    currentSong
  } = useMusic();

  const nextSongs = playlist.slice(currentIndex + 1);

  const handleScrollNextSongs = (e) => {
    if (isSidebarLoading || !sidebarHasMore) return;
    const { scrollTop, scrollHeight, clientHeight } = e.target;
    const isNearBottom = scrollHeight - scrollTop <= clientHeight + 20;
    
    if (!isNearBottom) return;
    const currentSongId = currentSong ? currentSong._id : null;
    if (currentSongId) {
        fetchRecommendNextSongs(currentSongId, false);
    }
  };

  return (
    <div className="p-4 flex flex-col flex-1 overflow-hidden">
      <div className="flex items-center justify-between mb-3">
        <h3 className="text-xs text-gray-400 uppercase tracking-wider">Gợi ý tiếp theo</h3>
        <span className="text-xs text-gray-500">{nextSongs.length} bài</span>
      </div>
      
      <div 
        className="space-y-2 overflow-y-auto pr-1 flex-1 [&::-webkit-scrollbar]:w-1.5 [&::-webkit-scrollbar-track]:bg-transparent [&::-webkit-scrollbar-thumb]:bg-gray-700 [&::-webkit-scrollbar-thumb]:rounded-full hover:[&::-webkit-scrollbar-thumb]:bg-gray-600" 
        onScroll={handleScrollNextSongs}
      >
        {/* Loading State Initial */}
        {nextSongs.length === 0 && isSidebarLoading ? (
          <>
            <SidebarSkeleton />
            <SidebarSkeleton />
            <SidebarSkeleton />
            <SidebarSkeleton />
          </>
        ) : (
          <>
            {/* List Songs */}
            {nextSongs.map((song, idx) => (
              <div 
                key={song.id || song._id || idx} 
                onClick={() => playFromPlaylist(idx + currentIndex + 1)} 
                className="flex items-center gap-2 p-2 rounded-lg hover:bg-gray-700 transition-colors cursor-pointer group"
              >
                <img src={song.image_url} alt={song.track_name} className="w-10 h-10 rounded object-cover" />
                <div className="flex-1 min-w-0">
                  <p className="text-xs font-medium text-white truncate group-hover:text-primary-400 transition-colors">{song.track_name}</p>
                  <p className="text-xs text-gray-400 truncate">{song.artist_name}</p>
                </div>
              </div>
            ))}

            {/* Spinner Load More */}
            {nextSongs.length > 0 && isSidebarLoading && (
              <div className="py-2 flex justify-center w-full">
                <Loader2 className="w-5 h-5 animate-spin text-primary-500" />
              </div>
            )}

            {/* End of List */}
            {!isSidebarLoading && !sidebarHasMore && nextSongs.length > 0 && (
              <p className="text-gray-500 text-[10px] text-center py-2 italic">~ End of results ~</p>
            )}
            
            {/* Empty */}
            {!isSidebarLoading && nextSongs.length === 0 && (
               <p className="text-gray-500 text-xs text-center py-4">No recommendations yet</p>
            )}
          </>
        )}
      </div>
    </div>
  );
});

export default SidebarPlaylist;