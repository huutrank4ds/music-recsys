import { useMusic } from '../context/MusicContext';
import ReactPlayer from 'react-player/youtube';
import { Play, Pause, SkipForward, SkipBack, Volume2, VolumeX, ListMusic } from 'lucide-react';
import { useState } from 'react';

const SidebarPlayer = () => {
  const {
    currentSong,
    isPlaying,
    togglePlay,
    playNext,
    playPrevious,
    playFromPlaylist,
    volume,
    played,
    duration,
    playerRef,
    handleProgress,
    handleDuration,
    handleSeek,
    handleVolumeChange,
    handleEnded,
    playlist,
    currentIndex,
    autoPlayNext,
    toggleAutoPlayNext,
  } = useMusic();

  const [isMuted, setIsMuted] = useState(false);
  const [showVolumeSlider, setShowVolumeSlider] = useState(false);

  const formatTime = (seconds) => {
    if (!seconds || isNaN(seconds)) return '0:00';
    const mins = Math.floor(seconds / 60);
    const secs = Math.floor(seconds % 60);
    return `${mins}:${secs.toString().padStart(2, '0')}`;
  };

  const toggleMute = () => {
    // Nếu volume đang là 0 mà bấm nút loa -> Tự động bật lên 50%
    if (volume === 0 && !isMuted) {
      handleVolumeChange(0.5);
      setIsMuted(false);
    } else {
      setIsMuted(!isMuted);
    }
  };

  // Logic mới: Khi kéo thanh trượt thì tự động bỏ Mute
  const onVolumeSliderChange = (e) => {
    const newVolume = parseFloat(e.target.value);
    
    // Nếu đang mute mà kéo thanh trượt -> Bỏ mute
    if (isMuted) {
      setIsMuted(false);
    }
    
    // Nếu kéo về 0 -> Tự động bật chế độ mute
    if (newVolume === 0) {
      setIsMuted(true);
    }
    
    handleVolumeChange(newVolume);
  };

  const effectiveVolume = isMuted ? 0 : volume;
  const nextSongs = playlist.slice(currentIndex + 1);

  return (
    <div className="fixed right-0 top-0 w-80 h-screen bg-gray-800 border-l border-gray-700 flex flex-col z-20 hidden lg:flex">
      {/* Hidden YouTube Player */}
      <ReactPlayer
        key={currentSong ? currentSong._id : 'no-song'}
        ref={playerRef}
        url={`https://www.youtube.com/watch?v=${currentSong.url}`}
        playing={isPlaying}
        volume={effectiveVolume}
        onProgress={handleProgress}
        onDuration={handleDuration}
        onEnded={handleEnded}
        width="0"
        height="0"
        config={{
          youtube: {
            playerVars: { showinfo: 0 },
            origin: window.location.origin,
          }
        }}
      />

      {/* Now Playing Section */}
      <div className="p-4 border-b border-gray-700">
        <h3 className="text-xs text-gray-400 mb-3 uppercase tracking-wider">Now Playing</h3>
        
        {/* Vinyl Record */}
        <div className="relative mb-3 mx-auto" style={{ width: '180px', height: '180px' }}>
          <div className={`w-full h-full relative ${isPlaying ? 'animate-spin-slow' : ''}`}>
            <div className="w-full h-full rounded-full overflow-hidden shadow-2xl">
              <img
                src={currentSong.image_url}
                alt={currentSong.track_name}
                className="w-full h-full object-cover"
              />
            </div>
            <div className="absolute inset-0 rounded-full" style={{
              background: 'radial-gradient(circle, transparent 20%, rgba(0,0,0,0.1) 20%, rgba(0,0,0,0.1) 22%, transparent 22%, transparent 40%, rgba(0,0,0,0.05) 40%)',
              pointerEvents: 'none'
            }}></div>
            <div className="absolute inset-0 flex items-center justify-center pointer-events-none">
              <div className="w-12 h-12 bg-gray-900 rounded-full shadow-2xl border-4 border-gray-800 relative">
                <div className="absolute inset-1 bg-gradient-to-br from-gray-700 to-gray-900 rounded-full"></div>
              </div>
            </div>
          </div>
        </div>

        {/* Song Info */}
        <div className="text-center mb-3">
          <h2 className="text-base font-bold text-white mb-1 truncate">
            {currentSong.track_name}
          </h2>
          <p className="text-gray-400 text-xs truncate">{currentSong.artist_name}</p>
        </div>

        {/* Progress Bar */}
        <div className="space-y-1">
          <input
            type="range"
            min="0"
            max="0.999999"
            step="any"
            value={played}
            onChange={(e) => handleSeek(e.target.value)}
            className="w-full h-1 bg-gray-700 rounded-lg appearance-none cursor-pointer accent-primary-500"
          />
          <div className="flex justify-between text-xs text-gray-400">
            <span>{formatTime(played * duration)}</span>
            <span>{formatTime(duration)}</span>
          </div>
        </div>

        {/* Playback Controls */}
        <div className="flex items-center justify-between mt-3">
          
          {/* --- VOLUME CONTROL FIX START --- */}
          <div
            className="relative"
            onMouseEnter={() => setShowVolumeSlider(true)}
            onMouseLeave={() => setShowVolumeSlider(false)}
          >
            <button
              onClick={toggleMute}
              className="p-2 hover:bg-gray-700 rounded-full transition-colors"
            >
              {isMuted || volume === 0 ? (
                <VolumeX className="w-5 h-5 text-gray-300" />
              ) : (
                <Volume2 className="w-5 h-5 text-gray-300" />
              )}
            </button>
            
            {showVolumeSlider && (
              <div className="absolute left-0 top-full pt-1 z-50">
                <div className="bg-gray-900 p-3 rounded-lg shadow-xl border border-gray-700">
                  <div className="flex flex-col gap-2 w-36">
                    <div className="flex items-center gap-2">
                      <Volume2 className="w-4 h-4 text-gray-400 flex-shrink-0" />
                      
                      {/* SLIDER ĐÃ SỬA: Value phụ thuộc vào isMuted */}
                      <input
                        type="range"
                        min="0"
                        max="1"
                        step="0.01"
                        value={effectiveVolume} 
                        onChange={onVolumeSliderChange}
                        className="flex-1 h-1 bg-gray-700 rounded-lg appearance-none cursor-pointer accent-primary-500"
                      />
                    </div>
                    <span className="text-xs text-gray-400 text-center">
                      {Math.round(effectiveVolume * 100)}%
                    </span>
                  </div>
                </div>
              </div>
            )}
          </div>
          {/* --- VOLUME CONTROL FIX END --- */}

          <div className="flex items-center gap-2">
            <button onClick={playPrevious} className="p-2 hover:bg-gray-700 rounded-full transition-colors">
              <SkipBack className="w-5 h-5 text-gray-300" />
            </button>
            <button onClick={togglePlay} className="p-3 bg-primary-500 hover:bg-primary-600 rounded-full transition-all hover:scale-110 shadow-lg">
              {isPlaying ? <Pause className="w-6 h-6 text-white fill-white" /> : <Play className="w-6 h-6 text-white fill-white" />}
            </button>
            <button onClick={playNext} className="p-2 hover:bg-gray-700 rounded-full transition-colors">
              <SkipForward className="w-5 h-5 text-gray-300" />
            </button>
          </div>

          <button
            onClick={toggleAutoPlayNext}
            className={`p-2 rounded-full transition-colors ${autoPlayNext ? 'bg-primary-500 hover:bg-primary-600 text-white' : 'hover:bg-gray-700 text-gray-400'}`}
            title={autoPlayNext ? 'Tắt tự động phát tiếp' : 'Bật tự động phát tiếp'}
          >
            <ListMusic className="w-5 h-5" />
          </button>
        </div>
      </div>

      <div className="p-4 flex flex-col flex-1 overflow-hidden">
        <div className="flex items-center justify-between mb-3">
          <h3 className="text-xs text-gray-400 uppercase tracking-wider">Gợi ý tiếp theo</h3>
          <span className="text-xs text-gray-500">{nextSongs.length} bài</span>
        </div>
        
        <div className="space-y-2 overflow-y-auto pr-1 [&::-webkit-scrollbar]:w-1.5 [&::-webkit-scrollbar-track]:bg-transparent [&::-webkit-scrollbar-thumb]:bg-gray-700 [&::-webkit-scrollbar-thumb]:rounded-full hover:[&::-webkit-scrollbar-thumb]:bg-gray-600" style={{ maxHeight: '240px' }}>
          {nextSongs.length > 0 ? (
            nextSongs.map((song, idx) => (
              <div key={song.id || song._id || 'song'} onClick={() => playFromPlaylist(idx + currentIndex + 1)} className="flex items-center gap-2 p-2 rounded-lg hover:bg-gray-700 transition-colors cursor-pointer group">
                <img src={song.image_url} alt={song.track_name} className="w-10 h-10 rounded object-cover" />
                <div className="flex-1 min-w-0">
                  <p className="text-xs font-medium text-white truncate group-hover:text-primary-400 transition-colors">{song.track_name}</p>
                  <p className="text-xs text-gray-400 truncate">{song.artist_name}</p>
                </div>
              </div>
            ))
          ) : (
            <p className="text-gray-500 text-xs text-center py-4">Không còn bài hát</p>
          )}
        </div>
      </div>
    </div>
  );
};

export default SidebarPlayer;