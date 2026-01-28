import React, { useState, useEffect } from 'react';
import ReactPlayer from 'react-player/youtube';
import { useMusic } from '../context/MusicContext';
import { Play, Pause, SkipForward, SkipBack, Volume2, VolumeX, ListMusic } from 'lucide-react';

const SidebarPlayer = () => {
  const {
    isSongLoading,
    currentSong, isPlaying, togglePlay, playNext, playPrevious,
    volume, playerRef,
    handleUpdateProgress, // Hàm sync ngầm
    handleUpdateDuration, // Hàm sync duration
    handleVolumeChange, 
    handlePlayerStart,
    handlePlayerBuffer,
    handlePlayerBufferEnd,
    handlePlayerEnded,
    autoPlayNext, 
    toggleAutoPlayNext,
  } = useMusic();

  // --- LOCAL STATE (Chỉ component này render mỗi giây) ---
  const [played, setPlayed] = useState(0); 
  const [duration, setDuration] = useState(0);
  
  const [isMuted, setIsMuted] = useState(false);
  const [showVolumeSlider, setShowVolumeSlider] = useState(false);

  // Reset khi đổi bài
  useEffect(() => {
    setPlayed(0);
  }, [currentSong]);

  // --- HANDLERS CỤC BỘ ---
  const onProgress = (state) => {
    // 1. Cập nhật UI ngay lập tức
    setPlayed(state.played);
    // 2. Báo cáo về Context (Ref)
    handleUpdateProgress(state);
  };

  const onDuration = (dur) => {
    setDuration(dur);
    handleUpdateDuration(dur);
  };

  const onSeek = (e) => {
    const newValue = parseFloat(e.target.value);
    setPlayed(newValue);
    playerRef.current?.seekTo(newValue);
  };

  const toggleMute = () => {
    if (volume === 0 && !isMuted) {
      handleVolumeChange(0.5);
      setIsMuted(false);
    } else {
      setIsMuted(!isMuted);
    }
  };

  const onVolumeSliderChange = (e) => {
    const newVolume = parseFloat(e.target.value);
    if (isMuted) setIsMuted(false);
    if (newVolume === 0) setIsMuted(true);
    handleVolumeChange(newVolume);
  };

  const formatTime = (seconds) => {
    if (!seconds || isNaN(seconds)) return '0:00';
    const mins = Math.floor(seconds / 60);
    const secs = Math.floor(seconds % 60);
    return `${mins}:${secs.toString().padStart(2, '0')}`;
  };

  const effectiveVolume = isMuted ? 0 : volume;

  if (!currentSong) return null;

  return (
    <>
      {/* Player ẩn, chỉ lấy âm thanh và logic thời gian */}
      <ReactPlayer
        key={currentSong._id}
        ref={playerRef}
        url={`https://www.youtube.com/watch?v=${currentSong.url}`}
        playing={isPlaying}
        volume={effectiveVolume}
        onStart={handlePlayerStart}
        onBuffer={handlePlayerBuffer}
        onBufferEnd={handlePlayerBufferEnd}
        onProgress={onProgress}
        onDuration={onDuration}
        onEnded={handlePlayerEnded}
        width="0" height="0"
        config={{
          youtube: {
            playerVars: { showinfo: 0, origin: window.location.origin, enablejsapi: 1, rel: 0 }
          }
        }}
      />

      {/* Giao diện Controls */}
      <div className="p-4 border-b border-gray-700">
        <h3 className="text-xs text-gray-400 mb-3 uppercase tracking-wider">Now Playing</h3>
        
        {/* Disk Animation */}
        <div className="relative mb-3 mx-auto" style={{ width: '180px', height: '180px' }}>
          <div className={`w-full h-full relative ${isPlaying ? 'animate-spin-slow' : ''}`}>
            <div className="w-full h-full rounded-full overflow-hidden shadow-2xl">
              <img
                src={currentSong.image_url}
                alt={currentSong.track_name}
                className="w-full h-full object-cover"
              />
            </div>
            {/* Hiệu ứng đĩa than */}
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

        {/* Info */}
        <div className="text-center mb-3">
          <h2 className="text-base font-bold text-white mb-1 truncate">
            {currentSong.track_name}
          </h2>
          <p className="text-gray-400 text-xs truncate">{currentSong.artist_name}</p>
        </div>

        {/* Seek Bar (Local State) */}
        <div className="space-y-1">
          <input
            type="range"
            min="0"
            max="0.999999"
            step="any"
            value={played}
            onChange={onSeek}
            className="w-full h-1 bg-gray-700 rounded-lg appearance-none cursor-pointer accent-primary-500"
          />
          <div className="flex justify-between text-xs text-gray-400">
            <span>{formatTime(played * duration)}</span>
            <span>{formatTime(duration)}</span>
          </div>
        </div>

        {/* Buttons Controls */}
        <div className="flex items-center justify-between mt-3">
          {/* Volume */}
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

          {/* Main Controls */}
          <div className="flex items-center gap-2">
            <button onClick={playPrevious} className="p-2 hover:bg-gray-700 rounded-full transition-colors">
              <SkipBack className="w-5 h-5 text-gray-300" />
            </button>
            <button onClick={togglePlay} disabled={isSongLoading} className="p-3 bg-primary-500 hover:bg-primary-600 rounded-full transition-all hover:scale-110 shadow-lg">
              {isPlaying ? <Pause className="w-6 h-6 text-white fill-white" /> : <Play className="w-6 h-6 text-white fill-white" />}
            </button>
            <button onClick={playNext} className="p-2 hover:bg-gray-700 rounded-full transition-colors">
              <SkipForward className="w-5 h-5 text-gray-300" />
            </button>
          </div>

          {/* Toggle AutoPlay */}
          <button
            onClick={toggleAutoPlayNext}
            className={`p-2 rounded-full transition-colors ${autoPlayNext ? 'bg-primary-500 hover:bg-primary-600 text-white' : 'hover:bg-gray-700 text-gray-400'}`}
            title={autoPlayNext ? 'Tắt tự động phát tiếp' : 'Bật tự động phát tiếp'}
          >
            <ListMusic className="w-5 h-5" />
          </button>
        </div>
      </div>
    </>
  );
};

export default SidebarPlayer;