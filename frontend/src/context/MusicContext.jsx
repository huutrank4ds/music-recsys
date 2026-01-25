import { createContext, useContext, useState, useRef, useEffect } from 'react';
import { fetchRecommendations } from '../services/recommendationService';

const MusicContext = createContext();
const USER_KEY_STORAGE = 'music_app_user';
const MUSIC_KEY_STORAGE = 'music_app_music';


export const useMusic = () => {
  const context = useContext(MusicContext);
  if (!context) {
    throw new Error('useMusic must be used within a MusicProvider');
  }
  return context;
};

export const MusicProvider = ({ children }) => {

  const getSavedState = () => {
    try {
      const saved = localStorage.getItem(MUSIC_KEY_STORAGE);
      return saved ? JSON.parse(saved) : null;
    } catch (e) {
      console.error("Lỗi đọc localStorage", e);
      return null;
    }
  };
  const savedState = getSavedState();
  
  const [currentUser, setCurrentUser] = useState(() => {
    const savedUser = localStorage.getItem(USER_KEY_STORAGE);
    return savedUser ? JSON.parse(savedUser) : null;
  });
  const [currentSong, setCurrentSong] = useState(() => {
    return savedState ? savedState.currentSong : null;
  });
  const [isPlaying, setIsPlaying] = useState(false);
  const [viewList, setViewList] = useState([]);
  const [playlist, setPlaylist] = useState(() => {
    return savedState ? savedState.playlist : [];
  });
  const [currentIndex, setCurrentIndex] = useState(() => {
    return savedState ? savedState.currentIndex : 0;
  });
  const [volume, setVolume] = useState(() => {
    return savedState ? savedState.volume : 0.5;
  });
  const [played, setPlayed] = useState(0);
  const [duration, setDuration] = useState(0);
  const [autoPlayNext, setAutoPlayNext] = useState(true);
  const [isLoading, setIsLoading] = useState(true);
  const playerRef = useRef(null);

  useEffect(() => {
    if (currentUser) {
      const loadDataOnRefresh = async () => {
        setIsLoading(true);
        try {
          const recommendedSongs = await fetchRecommendations(currentUser.id);
          setViewList(recommendedSongs);
        } catch (error) {
          console.error("Lỗi tải lại nhạc:", error);
        } finally {
          setIsLoading(false);
        }
      };
      loadDataOnRefresh();
    }
  }, []);

  useEffect(() => {
    if (currentSong && playlist.length > 0) {
      const stateToSave = {
        playlist,
        currentSong,
        currentIndex,
        volume
      };
      localStorage.setItem(MUSIC_KEY_STORAGE, JSON.stringify(stateToSave));
    }
  }, [playlist, currentSong, currentIndex, volume]);

  const login = async (user) => {
    setIsLoading(true);
    localStorage.setItem(USER_KEY_STORAGE, JSON.stringify(user));
    setCurrentUser(user);
    try {
      const recommendedSongs = await fetchRecommendations(user.id);
      setViewList(recommendedSongs);
    } catch (error) {
      console.error("Failed to load playlist", error);
      setViewList([]);
    } finally {
      setIsLoading(false);
    }
  };

  const logout = () => {
    localStorage.removeItem(USER_KEY_STORAGE); // Xóa user đã lưu
    localStorage.removeItem(MUSIC_KEY_STORAGE); // Xóa trạng thái nhạc đã lưu
    setCurrentUser(null);
    setCurrentSong(null);
    setIsPlaying(false);
    setPlaylist([]);
  };

  const playFromViewList = async (song) => {
    let history = [];
    if (currentSong) {
        history = [currentSong];
    }
    setPlayed(0);
    setCurrentSong(song);
    setIsPlaying(true);
    const newIndex = history.length; 
    setCurrentIndex(newIndex);
    const tempQueue = [...history, song];
    setPlaylist(tempQueue);
    try {
        const songId = song._id || song.id;
        const recommendations = await fetchRecommendations(songId);
        const filteredRecs = recommendations.filter(s => (s._id || s.id) !== songId);
        setPlaylist([...history, song, ...filteredRecs]);
        
    } catch (error) {
        console.error("Lỗi lấy gợi ý:", error);
    }
  };

  const playFromPlaylist = (index) => {
    if (index >= 0 && index < playlist.length) {
      setCurrentIndex(index);
      setCurrentSong(playlist[index]);
      setPlayed(0);
      setIsPlaying(true);
    }
  };

  const togglePlay = () => {
    setIsPlaying(!isPlaying);
  };

  const playNext = () => {
    const nextIndex = (currentIndex + 1) % playlist.length;
    setCurrentIndex(nextIndex);
    setPlayed(0);
    setCurrentSong(playlist[nextIndex]);
    setIsPlaying(true);
  };

  const playPrevious = () => {
    if (currentIndex === 0) {
      return;
    }
    const prevIndex = currentIndex === 0 ? playlist.length - 1 : currentIndex - 1;
    setCurrentIndex(prevIndex);
    setCurrentSong(playlist[prevIndex]);
    setIsPlaying(true);
  };

  const handleProgress = (state) => {
    setPlayed(state.played);
  };

  const handleDuration = (dur) => {
    setDuration(dur);
  };

  const handleSeek = (value) => {
    const seekTo = parseFloat(value);
    setPlayed(seekTo);
    playerRef.current?.seekTo(seekTo);
  };

  const handleVolumeChange = (value) => {
    setVolume(parseFloat(value));
  };

  const handleEnded = () => {
    if (autoPlayNext) {
      playNext();
    } else {
      setIsPlaying(false);
    }
  };

  const toggleAutoPlayNext = () => {
    setAutoPlayNext(!autoPlayNext);
  };

  const value = {
    currentUser,
    currentSong,
    isPlaying,
    viewList,
    playlist,
    currentIndex,
    volume,
    played,
    duration,
    autoPlayNext,
    playerRef,
    isLoading,
    login,
    logout,
    playFromViewList,
    playFromPlaylist,
    togglePlay,
    playNext,
    playPrevious,
    handleProgress,
    handleDuration,
    handleSeek,
    handleVolumeChange,
    handleEnded,
    toggleAutoPlayNext,
  };

  return (
    <MusicContext.Provider value={value}>
      {children}
    </MusicContext.Provider>
  );
};
