import { createContext, useContext, useState, useRef, useEffect, useMemo, useCallback } from 'react';
import { fetchRecommendations, fetchNextSongs } from '../services/recommendationService';
import { logUserEvent } from '../services/loggingService';
import { useAuth } from './AuthContext'; // Giả sử bạn đã tách Auth

const MusicContext = createContext();
const MUSIC_KEY_STORAGE = 'music_app_music';
const API_URL_SEND_LOG = 'http://localhost:8000/api/logs/event';

// Hàm helper đưa ra ngoài để tránh khởi tạo lại
const getSavedState = () => {
  try {
    const saved = localStorage.getItem(MUSIC_KEY_STORAGE);
    return saved ? JSON.parse(saved) : null;
  } catch (e) {
    console.error("Lỗi đọc localStorage", e);
    return null;
  }
};

export const useMusic = () => {
  const context = useContext(MusicContext);
  if (!context) {
    throw new Error('useMusic must be used within a MusicProvider');
  }
  return context;
};

export const MusicProvider = ({ children }) => {
  const savedState = getSavedState();
  const { user } = useAuth(); // Lấy user từ AuthContext

  // State phát nhạc
  const [isPlaying, setIsPlaying] = useState(false);
  const [currentSong, setCurrentSong] = useState(() => savedState ? savedState.currentSong : null);
  const [volume, setVolume] = useState(() => savedState ? savedState.volume : 0.5);
  const [playlist, setPlaylist] = useState(() => savedState ? savedState.playlist : []);
  const [currentIndex, setCurrentIndex] = useState(() => savedState ? savedState.currentIndex : 0);
  const [autoPlayNext, setAutoPlayNext] = useState(true);
  const [isSongLoading, setIsSongLoading] = useState(false);

  // Refs 
  const playerRef = useRef(null);
  const progressRef = useRef({ playedSeconds: 0, played: 0, loaded: 0 }); // Lưu tiến độ
  const durationRef = useRef(0); // Lưu tổng thời lượng
  const accumulatedTimeRef = useRef(0); // Tính thời gian nghe thực tế
  const lastStartTimeRef = useRef(0);

  // State cho giao diện feed và sidebar
  const [viewList, setViewList] = useState([]);
  const [isLoading, setIsLoading] = useState(true);
  const [viewHasMore, setViewHasMore] = useState(true);

  const [isSidebarLoading, setIsSidebarLoading] = useState(false);
  const [sidebarHasMore, setSidebarHasMore] = useState(true);

  const viewLimit = 20;
  const sidebarLimit = 15;

  // Logic lưu trạng thái phát nhạc vào localStorage
  useEffect(() => {
    // Lưu trạng thái bài hát hiện tại
    if (currentSong && playlist.length > 0) {
      const stateToSave = { playlist, currentSong, currentIndex, volume };
      localStorage.setItem(MUSIC_KEY_STORAGE, JSON.stringify(stateToSave));
    }
  }, [playlist, currentSong, currentIndex, volume]);

  // Logic load data
  const fetchHomeFeed = useCallback(async (refresh = false) => {
    if (!user) return;
    if (isLoading && !refresh) return;
    setIsLoading(true);
    try {
      const { songs, has_more } = await fetchRecommendations(user._id, viewLimit, refresh);
      if (refresh) {
        setViewList(songs);
      } else {
        setViewList(prev => [...prev, ...songs]);
      }
      setViewHasMore(has_more);
    } catch (err) {
      console.error("Lỗi lấy home feed:", err);
    } finally {
      setIsLoading(false);
    }
  }, [user, isLoading]);

  // Load nhạc khi user login
  useEffect(() => {
    if (user) {
      fetchHomeFeed(true);
    } else {
      setViewList([]);
      setPlaylist([]);
      setCurrentSong(null);
    }
  }, [user]);

  const fetchRecommendNextSongs = useCallback(async (currentSongId, refresh) => {
    if (isSidebarLoading) return;
    setIsSidebarLoading(true);
    try {
      const { next_songs, has_more } = await fetchNextSongs(user._id, currentSongId, sidebarLimit, refresh);
      setPlaylist(prev => [...prev, ...next_songs]);
      setSidebarHasMore(has_more);
    } catch (error) {
      console.error("Lỗi lấy bài tiếp theo:", error);
    } finally {
      setIsSidebarLoading(false);
    }
  }, [user, isSidebarLoading]);

  // Đồng bộ tiến độ và thời lượng từ player
  const handleUpdateProgress = useCallback((state) => {
    progressRef.current = state;
  }, []);

  const handleUpdateDuration = useCallback((duration) => {
    durationRef.current = duration;
  }, []);

  // Tạo log snapshot để gửi lên server Kafka
  const createLogSnapshot = useCallback((triggerType) => {
    const now = Date.now();
    let sessionTime = accumulatedTimeRef.current;
    if (lastStartTimeRef.current > 0) {
      sessionTime += (now - lastStartTimeRef.current);
    }
    const listenSeconds = sessionTime / 1000;
    if (listenSeconds < 2) return null;
    const totalSeconds = durationRef.current; // Lấy từ Ref

    const ratio = totalSeconds > 0 ? (listenSeconds / totalSeconds) : 0;
    let action = "listen";

    if (triggerType === 'auto_ended' || ratio >= 0.9) action = "complete";
    else if (ratio < 0.5) action = "skip";

    return {
      user_id: user?._id,
      track_id: currentSong?._id,
      action: action,
      duration: Math.floor(listenSeconds),
      total_duration: Math.floor(totalSeconds),
      timestamp: now,
      source: "real_user"
    };
  }, [user, currentSong]);

  const sendLog = async (snapshot) => {
    if (!snapshot) return;
    try {
      await logUserEvent(snapshot);
    } catch (e) {
      console.error("Lỗi gửi log:", e);
    }
  };

  // Cập nhật bộ đếm thời gian nghe
  const updateListenTimer = useCallback((state) => {
    const now = Date.now();
    if (state === 'play') {
      lastStartTimeRef.current = now;
    } else if (state === 'pause') {
      if (lastStartTimeRef.current) {
        accumulatedTimeRef.current += (now - lastStartTimeRef.current);
        lastStartTimeRef.current = 0;
      }
    } else if (state === 'reset') {
      accumulatedTimeRef.current = 0;
      lastStartTimeRef.current = 0;
    }
  }, []);

  // Các hàm điều khiển phát nhạc
  const playFromViewList = useCallback(async (song) => {
    let historyPlaylist = [];
    if (currentSong) {
      historyPlaylist = [currentSong]
      const snapshot = createLogSnapshot('switch');
      sendLog(snapshot);
    }
    setCurrentSong(song);
    setIsPlaying(true);
    setPlaylist([...historyPlaylist, song]);
    setCurrentIndex(historyPlaylist.length);
    updateListenTimer('reset');
    setIsSongLoading(true);
    try {
      await fetchRecommendNextSongs(song._id || song.id, true);
    } catch (e) { console.error(e); }
  }, [currentSong, createLogSnapshot, fetchRecommendNextSongs, updateListenTimer]);

  const playFromPlaylist = useCallback(async (index) => {
    if (currentSong) {
      const snapshot = createLogSnapshot('switch');
      sendLog(snapshot);
    }
    if (index >= 0 && index < playlist.length) {
      setCurrentIndex(index);
      setCurrentSong(playlist[index]);
      setIsPlaying(true);
      updateListenTimer('reset');
      setIsSongLoading(true);
    }
    const remainCount = playlist.length - index - 1;
    if (remainCount <= sidebarLimit / 2 && sidebarHasMore) {
      if (playlist[index]?._id) {
        fetchRecommendNextSongs(playlist[index]._id, false);
      }
    }
  }, [currentSong, playlist, createLogSnapshot, updateListenTimer]);

  const togglePlay = useCallback(() => {
    if (isPlaying) {
      updateListenTimer('pause');
    } else {
      updateListenTimer('play');
    }
    setIsPlaying(prev => !prev);
  }, [isPlaying, updateListenTimer]);

  const playNext = useCallback(async (trigger = 'manual') => {
    if (currentSong) {
      const snapshot = createLogSnapshot(trigger === 'auto' ? 'auto_ended' : 'skip');
      sendLog(snapshot);
    }

    const nextIndex = (currentIndex + 1) % playlist.length;
    setCurrentIndex(nextIndex);
    setCurrentSong(playlist[nextIndex]);
    setIsPlaying(true);
    updateListenTimer('reset');
    setIsSongLoading(true);

    // Fetch more if near end
    const remainCount = playlist.length - nextIndex - 1;
    if (remainCount <= sidebarLimit / 2 && sidebarHasMore) {
      if (playlist[nextIndex]?._id) {
        fetchRecommendNextSongs(playlist[nextIndex]._id, false);
      }
    }
  }, [currentIndex, playlist, currentSong, sidebarHasMore, createLogSnapshot, fetchRecommendNextSongs, updateListenTimer]);

  const playPrevious = useCallback(async () => {
    if (currentSong) {
      const snapshot = createLogSnapshot('skip');
      sendLog(snapshot);
    }
    if (currentIndex === 0) return;

    const prevIndex = currentIndex - 1;
    setCurrentIndex(prevIndex);
    setCurrentSong(playlist[prevIndex]);
    setIsPlaying(true);
    updateListenTimer('reset');
    setIsSongLoading(true);
  }, [currentIndex, playlist, currentSong, createLogSnapshot, updateListenTimer]);

  const handlePlayerStart = useCallback(() => {
    console.log("Audio bắt đầu phát - Bắt đầu tính giờ");
    setIsSongLoading(false);
    updateListenTimer('play');
  }, [updateListenTimer]);

  const handlePlayerBuffer = useCallback(() => {
    console.log("Buffering... Tạm dừng tính giờ");
    updateListenTimer('pause');
    setIsSongLoading(true);
  }, [updateListenTimer]);

  const handlePlayerBufferEnd = useCallback(() => {
    console.log("Buffer xong - Tiếp tục tính giờ");
    updateListenTimer('play');
    setIsSongLoading(false);
  }, [updateListenTimer]);

  const handlePlayerEnded = useCallback(() => {
    if (autoPlayNext) {
      playNext('auto');
    } else {
      setIsPlaying(false);
      updateListenTimer('pause');
    }
  }, [autoPlayNext, playNext, updateListenTimer]);

  const handleVolumeChange = useCallback((val) => {
    setVolume(val);
  }, []);

  const toggleAutoPlayNext = useCallback(() => {
    setAutoPlayNext(prev => !prev);
  }, []);

  const cleanupMusicSession = useCallback(async () => {
    if (currentSong) {
      const snapshot = createLogSnapshot('logout');
      console.log("Đang gửi log cuối trước khi logout...", snapshot);
      await sendLog(snapshot);
    }
    setIsPlaying(false);
    setCurrentSong(null);
    setCurrentIndex(0);
    setVolume(0.5);
    setPlaylist([]);
    setViewList([]);
    setAutoPlayNext(true);
    updateListenTimer('reset');
    // Reset các Ref
    durationRef.current = 0;
    progressRef.current = { playedSeconds: 0, played: 0 };
    accumulatedTimeRef.current = 0;
    lastStartTimeRef.current = 0;
  }, [currentSong, createLogSnapshot, sendLog]); // Dependencies

  useEffect(() => {
    const handleBeforeUnload = (event) => {
      if (currentSong && user) {

        const payload = createLogSnapshot('unload');
        if (!payload) return;

        // sendBeacon cho phép trình duyệt gửi dữ liệu ngầm ngay cả khi trang đã đóng
        const blob = new Blob([JSON.stringify(payload)], { type: 'application/json' });
        const success = navigator.sendBeacon(API_URL_SEND_LOG, blob);

        if (success) {
          console.log("Đã gửi log thoát trang thành công");
        }
      }
    };
    window.addEventListener('beforeunload', handleBeforeUnload);
    return () => {
      window.removeEventListener('beforeunload', handleBeforeUnload);
    };
  }, [currentSong, user, createLogSnapshot]);

  // MEMO GIÁ TRỊ CUNG CẤP CHO CONTEXT
  const value = useMemo(() => ({
    // Trạng thái phát nhạc
    currentSong,
    isPlaying,
    isSongLoading, // [MỚI] Trạng thái đang tải nhạc
    volume,
    playlist,
    currentIndex,
    autoPlayNext,

    // Giao diện feed và sidebar
    viewList,
    isLoading,
    viewHasMore,
    isSidebarLoading,
    sidebarHasMore,

    // Các hành động load data
    fetchHomeFeed,
    fetchRecommendNextSongs,

    // Các chức năng điều khiển phát nhạc
    playFromViewList,
    playFromPlaylist,
    togglePlay,
    playNext,
    playPrevious,
    handleVolumeChange,
    toggleAutoPlayNext,

    // Các sự kiện của player 
    handlePlayerStart,
    handlePlayerBuffer,
    handlePlayerBufferEnd,
    handlePlayerEnded,

    // Refs và handlers để đồng bộ với ReactPlayer
    playerRef,
    handleUpdateProgress,
    handleUpdateDuration,

    // Sự kiện cleanup khi logout (gửi log cuối cùng)
    cleanupMusicSession

  }), [
    currentSong,
    isPlaying,
    isSongLoading,
    volume,
    playlist,
    currentIndex,
    autoPlayNext,
    viewList,
    isLoading,
    viewHasMore,
    isSidebarLoading,
    sidebarHasMore,
    fetchHomeFeed,
    fetchRecommendNextSongs,
    playFromViewList,
    playFromPlaylist,
    togglePlay,
    playNext,
    playPrevious,
    handleVolumeChange,
    toggleAutoPlayNext,
    handlePlayerEnded,
    handlePlayerStart,
    handlePlayerBuffer,
    handlePlayerBufferEnd,
    handleUpdateProgress,
    handleUpdateDuration,
    cleanupMusicSession
  ]);

  return (
    <MusicContext.Provider value={value}>
      {children}
    </MusicContext.Provider>
  );
};