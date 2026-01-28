import React from 'react';
import SidebarPlayer from './SidebarPlayer';
import SidebarPlaylist from './SidebarPlaylist';
import { useMusic } from '../context/MusicContext';

const RightSidebar = () => {
  const { currentSong } = useMusic();

  // Ẩn sidebar nếu chưa có bài hát nào được chọn
  if (!currentSong) return null;

  return (
    <div className="fixed right-0 top-0 w-80 h-screen bg-gray-800 border-l border-gray-700 flex flex-col z-20 hidden lg:flex">
      {/* 1. Player & Controls (Height cố định theo nội dung) */}
      <SidebarPlayer />

      {/* 2. Playlist (Chiếm phần còn lại và scroll) */}
      <SidebarPlaylist />
    </div>
  );
};

export default RightSidebar;