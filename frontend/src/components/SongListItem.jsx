// src/components/SongListItem.jsx
import { Play, Heart, Clock } from 'lucide-react';
import { useMusic } from '../context/MusicContext';

const SongListItem = ({ song, index }) => {
  const { playFromViewList, currentSong } = useMusic();
  const isPlaying = currentSong?._id === song._id;

  // Format milliseconds to mm:ss
  const formatDuration = (ms) => {
    if (!ms) return "00:00";
    const minutes = Math.floor(ms / 60000);
    const seconds = ((ms % 60000) / 1000).toFixed(0);
    return minutes + ":" + (seconds < 10 ? "0" : "") + seconds;
  };

  return (
    <div 
      onClick={() => playFromViewList(song)}
      className={`group flex items-center p-3 rounded-md cursor-pointer transition-colors ${
        isPlaying ? 'bg-primary-500/20 border border-primary-500/30' : 'hover:bg-gray-800'
      }`}
    >
      {/* Index or Icon */}
      <div className="w-10 text-center text-gray-400 group-hover:hidden">
        {index + 1}
      </div>
      <div className="w-10 hidden group-hover:flex justify-center text-primary-400">
        <Play className="w-4 h-4 fill-current" />
      </div>

      {/* Image */}
      <img 
        src={song.image_url || "/default-music.png"} 
        alt={song.track_name} 
        className="w-10 h-10 rounded object-cover mx-4"
      />

      {/* Info */}
      <div className="flex-1 min-w-0">
        <h4 className={`truncate font-medium ${isPlaying ? 'text-primary-400' : 'text-white'}`}>
          {song.track_name}
        </h4>
        <p className="truncate text-sm text-gray-400">{song.artist_name}</p>
      </div>

      {/* Duration (Hidden on mobile) */}
      <div className="hidden md:flex items-center text-gray-500 text-sm mr-4">
        <Clock className="w-3 h-3 mr-1" />
        {formatDuration(song.duration_ms)}
      </div>

      {/* Like Button (Placeholder) */}
      <button className="text-gray-500 hover:text-white p-2">
        <Heart className="w-4 h-4" />
      </button>
    </div>
  );
};

export default SongListItem;