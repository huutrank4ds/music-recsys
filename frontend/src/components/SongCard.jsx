import { Play } from 'lucide-react';
import { useMusic } from '../context/MusicContext';

const SongCard = ({ song }) => {
  const { playFromViewList, currentSong } = useMusic();
  const isCurrentSong = currentSong?._id === song._id;

  const handlePlay = () => {
    playFromViewList(song);
    console.log('Playing song:', song);
  };

  return (
    <div className="group relative bg-gray-800 rounded-lg p-4 hover:bg-gray-700 transition-all duration-300 cursor-pointer">
      <div className="relative aspect-square mb-4 overflow-hidden rounded-md">
        <img
          src={song.image_url}
          alt={song.track_name}
          className="w-full h-full object-cover group-hover:scale-110 transition-transform duration-300"
        />
        
        {/* Play Button Overlay */}
        <div
          onClick={handlePlay}
          className={`absolute inset-0 bg-black bg-opacity-50 flex items-center justify-center transition-opacity duration-300 ${
            isCurrentSong ? 'opacity-100' : 'opacity-0 group-hover:opacity-100'
          }`}
        >
          <button className="bg-primary-500 rounded-full p-4 hover:bg-primary-600 hover:scale-110 transition-all duration-300 shadow-lg">
            <Play className="w-8 h-8 text-white fill-white" />
          </button>
        </div>

        {/* Current Playing Indicator */}
        {isCurrentSong && (
          <div className="absolute top-2 right-2 bg-primary-500 rounded-full p-2">
            <div className="w-2 h-2 bg-white rounded-full animate-pulse"></div>
          </div>
        )}
      </div>

      <div className="space-y-1">
        <h3 className="font-semibold text-white truncate group-hover:text-primary-400 transition-colors">
          {song.track_name}
        </h3>
        <p className="text-sm text-gray-400 truncate">{song.artist_name}</p>
        <p className="text-xs text-gray-500">
          {/* Tính phút */}
          {Math.floor((song.duration || 0) / 60)}:
          {/* Tính giây còn dư và thêm số 0 đằng trước */}
          {String(Math.floor((song.duration || 0) % 60)).padStart(2, '0')}
        </p>
      </div>
    </div>
  );
};

export default SongCard;
