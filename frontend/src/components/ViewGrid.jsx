import React from 'react';
import { Loader2 } from 'lucide-react';
import SongCard from './SongCard';
import { useMusic } from '../context/MusicContext';

const ViewGrid = ({ viewList, isContextLoading }) => {
    const { currentSong } = useMusic();

    const gridClass = currentSong 
        ? "grid-cols-2 md:grid-cols-3 xl:grid-cols-4"       // Chế độ hẹp (Có Sidebar)
        : "grid-cols-2 md:grid-cols-3 lg:grid-cols-4 xl:grid-cols-5"; // Chế độ rộng (Full màn hình
    return (
    <>
        <div className="mb-6 mt-4 ml-4">
        <h2 className="text-xl font-bold text-white mb-2">Recommended for You</h2>
        </div>

        <div className={`grid ${gridClass} gap-6 pb-20 px-4`}>
            {viewList.map((song) => (
            <SongCard key={song._id} song={song} />
            ))}
        </div>

        {isContextLoading && viewList.length > 0 && (
        <div className="py-4 flex justify-center w-full">
            <Loader2 className="w-8 h-8 animate-spin text-primary-500" />
        </div>
        )}
        
        {isContextLoading && viewList.length === 0 && <LoadingSpinner text="Analyzing your taste..." />}
        {!isContextLoading && viewList.length === 0 && <EmptyState message="No recommendations available." />}
    </>
  );
}

const LoadingSpinner = ({ text }) => (
  <div className="w-full h-64 flex flex-col items-center justify-center">
    <Loader2 className="w-10 h-10 animate-spin text-primary-500 mb-4" />
    <p className="text-gray-400 animate-pulse">{text}</p>
  </div>
);

const EmptyState = ({ message }) => (
  <div className="text-center py-20 bg-gray-800/30 rounded-xl border border-dashed border-gray-700 m-4">
    <p className="text-gray-400 text-lg">{message}</p>
  </div>
);

export default ViewGrid;