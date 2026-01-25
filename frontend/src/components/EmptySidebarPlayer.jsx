import { Play, SkipForward, SkipBack, Volume2, ListMusic, Music } from 'lucide-react';

const EmptySidebarPlayer = () => {
  return (
    <div className="fixed right-0 top-0 w-80 h-screen bg-gray-800 border-l border-gray-700 flex flex-col z-20 hidden lg:flex">
      {/* --- Now Playing Section (Empty State) --- */}
      <div className="p-4 border-b border-gray-700">
        <h3 className="text-xs text-gray-500 mb-3 uppercase tracking-wider">Now Playing</h3>
        
        {/* Vinyl Record Placeholder (Màu xám, không xoay) */}
        <div className="relative mb-3 mx-auto" style={{ width: '180px', height: '180px' }}>
          <div className="w-full h-full relative">
            {/* Vòng tròn nền xám */}
            <div className="w-full h-full rounded-full bg-gray-700 flex items-center justify-center shadow-inner">
              <Music className="w-16 h-16 text-gray-500 opacity-50" />
            </div>
            
            {/* Hiệu ứng vân đĩa (Giữ nguyên để đồng bộ style) */}
            <div className="absolute inset-0 rounded-full" style={{
              background: 'radial-gradient(circle, transparent 20%, rgba(0,0,0,0.2) 20%, rgba(0,0,0,0.2) 22%, transparent 22%, transparent 40%, rgba(0,0,0,0.1) 40%)',
              pointerEvents: 'none'
            }}></div>
            
            {/* Lỗ tròn ở giữa */}
            <div className="absolute inset-0 flex items-center justify-center pointer-events-none">
              <div className="w-12 h-12 bg-gray-800 rounded-full shadow-2xl border-4 border-gray-700"></div>
            </div>
          </div>
        </div>

        {/* Song Info Placeholder */}
        <div className="text-center mb-3">
          <h2 className="text-base font-bold text-gray-500 mb-1">
            Chưa chọn bài hát
          </h2>
          <p className="text-gray-600 text-xs">Hãy chọn một bài để bắt đầu</p>
        </div>

        {/* Progress Bar Disabled */}
        <div className="space-y-1">
          <div className="w-full h-1 bg-gray-700 rounded-lg overflow-hidden">
            <div className="h-full bg-gray-600 w-0"></div>
          </div>
          <div className="flex justify-between text-xs text-gray-600">
            <span>--:--</span>
            <span>--:--</span>
          </div>
        </div>

        {/* Controls Disabled (Màu tối hơn, không click được) */}
        <div className="flex items-center justify-between mt-3 opacity-50 pointer-events-none select-none">
          {/* Volume */}
          <button className="p-2">
            <Volume2 className="w-5 h-5 text-gray-600" />
          </button>

          {/* Play Controls */}
          <div className="flex items-center gap-2">
            <button className="p-2">
              <SkipBack className="w-5 h-5 text-gray-600" />
            </button>

            <button className="p-3 bg-gray-700 rounded-full shadow-none">
              <Play className="w-6 h-6 text-gray-500 fill-gray-500" />
            </button>

            <button className="p-2">
              <SkipForward className="w-5 h-5 text-gray-600" />
            </button>
          </div>

          {/* Autoplay Toggle */}
          <button className="p-2">
            <ListMusic className="w-5 h-5 text-gray-600" />
          </button>
        </div>
      </div>

      {/* --- Next Up Section (Empty State) --- */}
      <div className="p-4 flex flex-col flex-1 overflow-hidden">
        <div className="flex items-center justify-between mb-3">
          <h3 className="text-xs text-gray-500 uppercase tracking-wider">Gợi ý tiếp theo</h3>
          <span className="text-xs text-gray-600">0 bài</span>
        </div>
        
        {/* Empty List Placeholder */}
        <div className="flex flex-col items-center justify-center h-40 text-gray-600 space-y-2 opacity-50">
          <ListMusic className="w-8 h-8 mb-2" />
          <p className="text-xs text-center">Danh sách phát đang trống</p>
        </div>
      </div>
    </div>
  );
};

export default EmptySidebarPlayer;