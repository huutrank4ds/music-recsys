# 🎵 Music Recommendation Frontend - Quick Reference

## 📁 Complete File Structure

```
frontend/
├── 📄 package.json              ✅ Updated with all dependencies
├── 📄 tailwind.config.js        ✅ Tailwind configuration
├── 📄 postcss.config.js         ✅ PostCSS configuration
├── 📄 vite.config.js            ✅ Vite configuration (existing)
├── 📄 README.md                 ✅ Full documentation
├── 📄 SETUP.md                  ✅ Setup instructions
├── 📄 install.ps1               ✅ Installation script
│
├── src/
│   ├── 📄 main.jsx              ✅ Entry point (existing)
│   ├── 📄 App.jsx               ✅ Root component (updated)
│   ├── 📄 App.css               ✅ Custom styles (updated)
│   ├── 📄 index.css             ✅ Tailwind imports (updated)
│   │
│   ├── context/
│   │   └── 📄 MusicContext.jsx  ✅ Global state management
│   │
│   ├── data/
│   │   └── 📄 mockData.js       ✅ Users & songs data
│   │
│   ├── pages/
│   │   ├── 📄 UserSelection.jsx ✅ User selection screen
│   │   └── 📄 Dashboard.jsx     ✅ Main dashboard
│   │
│   └── components/
│       ├── 📄 SearchBar.jsx     ✅ Search component (existing)
│       ├── 📄 SongCard.jsx      ✅ Song card component
│       └── 📄 SidebarPlayer.jsx ✅ Music player
```

## 🚀 Quick Start Commands

```powershell
# Option 1: Use installation script
cd d:\Project\BigData\endterm\music-recsys
.\frontend\install.ps1

# Option 2: Manual installation
cd frontend
npm install
npm run dev
```

## 🎯 Key Components Overview

### 1. MusicContext (State Management)
**Location**: `src/context/MusicContext.jsx`
**Purpose**: Global state for user, songs, and player

**Key Functions**:
- `login(user)` - Set current user
- `logout()` - Clear user and reset state
- `playSong(song)` - Play a specific song
- `togglePlay()` - Play/pause current song
- `playNext()` - Skip to next song
- `playPrevious()` - Go to previous song
- `handleSeek(value)` - Seek to position
- `handleVolumeChange(value)` - Adjust volume

### 2. UserSelection (Page)
**Location**: `src/pages/UserSelection.jsx`
**Features**:
- 6 user profiles in grid
- Hover scale animation
- Border glow effect
- Avatar images from ui-avatars.com

### 3. Dashboard (Page)
**Location**: `src/pages/Dashboard.jsx`
**Features**:
- Welcome header with user name
- Search bar with real-time filtering
- Logout button
- Responsive song grid
- Integrates SongCard and SidebarPlayer

### 4. SongCard (Component)
**Location**: `src/components/SongCard.jsx`
**Features**:
- Album cover art
- Song title and artist
- Duration display
- Play button overlay on hover
- Current playing indicator
- Click to play

### 5. SidebarPlayer (Component)
**Location**: `src/components/SidebarPlayer.jsx`
**Features**:
- Fixed right sidebar (desktop)
- Spinning album art when playing
- YouTube player (hidden)
- Play/Pause/Next/Previous controls
- Progress bar (seekable)
- Volume slider with mute
- Next Up queue display

## 📦 Dependencies

### Production Dependencies
```json
{
  "react": "^19.2.0",
  "react-dom": "^19.2.0",
  "lucide-react": "^0.263.1",
  "react-player": "^2.13.0"
}
```

### Dev Dependencies
```json
{
  "vite": "^7.2.4",
  "tailwindcss": "^3.4.0",
  "postcss": "^8.4.32",
  "autoprefixer": "^10.4.16",
  "@vitejs/plugin-react": "^5.1.1"
}
```

## 🎨 Design System

### Colors
```javascript
Primary Green:   #22c55e (bg-primary-500)
Accent Purple:   #a855f7 (bg-accent-500)
Background Dark: #111827 (bg-gray-900)
Card Dark:       #1f2937 (bg-gray-800)
Text White:      #ffffff (text-white)
Text Gray:       #9ca3af (text-gray-400)
```

### Breakpoints
```css
Mobile:  < 640px  (grid-cols-2)
Tablet:  640px+   (md:grid-cols-3)
Desktop: 1024px+  (lg:grid-cols-4, sidebar visible)
```

## 🔄 Data Flow

```
UserSelection
    ↓ (user selected)
MusicContext.login(user)
    ↓
Dashboard renders
    ↓
User clicks SongCard
    ↓
MusicContext.playSong(song)
    ↓
SidebarPlayer shows and plays
    ↓
ReactPlayer streams YouTube audio
```

## 🎵 Mock Data Structure

### User Object
```javascript
{
  id: 1,
  name: "Alice",
  avatar: "https://ui-avatars.com/api/?name=Alice..."
}
```

### Song Object
```javascript
{
  id: "101",
  track_name: "Lạc Trôi",
  artist_name: "Sơn Tùng M-TP",
  image_url: "https://photo-resize-zmp3.zmdcdn.me/...",
  url: "Llw9Q6akRo4", // YouTube video ID
  duration_ms: 250000
}
```

## 🛠️ Common Tasks

### Add a New Song
Edit `src/data/mockData.js`:
```javascript
export const songs = [
  // ...existing songs
  {
    id: "113",
    track_name: "New Song",
    artist_name: "Artist Name",
    image_url: "https://...",
    url: "YouTube_Video_ID",
    duration_ms: 240000
  }
];
```

### Add a New User
Edit `src/data/mockData.js`:
```javascript
export const users = [
  // ...existing users
  {
    id: 7,
    name: "Frank",
    avatar: "https://ui-avatars.com/api/?name=Frank&background=ff0000&color=fff&size=200"
  }
];
```

### Change Color Theme
Edit `tailwind.config.js`:
```javascript
theme: {
  extend: {
    colors: {
      primary: {
        500: '#your-color-here',
      }
    }
  }
}
```

## 🐛 Troubleshooting

### Module not found
```bash
cd frontend
rm -rf node_modules package-lock.json
npm install
```

### Tailwind not working
- Check `index.css` has `@tailwind` directives
- Check `tailwind.config.js` content paths
- Restart dev server

### Player not working
- Check internet connection (YouTube API)
- Open browser console for errors
- Verify YouTube video IDs are valid

### Search not working
- Check `Dashboard.jsx` filteredSongs logic
- Verify searchQuery state updates
- Check console for errors

## 📝 Development Tips

1. **Hot Reload**: Changes auto-update in browser
2. **Console Errors**: Check browser DevTools (F12)
3. **State Debugging**: Add `console.log` in MusicContext
4. **Tailwind Classes**: Use VS Code IntelliSense
5. **Component Props**: Check prop types carefully

## 🚀 Next Steps

1. **Connect Backend API**:
   - Replace mockData with API calls
   - Add loading states
   - Handle errors

2. **Add Features**:
   - User playlists
   - Favorites/likes
   - Shuffle and repeat
   - Lyrics display

3. **Improve UX**:
   - Loading skeletons
   - Error boundaries
   - Toast notifications
   - Mobile optimization

## 📚 Resources

- [React Documentation](https://react.dev)
- [Tailwind CSS Docs](https://tailwindcss.com)
- [Lucide Icons](https://lucide.dev)
- [React Player](https://github.com/cookpete/react-player)

## ✨ Summary

**Total Files Created**: 11 files
**Total Lines of Code**: ~1000+ lines
**Time to Setup**: 2-3 minutes
**Features**: User selection, music player, search, queue

**Ready to use**: Just run `npm install` and `npm run dev`! 🎵
