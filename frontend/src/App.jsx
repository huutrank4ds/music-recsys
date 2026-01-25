import { MusicProvider, useMusic } from './context/MusicContext';
import UserSelection from './pages/UserSelection';
import Dashboard from './pages/Dashboard';

function AppContent() {
  const { currentUser } = useMusic();

  return (
    <div className="min-h-screen bg-gray-900">
      {!currentUser ? <UserSelection /> : <Dashboard />}
    </div>
  );
}

function App() {
  return (
    <MusicProvider>
      <AppContent />
    </MusicProvider>
  );
}

export default App;
