import { MusicProvider, useMusic } from './context/MusicContext';
import { AuthProvider, useAuth } from './context/AuthContext';
import UserSelection from './pages/UserSelection';
import Dashboard from './pages/Dashboard';

function AppContent() {
  const { user } = useAuth();

  return (
    <div className="min-h-screen bg-gray-900">
      {!user ? <UserSelection /> : <Dashboard />}
    </div>
  );
}

function App() {
  return (
    <AuthProvider>
      <MusicProvider>
        <AppContent />
      </MusicProvider>
    </AuthProvider>
  );
}

export default App;
