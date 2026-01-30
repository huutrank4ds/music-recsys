import { MusicProvider, useMusic } from './context/MusicContext';
import { AuthProvider, useAuth } from './context/AuthContext';
import UserSelection from './pages/UserSelection';
import Dashboard from './pages/Dashboard';
import { Loader2 } from 'lucide-react';

function AppContent() {
  const { user, isCheckingAuth } = useAuth();

  if (isCheckingAuth) {
    return (
      <div className="min-h-screen bg-gray-900 flex items-center justify-center">
        <div className="text-center">
          <Loader2 className="w-12 h-12 text-primary-500 animate-spin mx-auto mb-4" />
          <p className="text-gray-400">Đang khởi động...</p>
        </div>
      </div>
    );
  }
  
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
