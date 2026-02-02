// src/App.jsx
import { useState } from 'react';
import { QueryClient, QueryClientProvider } from '@tanstack/react-query';
import { RefreshCw } from 'lucide-react';
import DashboardFilters from './components/filters/DashboardFilters';
import KPIsSection from './components/kpis/KPIsSection';
import SentimentTrendChart from './components/charts/SentimentTrendChart';
import FixFirstRanking from './components/charts/FixFirstRanking';
import AlertsFeed from './components/alerts/AlertsFeed';
import ChannelBreakdown from './components/charts/ChannelBreakdown';
import WordCloud from './components/charts/WordCloud';
import DatabaseView from './components/crud/DatabaseView';

// Create QueryClient with auto-refresh configuration
const queryClient = new QueryClient({
  defaultOptions: {
    queries: {
      refetchInterval: 30000, // Refetch every 30 seconds
      refetchIntervalInBackground: false,
      staleTime: 20000,
    },
  },
});

function App() {
  const [filters, setFilters] = useState({
    timeWindow: '7d',
    productId: null,
    channel: null,
  });

  const [activeTab, setActiveTab] = useState('dashboard'); // 'dashboard' or 'database'

  const handleFilterChange = (newFilters) => {
    setFilters((prev) => ({ ...prev, ...newFilters }));
  };

  return (
    <QueryClientProvider client={queryClient}>
      <div className="min-h-screen">
        {/* Header */}
        <header className="glass-header sticky top-0 z-50">
          <div className="max-w-7xl mx-auto px-4 sm:px-6 lg:px-8 py-8">
            <div className="flex items-center justify-between">
              <div>
                <h1 className="text-4xl font-bold text-white mb-2 tracking-tight drop-shadow-md">
                  Real Time customer sentiment analysis
                </h1>
                <p className="text-base text-slate-300 font-medium">
                  Sentiment & churn insights • product and channel analytics
                </p>
              </div>
              <div className="flex items-center gap-4 px-6 py-3 bg-gradient-to-r from-blue-500/20 via-purple-500/20 to-pink-500/20 backdrop-blur-md rounded-2xl border-2 border-blue-400/40 shadow-lg">
                <RefreshCw className="w-5 h-5 animate-spin text-cyan-300" />
                <div className="flex flex-col">
                  <span className="text-xs text-cyan-200 font-medium">Live Updates</span>
                  <span className="text-sm text-white font-bold">Every 30s</span>
                </div>
              </div>
            </div>

            {/* Tab Navigation */}
            <div className="flex gap-4 mt-6">
              <button
                onClick={() => setActiveTab('dashboard')}
                className={`px-6 py-3 rounded-xl font-semibold transition-all ${activeTab === 'dashboard'
                  ? 'bg-gradient-to-r from-blue-500 to-cyan-600 text-white shadow-lg'
                  : 'bg-slate-800/50 text-slate-300 hover:bg-slate-700/50'
                  }`}
              >
                📊 Dashboard
              </button>
              <button
                onClick={() => setActiveTab('database')}
                className={`px-6 py-3 rounded-xl font-semibold transition-all ${activeTab === 'database'
                  ? 'bg-gradient-to-r from-green-500 to-emerald-600 text-white shadow-lg'
                  : 'bg-slate-800/50 text-slate-300 hover:bg-slate-700/50'
                  }`}
              >
                🗄️ Database View
              </button>
            </div>
          </div>
        </header>

        {/* Main Content */}
        <main className="max-w-7xl mx-auto px-4 sm:px-6 lg:px-8 py-8">
          {activeTab === 'dashboard' ? (
            <>
              {/* Filters */}
              <div className="mb-8">
                <DashboardFilters filters={filters} onFilterChange={handleFilterChange} />
              </div>

              {/* KPIs Section */}
              <div className="mb-8">
                <KPIsSection filters={filters} />
              </div>

              {/* Charts Grid */}
              <div className="grid grid-cols-1 lg:grid-cols-2 gap-8 mt-8">
                {/* Sentiment Trend */}
                <div className="lg:col-span-2 transform transition-all duration-300 hover:scale-[1.01]">
                  <SentimentTrendChart filters={filters} />
                </div>

                {/* Fix-First Ranking */}
                <div className="lg:col-span-2 transform transition-all duration-300 hover:scale-[1.01]">
                  <FixFirstRanking filters={filters} />
                </div>

                {/* Channel Breakdown */}
                <div className="transform transition-all duration-300 hover:scale-[1.02]">
                  <ChannelBreakdown filters={filters} />
                </div>

                {/* Word Cloud - Top Negative Keywords */}
                <div className="transform transition-all duration-300 hover:scale-[1.02]">
                  <WordCloud filters={filters} />
                </div>
              </div>

              {/* Alerts Feed */}
              <div className="mt-8 transform transition-all duration-300 hover:scale-[1.01]">
                <AlertsFeed filters={filters} />
              </div>
            </>
          ) : activeTab === 'database' ? (
            /* Database View Tab */
            <DatabaseView />
          ) : null}
        </main>

        {/* Footer */}
        <footer className="glass-header mt-16">
          <div className="max-w-7xl mx-auto px-4 sm:px-6 lg:px-8 py-6">
            <p className="text-center text-sm text-white font-medium drop-shadow-lg">
              © 2026 ITD Sentiment Dashboard | Phase 7B | Real-time Analytics ✨
            </p>
          </div>
        </footer>
      </div>
    </QueryClientProvider>
  );
}

export default App;
