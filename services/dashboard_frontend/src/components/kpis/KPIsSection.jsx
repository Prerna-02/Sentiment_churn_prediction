// src/components/kpis/KPIsSection.jsx
import { useState } from 'react';
import { useQuery } from '@tanstack/react-query';
import { 
  Activity, 
  TrendingDown, 
  Target, 
  Shield, 
  AlertTriangle,
  MessageSquare,
  Info
} from 'lucide-react';
import { dashboardAPI } from '../../api/dashboard';
import QueryModal from '../common/QueryModal';
import { MONGO_QUERIES } from '../../data/mongoQueries';

function KPICard({ title, value, subtitle, icon: Icon, color = 'blue', loading = false, onInfoClick }) {
  const colorClasses = {
    blue: 'bg-blue-100 text-blue-600',
    red: 'bg-red-100 text-red-600',
    green: 'bg-green-100 text-green-600',
    yellow: 'bg-yellow-100 text-yellow-600',
    purple: 'bg-purple-100 text-purple-600',
    gray: 'bg-gray-100 text-gray-600',
  };

  if (loading) {
    return (
      <div className="bg-white rounded-lg shadow-md p-6 animate-pulse">
        <div className="h-12 bg-gray-200 rounded mb-2"></div>
        <div className="h-8 bg-gray-200 rounded mb-2"></div>
        <div className="h-4 bg-gray-200 rounded w-2/3"></div>
      </div>
    );
  }

  return (
    <div className="kpi-card relative group">
      {/* Info Icon */}
      {onInfoClick && (
        <button
          onClick={onInfoClick}
          className="absolute top-4 right-4 p-2 text-white/60 hover:text-white hover:bg-gradient-to-r hover:from-blue-500/30 hover:to-purple-500/30 rounded-xl transition-all opacity-0 group-hover:opacity-100 z-10 border border-white/20"
          title="View MongoDB Query"
        >
          <Info className="w-5 h-5" />
        </button>
      )}
      
      <div className="flex items-center justify-between">
        <div className="flex-1">
          <p className="text-sm font-bold text-cyan-300 mb-3 uppercase tracking-wider drop-shadow-lg">{title}</p>
          <p className="text-5xl font-black text-white mb-3 drop-shadow-2xl" style={{
            textShadow: '0 0 20px rgba(59, 130, 246, 0.5), 0 0 40px rgba(147, 51, 234, 0.3)'
          }}>{value}</p>
          <p className="text-sm text-cyan-100 font-semibold">{subtitle}</p>
        </div>
        <div className={`p-5 rounded-3xl ${colorClasses[color]} backdrop-blur-sm shadow-2xl transform transition-all group-hover:scale-125 group-hover:rotate-12 border-2 border-white/30`}
          style={{
            boxShadow: '0 0 30px rgba(59, 130, 246, 0.4), 0 0 60px rgba(147, 51, 234, 0.2)'
          }}>
          <Icon className="w-8 h-8" />
        </div>
      </div>
    </div>
  );
}

function KPIsSection({ filters }) {
  const [modalOpen, setModalOpen] = useState(false);
  const [selectedQuery, setSelectedQuery] = useState(null);

  const { data, isLoading, isError } = useQuery({
    queryKey: ['kpis', filters],
    queryFn: () => dashboardAPI.getKPIs(filters),
  });

  const handleInfoClick = () => {
    setSelectedQuery(MONGO_QUERIES.totalReviews);
    setModalOpen(true);
  };

  if (isError) {
    return (
      <div className="bg-red-50 border border-red-200 rounded-lg p-4 mb-6">
        <p className="text-red-800">Failed to load KPIs. Please try again.</p>
      </div>
    );
  }

  const kpis = [
    {
      title: 'Total Reviews',
      value: isLoading ? '—' : data?.total_reviews?.toLocaleString() || '0',
      subtitle: filters.timeWindow === 'all' ? 'All time' : `Last ${filters.timeWindow}`,
      icon: MessageSquare,
      color: 'blue',
    },
    {
      title: 'Negative Reviews',
      value: isLoading ? '—' : `${data?.negative_percentage?.toFixed(1) || '0'}%`,
      subtitle: 'Percentage of total',
      icon: TrendingDown,
      color: 'red',
    },
    {
      title: 'Avg Sentiment Score',
      value: isLoading ? '—' : data?.avg_sentiment_score?.toFixed(2) || '0.00',
      subtitle: 'Range: -1 (negative) to +1 (positive)',
      icon: Activity,
      color: data?.avg_sentiment_score >= 0 ? 'green' : 'red',
    },
    {
      title: 'Avg Confidence',
      value: isLoading ? '—' : data?.avg_confidence?.toFixed(2) || '0.00',
      subtitle: 'Model prediction confidence',
      icon: Shield,
      color: 'purple',
    },
    {
      title: 'High-Risk Products',
      value: isLoading ? '—' : data?.high_risk_products?.toLocaleString() || '0',
      subtitle: 'Products with >50% negative',
      icon: AlertTriangle,
      color: 'yellow',
    },
    {
      title: 'Alerts Triggered',
      value: isLoading ? '—' : data?.alerts_triggered?.toLocaleString() || '0',
      subtitle: 'Last 24 hours',
      icon: Target,
      color: 'gray',
    },
  ];

  return (
    <>
      <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-6">
        {kpis.map((kpi, idx) => (
          <KPICard 
            key={idx} 
            {...kpi} 
            loading={isLoading} 
            onInfoClick={handleInfoClick}
          />
        ))}
      </div>

      {/* MongoDB Query Modal */}
      {selectedQuery && (
        <QueryModal
          isOpen={modalOpen}
          onClose={() => setModalOpen(false)}
          title={selectedQuery.title}
          query={selectedQuery.query}
          executionTime={selectedQuery.executionTime}
          explanation={selectedQuery.explanation}
        />
      )}
    </>
  );
}

export default KPIsSection;

