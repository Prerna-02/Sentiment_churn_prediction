// src/components/charts/SentimentTrendChart.jsx
import { useState } from 'react';
import { useQuery } from '@tanstack/react-query';
import { LineChart, Line, XAxis, YAxis, CartesianGrid, Tooltip, Legend, ResponsiveContainer } from 'recharts';
import { TrendingUp, Info } from 'lucide-react';
import { dashboardAPI } from '../../api/dashboard';
import QueryModal from '../common/QueryModal';
import { MONGO_QUERIES } from '../../data/mongoQueries';

function SentimentTrendChart({ filters }) {
  const [modalOpen, setModalOpen] = useState(false);

  const { data, isLoading, isError } = useQuery({
    queryKey: ['sentimentTrend', filters],
    queryFn: () => dashboardAPI.getSentimentTrend(filters),
  });

  if (isError) {
    return (
      <div className="glass-card p-8">
        <h2 className="text-2xl font-bold text-white mb-4">Sentiment Trend</h2>
        <div className="text-red-300 font-medium bg-red-500/20 backdrop-blur-sm p-4 rounded-lg border border-red-400/30">
          Failed to load sentiment trend data.
        </div>
      </div>
    );
  }

  if (isLoading) {
    return (
      <div className="glass-card p-8">
        <h2 className="text-2xl font-bold text-white mb-4">Sentiment Trend</h2>
        <div className="h-64 bg-white/10 rounded-xl animate-pulse shimmer"></div>
      </div>
    );
  }

  const chartData = data?.data || [];

  return (
    <>
      <div className="glass-card p-8 relative group">
        {/* Info Icon */}
        <button
          onClick={() => setModalOpen(true)}
          className="absolute top-6 right-6 p-2 text-white/70 hover:text-white hover:bg-white/20 rounded-full transition-all opacity-0 group-hover:opacity-100 z-10"
          title="View MongoDB Query"
        >
          <Info className="w-5 h-5" />
        </button>

        <div className="flex items-center justify-between mb-6">
          <div className="flex items-center gap-3">
            <div className="p-3 bg-blue-500/30 backdrop-blur-sm rounded-xl">
              <TrendingUp className="w-6 h-6 text-white" />
            </div>
            <h2 className="text-2xl font-bold text-white drop-shadow-lg">Sentiment Trend</h2>
          </div>
          <div className="px-4 py-2 bg-white/20 backdrop-blur-md rounded-full border border-white/30">
            <span className="text-sm text-white font-medium">
              Granularity: {data?.granularity || 'auto'}
            </span>
          </div>
        </div>

      {chartData.length === 0 ? (
        <div className="h-64 flex items-center justify-center">
          <p className="text-white/70 font-medium">No data available for the selected time window</p>
        </div>
      ) : (
        <div className="chart-container">
          <ResponsiveContainer width="100%" height={320}>
            <LineChart data={chartData}>
              <CartesianGrid strokeDasharray="3 3" stroke="#e5e7eb" />
              <XAxis 
                dataKey="timestamp" 
                tickFormatter={(value) => new Date(value).toLocaleDateString()}
                stroke="#6b7280"
                style={{ fontSize: '12px' }}
              />
              <YAxis 
                yAxisId="left"
                label={{ value: 'Sentiment Score', angle: -90, position: 'insideLeft' }}
                stroke="#6b7280"
                style={{ fontSize: '12px' }}
              />
              <YAxis 
                yAxisId="right" 
                orientation="right"
                label={{ value: 'Negative %', angle: 90, position: 'insideRight' }}
                stroke="#6b7280"
                style={{ fontSize: '12px' }}
              />
              <Tooltip 
                contentStyle={{ backgroundColor: '#fff', border: '1px solid #e5e7eb', borderRadius: '8px' }}
                labelFormatter={(value) => new Date(value).toLocaleString()}
              />
              <Legend />
              <Line 
                yAxisId="left"
                type="monotone" 
                dataKey="avg_sentiment_score" 
                stroke="#3b82f6" 
                strokeWidth={3}
                name="Avg Sentiment Score"
                dot={{ r: 4 }}
                activeDot={{ r: 7 }}
              />
              <Line 
                yAxisId="right"
                type="monotone" 
                dataKey="negative_percentage" 
                stroke="#ef4444" 
                strokeWidth={3}
                name="Negative %"
                dot={{ r: 4 }}
                activeDot={{ r: 7 }}
              />
            </LineChart>
          </ResponsiveContainer>
        </div>
      )}
      </div>

      {/* MongoDB Query Modal */}
      <QueryModal
        isOpen={modalOpen}
        onClose={() => setModalOpen(false)}
        title={MONGO_QUERIES.sentimentTrend.title}
        query={MONGO_QUERIES.sentimentTrend.query}
        executionTime={MONGO_QUERIES.sentimentTrend.executionTime}
        explanation={MONGO_QUERIES.sentimentTrend.explanation}
      />
    </>
  );
}

export default SentimentTrendChart;

