// src/components/charts/ChurnOverTimeChart.jsx
import { useQuery } from '@tanstack/react-query';
import {
  LineChart,
  Line,
  XAxis,
  YAxis,
  CartesianGrid,
  Tooltip,
  Legend,
  ResponsiveContainer,
} from 'recharts';
import { Activity } from 'lucide-react';
import { dashboardAPI } from '../../api/dashboard';

function ChurnOverTimeChart({ filters }) {
  const { data, isLoading, isError } = useQuery({
    queryKey: ['churnOverTime', filters],
    queryFn: () => dashboardAPI.getChurnOverTime(filters),
  });

  if (isError) {
    return (
      <div className="glass-card p-8">
        <h2 className="text-2xl font-bold text-white mb-4">Churn over time</h2>
        <div className="text-red-300 font-medium bg-red-500/20 backdrop-blur-sm p-4 rounded-lg border border-red-400/30">
          Failed to load churn-over-time data.
        </div>
      </div>
    );
  }

  if (isLoading) {
    return (
      <div className="glass-card p-8">
        <h2 className="text-2xl font-bold text-white mb-4">Churn over time</h2>
        <div className="h-64 bg-white/10 rounded-xl animate-pulse" />
      </div>
    );
  }

  const chartData = data?.data || [];

  return (
    <div className="glass-card p-8">
      <div className="flex items-center justify-between mb-6">
        <div className="flex items-center gap-3">
          <div className="p-3 bg-amber-500/30 backdrop-blur-sm rounded-xl">
            <Activity className="w-6 h-6 text-white" />
          </div>
          <h2 className="text-2xl font-bold text-white drop-shadow-lg">Churn over time</h2>
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
                tickFormatter={(v) => new Date(v).toLocaleString(undefined, { dateStyle: 'short', timeStyle: 'short' })}
                stroke="#6b7280"
                style={{ fontSize: '12px' }}
              />
              <YAxis
                yAxisId="left"
                label={{ value: 'Churn risk (0–1)', angle: -90, position: 'insideLeft' }}
                stroke="#6b7280"
                style={{ fontSize: '12px' }}
                domain={[0, 1]}
              />
              <YAxis
                yAxisId="right"
                orientation="right"
                label={{ value: 'Negative %', angle: 90, position: 'insideRight' }}
                stroke="#6b7280"
                style={{ fontSize: '12px' }}
              />
              <Tooltip
                contentStyle={{
                  backgroundColor: '#fff',
                  border: '1px solid #e5e7eb',
                  borderRadius: '8px',
                }}
                labelFormatter={(v) => new Date(v).toLocaleString()}
              />
              <Legend />
              <Line
                yAxisId="left"
                type="monotone"
                dataKey="churn_risk"
                stroke="#f59e0b"
                strokeWidth={3}
                name="Churn risk"
                dot={{ r: 4 }}
                activeDot={{ r: 7 }}
              />
              <Line
                yAxisId="right"
                type="monotone"
                dataKey="negative_percentage"
                stroke="#ef4444"
                strokeWidth={2}
                name="Negative %"
                dot={{ r: 3 }}
                activeDot={{ r: 6 }}
              />
            </LineChart>
          </ResponsiveContainer>
        </div>
      )}
    </div>
  );
}

export default ChurnOverTimeChart;
