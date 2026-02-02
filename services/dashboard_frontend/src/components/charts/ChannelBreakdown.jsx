// src/components/charts/ChannelBreakdown.jsx
import { useQuery } from '@tanstack/react-query';
import { PieChart, Pie, Cell, ResponsiveContainer, Legend, Tooltip } from 'recharts';
import { BarChart2, Filter } from 'lucide-react';
import { dashboardAPI } from '../../api/dashboard';

const COLORS = {
  app: '#3b82f6',
  web: '#10b981',
  email: '#f59e0b',
  callcenter: '#8b5cf6',
  unknown: '#6b7280',
};

// Custom tooltip to show sentiment breakdown
const CustomTooltip = ({ active, payload }) => {
  if (active && payload && payload.length) {
    const data = payload[0].payload;
    // Find the channel data with sentiment distribution
    return (
      <div className="bg-white p-3 rounded-lg shadow-lg border border-gray-200">
        <p className="font-semibold text-gray-900 mb-2">{data.name}</p>
        <p className="text-sm text-gray-600">{data.value.toLocaleString()} reviews</p>
        <div className="mt-2 space-y-1 text-xs">
          <div className="flex items-center gap-2">
            <div className="w-3 h-3 rounded-full bg-green-500"></div>
            <span>Positive: {data.positive || 0}</span>
          </div>
          <div className="flex items-center gap-2">
            <div className="w-3 h-3 rounded-full bg-gray-500"></div>
            <span>Neutral: {data.neutral || 0}</span>
          </div>
          <div className="flex items-center gap-2">
            <div className="w-3 h-3 rounded-full bg-red-500"></div>
            <span>Negative: {data.negative || 0}</span>
          </div>
        </div>
        <p className="text-xs text-red-600 mt-2 font-medium">
          {data.negativePct}% negative
        </p>
      </div>
    );
  }
  return null;
};

function ChannelBreakdown({ filters }) {
  const { data, isLoading, isError } = useQuery({
    queryKey: ['channelBreakdown', filters],
    queryFn: () => dashboardAPI.getChannelBreakdown(filters),
  });

  if (isError) {
    return (
      <div className="glass-card p-6">
        <h2 className="text-xl font-bold text-white mb-4">Channel Breakdown</h2>
        <div className="text-red-300 font-medium bg-red-500/20 backdrop-blur-sm p-4 rounded-lg border border-red-400/30">
          Failed to load channel data.
        </div>
      </div>
    );
  }

  if (isLoading) {
    return (
      <div className="glass-card p-6">
        <h2 className="text-xl font-bold text-white mb-4">Channel Breakdown</h2>
        <div className="h-80 bg-white/10 rounded-xl animate-pulse shimmer"></div>
      </div>
    );
  }

  const channels = data?.channels || [];
  const chartData = channels.map(ch => ({
    name: ch.channel.charAt(0).toUpperCase() + ch.channel.slice(1),
    value: ch.review_count,
    positive: ch.sentiment_distribution.positive,
    neutral: ch.sentiment_distribution.neutral,
    negative: ch.sentiment_distribution.negative,
    negativePct: ch.negative_percentage,
  }));

  // Check if channel filter is active
  const isChannelFiltered = filters.channel !== null && filters.channel !== '';

  return (
    <div className="glass-card p-6">
      <div className="flex items-center justify-between mb-4">
        <div className="flex items-center gap-2">
          <div className="p-2 bg-blue-500/30 backdrop-blur-sm rounded-lg">
            <BarChart2 className="w-5 h-5 text-white" />
          </div>
          <h2 className="text-xl font-bold text-white drop-shadow-lg">Channel Breakdown</h2>
        </div>
        {isChannelFiltered && (
          <div className="flex items-center gap-1 text-xs bg-blue-500/30 backdrop-blur-md text-white px-3 py-2 rounded-full border border-white/30">
            <Filter className="w-3 h-3" />
            Filtered: {filters.channel}
          </div>
        )}
      </div>

      {channels.length === 0 ? (
        <div className="h-80 flex items-center justify-center">
          <p className="text-white/70 font-medium">No channel data available</p>
        </div>
      ) : isChannelFiltered && channels.length === 1 ? (
        // When channel is filtered to one, show sentiment distribution instead
        <div className="space-y-4">
          <div className="bg-blue-50 border border-blue-200 rounded-lg p-4 mb-4">
            <p className="text-sm text-blue-800">
              <strong>Viewing {filters.channel} channel only.</strong> Clear filter to see all channels.
            </p>
          </div>
          <div className="grid grid-cols-3 gap-4">
            <div className="bg-green-50 rounded-lg p-4 text-center">
              <div className="text-2xl font-bold text-green-700">
                {channels[0].sentiment_distribution.positive}
              </div>
              <div className="text-sm text-green-600 mt-1">Positive</div>
            </div>
            <div className="bg-gray-50 rounded-lg p-4 text-center">
              <div className="text-2xl font-bold text-gray-700">
                {channels[0].sentiment_distribution.neutral}
              </div>
              <div className="text-sm text-gray-600 mt-1">Neutral</div>
            </div>
            <div className="bg-red-50 rounded-lg p-4 text-center">
              <div className="text-2xl font-bold text-red-700">
                {channels[0].sentiment_distribution.negative}
              </div>
              <div className="text-sm text-red-600 mt-1">Negative</div>
            </div>
          </div>
          <div className="text-center text-sm text-gray-600 mt-4">
            {channels[0].review_count.toLocaleString()} total reviews | {channels[0].negative_percentage.toFixed(1)}% negative
          </div>
        </div>
      ) : (
        <>
          <ResponsiveContainer width="100%" height={280}>
            <PieChart>
              <Pie
                data={chartData}
                cx="50%"
                cy="50%"
                labelLine={false}
                label={({ name, percent }) => `${name} ${(percent * 100).toFixed(0)}%`}
                outerRadius={90}
                fill="#8884d8"
                dataKey="value"
              >
                {chartData.map((entry, index) => (
                  <Cell 
                    key={`cell-${index}`} 
                    fill={COLORS[entry.name.toLowerCase()] || COLORS.unknown} 
                  />
                ))}
              </Pie>
              <Tooltip content={<CustomTooltip />} />
            </PieChart>
          </ResponsiveContainer>

          <div className="mt-4 space-y-2">
            {channels.map((ch) => (
              <div key={ch.channel} className="flex items-center justify-between p-2 bg-gray-50 rounded">
                <div className="flex items-center gap-2">
                  <div 
                    className="w-3 h-3 rounded-full" 
                    style={{ backgroundColor: COLORS[ch.channel] || COLORS.unknown }}
                  ></div>
                  <span className="text-sm font-medium capitalize">{ch.channel}</span>
                </div>
                <div className="flex gap-4 text-sm">
                  <span className="text-gray-600">
                    {ch.review_count.toLocaleString()} reviews
                  </span>
                  <span className="text-red-600">
                    {ch.negative_percentage.toFixed(1)}% negative
                  </span>
                </div>
              </div>
            ))}
          </div>
        </>
      )}
    </div>
  );
}

export default ChannelBreakdown;

