// src/components/filters/DashboardFilters.jsx
import { Clock } from 'lucide-react';

const TIME_WINDOWS = [
  { value: '1h', label: 'Last Hour' },
  { value: '24h', label: 'Last 24 Hours' },
  { value: '7d', label: 'Last 7 Days' },
  { value: '30d', label: 'Last 30 Days' },
  { value: 'all', label: 'All Time' },
];

const CHANNELS = [
  { value: null, label: 'All Channels' },
  { value: 'app', label: 'App' },
  { value: 'web', label: 'Web' },
  { value: 'email', label: 'Email' },
  { value: 'callcenter', label: 'Call Center' },
];

function DashboardFilters({ filters, onFilterChange }) {
  return (
    <div className="glass-card p-6">
      <div className="flex items-center gap-6 flex-wrap">
        {/* Time Window Filter */}
        <div className="flex items-center gap-3">
          <Clock className="w-5 h-5 text-white" />
          <label className="text-sm font-semibold text-white">Time Window:</label>
          <select
            value={filters.timeWindow}
            onChange={(e) => onFilterChange({ timeWindow: e.target.value })}
            className="px-4 py-2 bg-white/20 backdrop-blur-md border border-white/30 rounded-xl focus:outline-none focus:ring-2 focus:ring-white/50 text-sm text-white font-medium transition-all hover:bg-white/30"
          >
            {TIME_WINDOWS.map((tw) => (
              <option key={tw.value} value={tw.value} className="bg-gray-800">
                {tw.label}
              </option>
            ))}
          </select>
        </div>

        {/* Channel Filter */}
        <div className="flex items-center gap-3">
          <label className="text-sm font-semibold text-white">Channel:</label>
          <select
            value={filters.channel || ''}
            onChange={(e) => onFilterChange({ channel: e.target.value || null })}
            className="px-4 py-2 bg-white/20 backdrop-blur-md border border-white/30 rounded-xl focus:outline-none focus:ring-2 focus:ring-white/50 text-sm text-white font-medium transition-all hover:bg-white/30"
          >
            {CHANNELS.map((ch) => (
              <option key={ch.value || 'all'} value={ch.value || ''} className="bg-gray-800">
                {ch.label}
              </option>
            ))}
          </select>
        </div>

        {/* Product Filter */}
        <div className="flex items-center gap-3">
          <label className="text-sm font-semibold text-white">Product:</label>
          <input
            type="text"
            placeholder="Enter Product ID..."
            value={filters.productId || ''}
            onChange={(e) => onFilterChange({ productId: e.target.value || null })}
            className="px-4 py-2 bg-white/20 backdrop-blur-md border border-white/30 rounded-xl focus:outline-none focus:ring-2 focus:ring-white/50 text-sm text-white placeholder-white/60 font-medium w-56 transition-all hover:bg-white/30"
          />
        </div>

        {/* Reset Filters */}
        {(filters.productId || filters.channel) && (
          <button
            onClick={() => onFilterChange({ productId: null, channel: null })}
            className="px-4 py-2 text-sm text-white font-semibold bg-red-500/30 backdrop-blur-md hover:bg-red-500/50 rounded-xl border border-white/30 transition-all transform hover:scale-105"
          >
            Clear Filters
          </button>
        )}
      </div>
    </div>
  );
}

export default DashboardFilters;


