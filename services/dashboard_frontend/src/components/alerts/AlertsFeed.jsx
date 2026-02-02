// src/components/alerts/AlertsFeed.jsx
import { useQuery } from '@tanstack/react-query';
import { AlertTriangle, TrendingUp, Activity, Bell } from 'lucide-react';
import { dashboardAPI } from '../../api/dashboard';

const ALERT_ICONS = {
  negative_spike: AlertTriangle,
  volume_surge: TrendingUp,
  sentiment_drop: Activity,
};

const SEVERITY_COLORS = {
  high: 'bg-red-100 text-red-800 border-red-300',
  medium: 'bg-yellow-100 text-yellow-800 border-yellow-300',
  low: 'bg-blue-100 text-blue-800 border-blue-300',
};

function AlertsFeed({ filters }) {
  const { data, isLoading, isError } = useQuery({
    queryKey: ['alerts', filters],
    queryFn: () => dashboardAPI.getAlerts({ timeWindow: '24h', limit: 10 }),
  });

  if (isError) {
    return (
      <div className="glass-card p-8">
        <h2 className="text-2xl font-bold text-white mb-4">Recent Alerts</h2>
        <div className="text-red-300 font-medium bg-red-500/20 backdrop-blur-sm p-4 rounded-lg border border-red-400/30">
          Failed to load alerts.
        </div>
      </div>
    );
  }

  if (isLoading) {
    return (
      <div className="glass-card p-8">
        <h2 className="text-2xl font-bold text-white mb-4">Recent Alerts</h2>
        <div className="h-64 bg-white/10 rounded-xl animate-pulse shimmer"></div>
      </div>
    );
  }

  const alerts = data?.alerts || [];

  return (
    <div className="glass-card p-8">
      <div className="flex items-center justify-between mb-6">
        <div className="flex items-center gap-3">
          <div className="p-3 bg-yellow-500/30 backdrop-blur-sm rounded-xl">
            <Bell className="w-6 h-6 text-white" />
          </div>
          <h2 className="text-2xl font-bold text-white drop-shadow-lg">Recent Alerts</h2>
        </div>
        <div className="px-4 py-2 bg-white/20 backdrop-blur-md rounded-full border border-white/30">
          <span className="text-sm text-white font-medium">
            {alerts.length} alert{alerts.length !== 1 ? 's' : ''} in last 24h
          </span>
        </div>
      </div>

      {alerts.length === 0 ? (
        <div className="h-32 flex items-center justify-center bg-white/10 backdrop-blur-sm rounded-xl border border-white/20">
          <div className="text-center">
            <Bell className="w-10 h-10 mx-auto mb-3 text-white/50" />
            <p className="text-white font-medium">No alerts detected</p>
            <p className="text-xs mt-1 text-white/70">System is monitoring for anomalies</p>
          </div>
        </div>
      ) : (
        <div className="space-y-4 max-h-96 overflow-y-auto pr-2">
          {alerts.map((alert) => {
            const Icon = ALERT_ICONS[alert.alert_type] || AlertTriangle;
            const colorClass = SEVERITY_COLORS[alert.severity] || SEVERITY_COLORS.low;
            
            return (
              <div 
                key={alert.alert_id} 
                className={`p-5 rounded-xl border-2 backdrop-blur-md ${colorClass} transform transition-all duration-300 hover:scale-[1.02] hover:shadow-xl`}
              >
                <div className="flex items-start gap-3">
                  <Icon className="w-5 h-5 flex-shrink-0 mt-0.5" />
                  <div className="flex-1 min-w-0">
                    <div className="flex items-center justify-between gap-2 mb-1">
                      <h3 className="text-sm font-semibold">
                        {alert.product_name}
                      </h3>
                      <span className="text-xs uppercase font-semibold px-2 py-1 rounded">
                        {alert.severity}
                      </span>
                    </div>
                    <p className="text-sm mb-1">{alert.reason}</p>
                    <div className="flex items-center gap-4 text-xs">
                      <span className="text-gray-600">
                        {alert.product_id}
                      </span>
                      <span className="text-gray-600">
                        {new Date(alert.timestamp).toLocaleString()}
                      </span>
                      <span className="text-gray-600 capitalize">
                        {alert.alert_type.replace('_', ' ')}
                      </span>
                    </div>
                  </div>
                </div>
              </div>
            );
          })}
        </div>
      )}
    </div>
  );
}

export default AlertsFeed;


