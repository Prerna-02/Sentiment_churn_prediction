// src/components/charts/FixFirstRanking.jsx
import { useState } from 'react';
import { useQuery } from '@tanstack/react-query';
import { AlertCircle, TrendingDown, TrendingUp, Minus, Info } from 'lucide-react';
import { dashboardAPI } from '../../api/dashboard';
import QueryModal from '../common/QueryModal';
import { MONGO_QUERIES } from '../../data/mongoQueries';

function FixFirstRanking({ filters }) {
  const [modalOpen, setModalOpen] = useState(false);
  
  const { data, isLoading, isError } = useQuery({
    queryKey: ['fixFirstRanking', filters],
    queryFn: () => dashboardAPI.getFixFirstRanking({ ...filters, limit: 10 }),
  });

  const getTrendIcon = (trend) => {
    if (trend === 'up') return <TrendingUp className="w-4 h-4 text-green-600" />;
    if (trend === 'down') return <TrendingDown className="w-4 h-4 text-red-600" />;
    return <Minus className="w-4 h-4 text-gray-400" />;
  };

  const getPriorityColor = (score) => {
    if (score >= 0.7) return 'bg-red-100 text-red-800';
    if (score >= 0.5) return 'bg-yellow-100 text-yellow-800';
    return 'bg-green-100 text-green-800';
  };

  if (isError) {
    return (
      <div className="glass-card p-8">
        <h2 className="text-2xl font-bold text-white mb-4">Fix-First Product Ranking</h2>
        <div className="text-red-300 font-medium bg-red-500/20 backdrop-blur-sm p-4 rounded-lg border border-red-400/30">
          Failed to load fix-first ranking data.
        </div>
      </div>
    );
  }

  if (isLoading) {
    return (
      <div className="glass-card p-8">
        <h2 className="text-2xl font-bold text-white mb-4">Fix-First Product Ranking</h2>
        <div className="h-96 bg-white/10 rounded-xl animate-pulse shimmer"></div>
      </div>
    );
  }

  const products = data?.products || [];

  return (
    <>
      <div className="glass-card p-8 relative group">
        <button
          onClick={() => setModalOpen(true)}
          className="absolute top-6 right-6 p-2 text-white/70 hover:text-white hover:bg-white/20 rounded-full transition-all opacity-0 group-hover:opacity-100 z-10"
          title="View MongoDB Query"
        >
          <Info className="w-5 h-5" />
        </button>

        <div className="flex items-center justify-between mb-6">
          <div className="flex items-center gap-3">
            <div className="p-3 bg-red-500/30 backdrop-blur-sm rounded-xl">
              <AlertCircle className="w-6 h-6 text-white" />
            </div>
            <h2 className="text-2xl font-bold text-white drop-shadow-lg">Fix-First Product Ranking</h2>
          </div>
          <div className="px-4 py-2 bg-white/20 backdrop-blur-md rounded-full border border-white/30">
            <span className="text-sm text-white font-medium">
              Showing top {products.length} products
            </span>
          </div>
        </div>

      {products.length === 0 ? (
        <div className="h-64 flex items-center justify-center">
          <p className="text-white/70 font-medium">No products data available</p>
        </div>
      ) : (
        <div className="overflow-x-auto bg-white/95 backdrop-blur-sm rounded-xl shadow-lg">
          <table className="min-w-full divide-y divide-gray-200">
            <thead className="bg-gradient-to-r from-purple-600/20 to-blue-600/20">
              <tr>
                <th className="px-4 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">
                  Rank
                </th>
                <th className="px-4 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">
                  Product
                </th>
                <th className="px-4 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">
                  Priority Score
                </th>
                <th className="px-4 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">
                  Negative %
                </th>
                <th className="px-4 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">
                  Reviews
                </th>
                <th className="px-4 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">
                  Trend
                </th>
              </tr>
            </thead>
            <tbody className="bg-white divide-y divide-gray-200">
              {products.map((product) => (
                <tr key={product.product_id} className="hover:bg-gray-50">
                  <td className="px-4 py-4 whitespace-nowrap">
                    <div className="flex items-center justify-center w-8 h-8 rounded-full bg-gray-100 text-gray-700 font-semibold">
                      {product.priority_rank}
                    </div>
                  </td>
                  <td className="px-4 py-4">
                    <div className="text-sm font-medium text-gray-900">
                      {product.product_name}
                    </div>
                    <div className="text-xs text-gray-500">{product.product_id}</div>
                  </td>
                  <td className="px-4 py-4 whitespace-nowrap">
                    <span className={`inline-flex px-2 py-1 text-xs font-semibold rounded-full ${getPriorityColor(product.priority_score)}`}>
                      {product.priority_score.toFixed(3)}
                    </span>
                  </td>
                  <td className="px-4 py-4 whitespace-nowrap text-sm text-gray-900">
                    {product.negative_percentage.toFixed(1)}%
                  </td>
                  <td className="px-4 py-4 whitespace-nowrap text-sm text-gray-900">
                    {product.review_count.toLocaleString()}
                  </td>
                  <td className="px-4 py-4 whitespace-nowrap">
                    <div className="flex items-center gap-1">
                      {getTrendIcon(product.trend)}
                      <span className="text-xs text-gray-500 capitalize">{product.trend}</span>
                    </div>
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      )}

      <div className="mt-6 p-4 bg-blue-500/20 backdrop-blur-md rounded-xl border border-blue-400/30">
        <p className="text-sm text-white font-medium">
          <strong>💡 Priority Score Algorithm:</strong> 40% negative ratio + 25% review volume + 15% confidence uncertainty + 20% sentiment velocity
        </p>
        </div>
      </div>

      <QueryModal
        isOpen={modalOpen}
        onClose={() => setModalOpen(false)}
        title={MONGO_QUERIES.fixFirstRanking.title}
        query={MONGO_QUERIES.fixFirstRanking.query}
        executionTime={MONGO_QUERIES.fixFirstRanking.executionTime}
        explanation={MONGO_QUERIES.fixFirstRanking.explanation}
      />
    </>
  );
}

export default FixFirstRanking;

