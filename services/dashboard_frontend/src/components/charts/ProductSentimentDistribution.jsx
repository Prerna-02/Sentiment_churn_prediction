// src/components/charts/ProductSentimentDistribution.jsx
import { useQuery } from '@tanstack/react-query';
import { BarChart, Bar, XAxis, YAxis, CartesianGrid, Tooltip, Legend, ResponsiveContainer } from 'recharts';
import { Package } from 'lucide-react';
import { dashboardAPI } from '../../api/dashboard';

function ProductSentimentDistribution({ filters }) {
  const { data, isLoading, isError } = useQuery({
    queryKey: ['productSentiment', filters],
    queryFn: () => dashboardAPI.getProductSentimentDistribution({ ...filters, limit: 10 }),
  });

  if (isError) {
    return (
      <div className="bg-white rounded-lg shadow-md p-6">
        <h2 className="text-lg font-semibold text-gray-900 mb-4">Product Sentiment Distribution</h2>
        <div className="text-red-600">Failed to load product sentiment data.</div>
      </div>
    );
  }

  if (isLoading) {
    return (
      <div className="bg-white rounded-lg shadow-md p-6">
        <h2 className="text-lg font-semibold text-gray-900 mb-4">Product Sentiment Distribution</h2>
        <div className="h-80 bg-gray-100 rounded animate-pulse"></div>
      </div>
    );
  }

  const products = data?.products || [];
  const chartData = products.map(p => ({
    name: p.product_name.length > 20 ? p.product_name.substring(0, 20) + '...' : p.product_name,
    positive: p.sentiment_distribution.positive,
    neutral: p.sentiment_distribution.neutral,
    negative: p.sentiment_distribution.negative,
  }));

  return (
    <div className="bg-white rounded-lg shadow-md p-6">
      <div className="flex items-center gap-2 mb-4">
        <Package className="w-5 h-5 text-blue-600" />
        <h2 className="text-lg font-semibold text-gray-900">Product Sentiment Distribution</h2>
      </div>

      {products.length === 0 ? (
        <div className="h-80 flex items-center justify-center text-gray-500">
          No product sentiment data available
        </div>
      ) : (
        <>
          <ResponsiveContainer width="100%" height={320}>
            <BarChart data={chartData}>
              <CartesianGrid strokeDasharray="3 3" stroke="#e5e7eb" />
              <XAxis 
                dataKey="name" 
                stroke="#6b7280"
                style={{ fontSize: '11px' }}
                angle={-45}
                textAnchor="end"
                height={80}
              />
              <YAxis stroke="#6b7280" style={{ fontSize: '12px' }} />
              <Tooltip contentStyle={{ backgroundColor: '#fff', border: '1px solid #e5e7eb' }} />
              <Legend />
              <Bar dataKey="positive" stackId="a" fill="#10b981" name="Positive" />
              <Bar dataKey="neutral" stackId="a" fill="#6b7280" name="Neutral" />
              <Bar dataKey="negative" stackId="a" fill="#ef4444" name="Negative" />
            </BarChart>
          </ResponsiveContainer>

          <div className="mt-4 text-xs text-gray-500">
            Showing top {products.length} products by review count
          </div>
        </>
      )}
    </div>
  );
}

export default ProductSentimentDistribution;


