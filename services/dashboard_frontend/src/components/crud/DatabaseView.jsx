// src/components/crud/DatabaseView.jsx
import { useState } from 'react';
import { useQuery } from '@tanstack/react-query';
import { Database, RefreshCw } from 'lucide-react';

const API_BASE = 'http://localhost:3001/api';

// Fetch reviews with pagination
const fetchReviews = async ({ page = 1, limit = 20, sentiment = '', channel = '' }) => {
    const params = new URLSearchParams({
        page: page.toString(),
        limit: limit.toString(),
        ...(sentiment && { sentiment }),
        ...(channel && { channel }),
    });

    const response = await fetch(`${API_BASE}/reviews?${params}`);
    if (!response.ok) throw new Error('Failed to fetch reviews');
    return response.json();
};

function DatabaseView() {
    // State
    const [page, setPage] = useState(1);
    const [filters, setFilters] = useState({ sentiment: '', channel: '' });

    // Queries with auto-refresh every 5 seconds
    const { data, isLoading, error, refetch } = useQuery({
        queryKey: ['database-reviews', page, filters],
        queryFn: () => fetchReviews({ page, limit: 20, ...filters }),
        refetchInterval: 5000, // Auto-refresh every 5 seconds
    });

    return (
        <div className="glass-card p-8">
            {/* Header */}
            <div className="flex items-center justify-between mb-8">
                <div>
                    <div className="flex items-center gap-3 mb-2">
                        <Database className="w-8 h-8 text-cyan-400" />
                        <h2 className="text-3xl font-bold text-white">Database View</h2>
                    </div>
                    <p className="text-slate-300">Real-time view of MongoDB reviews collection • Auto-refreshes every 5 seconds</p>
                </div>
                <button
                    onClick={() => refetch()}
                    className="flex items-center gap-2 px-6 py-3 bg-gradient-to-r from-blue-500 to-cyan-600 text-white rounded-xl font-semibold hover:shadow-lg hover:scale-105 transition-all"
                >
                    <RefreshCw className="w-5 h-5" />
                    Refresh Now
                </button>
            </div>

            {/* Stats Bar */}
            {data && (
                <div className="grid grid-cols-1 md:grid-cols-4 gap-4 mb-6">
                    <div className="bg-gradient-to-br from-blue-500/20 to-cyan-500/20 border border-blue-500/30 rounded-xl p-4">
                        <div className="text-sm text-blue-300 mb-1">Total Reviews</div>
                        <div className="text-2xl font-bold text-white">{data.total?.toLocaleString()}</div>
                    </div>
                    <div className="bg-gradient-to-br from-green-500/20 to-emerald-500/20 border border-green-500/30 rounded-xl p-4">
                        <div className="text-sm text-green-300 mb-1">Current Page</div>
                        <div className="text-2xl font-bold text-white">{data.page} of {data.total_pages}</div>
                    </div>
                    <div className="bg-gradient-to-br from-purple-500/20 to-pink-500/20 border border-purple-500/30 rounded-xl p-4">
                        <div className="text-sm text-purple-300 mb-1">Showing</div>
                        <div className="text-2xl font-bold text-white">{data.reviews?.length || 0} rows</div>
                    </div>
                    <div className="bg-gradient-to-br from-orange-500/20 to-red-500/20 border border-orange-500/30 rounded-xl p-4">
                        <div className="text-sm text-orange-300 mb-1">Collection</div>
                        <div className="text-lg font-bold text-white">reviews_enriched</div>
                    </div>
                </div>
            )}

            {/* Filters */}
            <div className="grid grid-cols-1 md:grid-cols-2 gap-4 mb-6">
                <div>
                    <label className="block text-sm font-medium text-slate-300 mb-2">Filter by Sentiment</label>
                    <select
                        value={filters.sentiment}
                        onChange={(e) => {
                            setFilters({ ...filters, sentiment: e.target.value });
                            setPage(1); // Reset to page 1 when filtering
                        }}
                        className="w-full px-4 py-2 bg-slate-800/50 border border-slate-600 rounded-lg text-white focus:ring-2 focus:ring-blue-500"
                    >
                        <option value="">All Sentiments</option>
                        <option value="positive">Positive</option>
                        <option value="neutral">Neutral</option>
                        <option value="negative">Negative</option>
                    </select>
                </div>
                <div>
                    <label className="block text-sm font-medium text-slate-300 mb-2">Filter by Channel</label>
                    <select
                        value={filters.channel}
                        onChange={(e) => {
                            setFilters({ ...filters, channel: e.target.value });
                            setPage(1); // Reset to page 1 when filtering
                        }}
                        className="w-full px-4 py-2 bg-slate-800/50 border border-slate-600 rounded-lg text-white focus:ring-2 focus:ring-blue-500"
                    >
                        <option value="">All Channels</option>
                        <option value="web">Web</option>
                        <option value="app">App</option>
                        <option value="email">Email</option>
                        <option value="callcenter">Call Center</option>
                        <option value="social">Social Media</option>
                    </select>
                </div>
            </div>

            {/* Reviews Table */}
            {isLoading ? (
                <div className="text-center py-12">
                    <RefreshCw className="w-12 h-12 text-cyan-400 animate-spin mx-auto mb-4" />
                    <p className="text-slate-300">Loading database records...</p>
                </div>
            ) : error ? (
                <div className="text-center py-12">
                    <div className="text-red-400 mb-2">❌ Error loading data</div>
                    <p className="text-slate-400 text-sm">{error.message}</p>
                </div>
            ) : (
                <>
                    <div className="overflow-x-auto">
                        <table className="w-full">
                            <thead>
                                <tr className="border-b-2 border-slate-700">
                                    <th className="text-left py-3 px-4 text-slate-300 font-semibold">Event ID</th>
                                    <th className="text-left py-3 px-4 text-slate-300 font-semibold">Customer</th>
                                    <th className="text-left py-3 px-4 text-slate-300 font-semibold">Product</th>
                                    <th className="text-left py-3 px-4 text-slate-300 font-semibold">Review Text</th>
                                    <th className="text-left py-3 px-4 text-slate-300 font-semibold">Sentiment</th>
                                    <th className="text-left py-3 px-4 text-slate-300 font-semibold">Channel</th>
                                    <th className="text-left py-3 px-4 text-slate-300 font-semibold">Confidence</th>
                                    <th className="text-left py-3 px-4 text-slate-300 font-semibold">Timestamp</th>
                                </tr>
                            </thead>
                            <tbody>
                                {data?.reviews?.map((review) => (
                                    <tr key={review._id} className="border-b border-slate-800 hover:bg-slate-800/30 transition-colors">
                                        <td className="py-3 px-4 text-slate-400 text-xs font-mono">{review.event_id?.substring(0, 8)}...</td>
                                        <td className="py-3 px-4 text-white font-medium">{review.customer_id}</td>
                                        <td className="py-3 px-4 text-slate-300 text-sm max-w-xs truncate">{review.product_name}</td>
                                        <td className="py-3 px-4 text-slate-300 max-w-md">
                                            <div className="truncate">{review.text}</div>
                                        </td>
                                        <td className="py-3 px-4">
                                            <span className={`px-3 py-1 rounded-full text-xs font-semibold ${review.sentiment_label === 'positive' ? 'bg-green-500/20 text-green-400 border border-green-500/30' :
                                                    review.sentiment_label === 'negative' ? 'bg-red-500/20 text-red-400 border border-red-500/30' :
                                                        'bg-yellow-500/20 text-yellow-400 border border-yellow-500/30'
                                                }`}>
                                                {review.sentiment_label}
                                            </span>
                                        </td>
                                        <td className="py-3 px-4">
                                            <span className="px-2 py-1 bg-slate-700 text-slate-300 rounded text-xs">
                                                {review.channel}
                                            </span>
                                        </td>
                                        <td className="py-3 px-4 text-slate-300">
                                            <span className="font-mono text-sm">{(review.confidence * 100).toFixed(1)}%</span>
                                        </td>
                                        <td className="py-3 px-4 text-slate-400 text-xs">
                                            {new Date(review.timestamp_utc).toLocaleString()}
                                        </td>
                                    </tr>
                                ))}
                            </tbody>
                        </table>
                    </div>

                    {/* Pagination */}
                    <div className="flex items-center justify-between mt-6">
                        <div className="text-slate-300">
                            Showing <span className="font-semibold text-white">{((page - 1) * 20) + 1}</span> to{' '}
                            <span className="font-semibold text-white">{Math.min(page * 20, data?.total || 0)}</span> of{' '}
                            <span className="font-semibold text-white">{data?.total?.toLocaleString()}</span> records
                        </div>
                        <div className="flex gap-2">
                            <button
                                onClick={() => setPage(1)}
                                disabled={page === 1}
                                className="px-4 py-2 bg-slate-700 text-white rounded-lg disabled:opacity-50 disabled:cursor-not-allowed hover:bg-slate-600 transition-colors"
                            >
                                First
                            </button>
                            <button
                                onClick={() => setPage(p => Math.max(1, p - 1))}
                                disabled={page === 1}
                                className="px-4 py-2 bg-slate-700 text-white rounded-lg disabled:opacity-50 disabled:cursor-not-allowed hover:bg-slate-600 transition-colors"
                            >
                                Previous
                            </button>
                            <div className="px-4 py-2 bg-slate-800 text-white rounded-lg border border-slate-600">
                                Page {page} of {data?.total_pages}
                            </div>
                            <button
                                onClick={() => setPage(p => p + 1)}
                                disabled={page >= data?.total_pages}
                                className="px-4 py-2 bg-slate-700 text-white rounded-lg disabled:opacity-50 disabled:cursor-not-allowed hover:bg-slate-600 transition-colors"
                            >
                                Next
                            </button>
                            <button
                                onClick={() => setPage(data?.total_pages || 1)}
                                disabled={page >= data?.total_pages}
                                className="px-4 py-2 bg-slate-700 text-white rounded-lg disabled:opacity-50 disabled:cursor-not-allowed hover:bg-slate-600 transition-colors"
                            >
                                Last
                            </button>
                        </div>
                    </div>
                </>
            )}
        </div>
    );
}

export default DatabaseView;
