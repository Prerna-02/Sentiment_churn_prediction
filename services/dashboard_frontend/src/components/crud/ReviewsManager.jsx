// src/components/crud/ReviewsManager.jsx
import { useState } from 'react';
import { useQuery, useMutation, useQueryClient } from '@tanstack/react-query';
import { Plus, Edit2, Trash2, X, Save, Search } from 'lucide-react';

const API_BASE = 'http://localhost:3001/api';

// Fetch reviews with pagination
const fetchReviews = async ({ page = 1, limit = 10, sentiment = '', channel = '' }) => {
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

// Create review
const createReview = async (reviewData) => {
    const response = await fetch(`${API_BASE}/reviews`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(reviewData),
    });
    if (!response.ok) throw new Error('Failed to create review');
    return response.json();
};

// Update review
const updateReview = async ({ id, data }) => {
    const response = await fetch(`${API_BASE}/reviews/${id}`, {
        method: 'PUT',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(data),
    });
    if (!response.ok) throw new Error('Failed to update review');
    return response.json();
};

// Delete review
const deleteReview = async (id) => {
    const response = await fetch(`${API_BASE}/reviews/${id}`, {
        method: 'DELETE',
    });
    if (!response.ok) throw new Error('Failed to delete review');
    return response.json();
};

function ReviewsManager() {
    const queryClient = useQueryClient();

    // State
    const [page, setPage] = useState(1);
    const [filters, setFilters] = useState({ sentiment: '', channel: '' });
    const [showCreateModal, setShowCreateModal] = useState(false);
    const [editingReview, setEditingReview] = useState(null);
    const [newReview, setNewReview] = useState({
        customer_id: '',
        product_id: '',
        product_name: '',
        text: '',
        channel: 'web',
    });

    // Queries
    const { data, isLoading, error } = useQuery({
        queryKey: ['reviews', page, filters],
        queryFn: () => fetchReviews({ page, limit: 10, ...filters }),
    });

    // Mutations
    const createMutation = useMutation({
        mutationFn: createReview,
        onSuccess: () => {
            queryClient.invalidateQueries(['reviews']);
            setShowCreateModal(false);
            setNewReview({ customer_id: '', product_id: '', product_name: '', text: '', channel: 'web' });
            alert('✅ Review created successfully!');
        },
        onError: (error) => alert(`❌ Error: ${error.message}`),
    });

    const updateMutation = useMutation({
        mutationFn: updateReview,
        onSuccess: () => {
            queryClient.invalidateQueries(['reviews']);
            setEditingReview(null);
            alert('✅ Review updated successfully!');
        },
        onError: (error) => alert(`❌ Error: ${error.message}`),
    });

    const deleteMutation = useMutation({
        mutationFn: deleteReview,
        onSuccess: () => {
            queryClient.invalidateQueries(['reviews']);
            alert('✅ Review deleted successfully!');
        },
        onError: (error) => alert(`❌ Error: ${error.message}`),
    });

    // Handlers
    const handleCreate = () => {
        if (!newReview.text || !newReview.customer_id || !newReview.product_id) {
            alert('Please fill in all required fields');
            return;
        }
        createMutation.mutate(newReview);
    };

    const handleUpdate = () => {
        if (!editingReview.text) {
            alert('Review text cannot be empty');
            return;
        }
        updateMutation.mutate({
            id: editingReview._id,
            data: {
                text: editingReview.text,
                product_name: editingReview.product_name,
                channel: editingReview.channel,
            },
        });
    };

    const handleDelete = (id) => {
        if (window.confirm('Are you sure you want to delete this review?')) {
            deleteMutation.mutate(id);
        }
    };

    return (
        <div className="glass-card p-8">
            {/* Header */}
            <div className="flex items-center justify-between mb-8">
                <div>
                    <h2 className="text-3xl font-bold text-white mb-2">Manage Reviews</h2>
                    <p className="text-slate-300">Create, edit, and delete customer reviews</p>
                </div>
                <button
                    onClick={() => setShowCreateModal(true)}
                    className="flex items-center gap-2 px-6 py-3 bg-gradient-to-r from-green-500 to-emerald-600 text-white rounded-xl font-semibold hover:shadow-lg hover:scale-105 transition-all"
                >
                    <Plus className="w-5 h-5" />
                    Add Review
                </button>
            </div>

            {/* Filters */}
            <div className="grid grid-cols-1 md:grid-cols-2 gap-4 mb-6">
                <div>
                    <label className="block text-sm font-medium text-slate-300 mb-2">Filter by Sentiment</label>
                    <select
                        value={filters.sentiment}
                        onChange={(e) => setFilters({ ...filters, sentiment: e.target.value })}
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
                        onChange={(e) => setFilters({ ...filters, channel: e.target.value })}
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
                <div className="text-center py-12 text-slate-300">Loading reviews...</div>
            ) : error ? (
                <div className="text-center py-12 text-red-400">Error: {error.message}</div>
            ) : (
                <>
                    <div className="overflow-x-auto">
                        <table className="w-full">
                            <thead>
                                <tr className="border-b border-slate-700">
                                    <th className="text-left py-3 px-4 text-slate-300 font-semibold">Customer</th>
                                    <th className="text-left py-3 px-4 text-slate-300 font-semibold">Product</th>
                                    <th className="text-left py-3 px-4 text-slate-300 font-semibold">Review</th>
                                    <th className="text-left py-3 px-4 text-slate-300 font-semibold">Sentiment</th>
                                    <th className="text-left py-3 px-4 text-slate-300 font-semibold">Channel</th>
                                    <th className="text-left py-3 px-4 text-slate-300 font-semibold">Confidence</th>
                                    <th className="text-right py-3 px-4 text-slate-300 font-semibold">Actions</th>
                                </tr>
                            </thead>
                            <tbody>
                                {data?.reviews?.map((review) => (
                                    <tr key={review._id} className="border-b border-slate-800 hover:bg-slate-800/30">
                                        <td className="py-3 px-4 text-white">{review.customer_id}</td>
                                        <td className="py-3 px-4 text-slate-300 text-sm">{review.product_name}</td>
                                        <td className="py-3 px-4 text-slate-300 max-w-md truncate">{review.text}</td>
                                        <td className="py-3 px-4">
                                            <span className={`px-3 py-1 rounded-full text-xs font-semibold ${review.sentiment_label === 'positive' ? 'bg-green-500/20 text-green-400' :
                                                review.sentiment_label === 'negative' ? 'bg-red-500/20 text-red-400' :
                                                    'bg-yellow-500/20 text-yellow-400'
                                                }`}>
                                                {review.sentiment_label}
                                            </span>
                                        </td>
                                        <td className="py-3 px-4 text-slate-300">{review.channel}</td>
                                        <td className="py-3 px-4 text-slate-300">{(review.confidence * 100).toFixed(1)}%</td>
                                        <td className="py-3 px-4">
                                            <div className="flex items-center justify-end gap-2">
                                                <button
                                                    onClick={() => setEditingReview(review)}
                                                    className="p-2 text-blue-400 hover:bg-blue-500/20 rounded-lg transition-colors"
                                                >
                                                    <Edit2 className="w-4 h-4" />
                                                </button>
                                                <button
                                                    onClick={() => handleDelete(review._id)}
                                                    className="p-2 text-red-400 hover:bg-red-500/20 rounded-lg transition-colors"
                                                >
                                                    <Trash2 className="w-4 h-4" />
                                                </button>
                                            </div>
                                        </td>
                                    </tr>
                                ))}
                            </tbody>
                        </table>
                    </div>

                    {/* Pagination */}
                    <div className="flex items-center justify-between mt-6">
                        <div className="text-slate-300">
                            Page {data?.page} of {data?.total_pages} ({data?.total} total reviews)
                        </div>
                        <div className="flex gap-2">
                            <button
                                onClick={() => setPage(p => Math.max(1, p - 1))}
                                disabled={page === 1}
                                className="px-4 py-2 bg-slate-700 text-white rounded-lg disabled:opacity-50 disabled:cursor-not-allowed hover:bg-slate-600 transition-colors"
                            >
                                Previous
                            </button>
                            <button
                                onClick={() => setPage(p => p + 1)}
                                disabled={page >= data?.total_pages}
                                className="px-4 py-2 bg-slate-700 text-white rounded-lg disabled:opacity-50 disabled:cursor-not-allowed hover:bg-slate-600 transition-colors"
                            >
                                Next
                            </button>
                        </div>
                    </div>
                </>
            )}

            {/* Create Modal */}
            {showCreateModal && (
                <div className="fixed inset-0 bg-black/70 flex items-center justify-center z-50 p-4">
                    <div className="bg-slate-900 rounded-2xl p-8 max-w-2xl w-full border border-slate-700">
                        <div className="flex items-center justify-between mb-6">
                            <h3 className="text-2xl font-bold text-white">Create New Review</h3>
                            <button onClick={() => setShowCreateModal(false)} className="text-slate-400 hover:text-white">
                                <X className="w-6 h-6" />
                            </button>
                        </div>

                        <div className="space-y-4">
                            <div>
                                <label className="block text-sm font-medium text-slate-300 mb-2">Customer ID *</label>
                                <input
                                    type="text"
                                    value={newReview.customer_id}
                                    onChange={(e) => setNewReview({ ...newReview, customer_id: e.target.value })}
                                    className="w-full px-4 py-2 bg-slate-800 border border-slate-600 rounded-lg text-white focus:ring-2 focus:ring-blue-500"
                                    placeholder="e.g., C123"
                                />
                            </div>

                            <div>
                                <label className="block text-sm font-medium text-slate-300 mb-2">Product ID *</label>
                                <input
                                    type="text"
                                    value={newReview.product_id}
                                    onChange={(e) => setNewReview({ ...newReview, product_id: e.target.value })}
                                    className="w-full px-4 py-2 bg-slate-800 border border-slate-600 rounded-lg text-white focus:ring-2 focus:ring-blue-500"
                                    placeholder="e.g., AMZN-ABC123"
                                />
                            </div>

                            <div>
                                <label className="block text-sm font-medium text-slate-300 mb-2">Product Name *</label>
                                <input
                                    type="text"
                                    value={newReview.product_name}
                                    onChange={(e) => setNewReview({ ...newReview, product_name: e.target.value })}
                                    className="w-full px-4 py-2 bg-slate-800 border border-slate-600 rounded-lg text-white focus:ring-2 focus:ring-blue-500"
                                    placeholder="e.g., Amazing Product"
                                />
                            </div>

                            <div>
                                <label className="block text-sm font-medium text-slate-300 mb-2">Review Text *</label>
                                <textarea
                                    value={newReview.text}
                                    onChange={(e) => setNewReview({ ...newReview, text: e.target.value })}
                                    className="w-full px-4 py-2 bg-slate-800 border border-slate-600 rounded-lg text-white focus:ring-2 focus:ring-blue-500 h-32"
                                    placeholder="Write the review here..."
                                />
                            </div>

                            <div>
                                <label className="block text-sm font-medium text-slate-300 mb-2">Channel</label>
                                <select
                                    value={newReview.channel}
                                    onChange={(e) => setNewReview({ ...newReview, channel: e.target.value })}
                                    className="w-full px-4 py-2 bg-slate-800 border border-slate-600 rounded-lg text-white focus:ring-2 focus:ring-blue-500"
                                >
                                    <option value="web">Web</option>
                                    <option value="app">App</option>
                                    <option value="email">Email</option>
                                    <option value="callcenter">Call Center</option>
                                    <option value="social">Social Media</option>
                                </select>
                            </div>
                        </div>

                        <div className="flex gap-4 mt-6">
                            <button
                                onClick={handleCreate}
                                disabled={createMutation.isLoading}
                                className="flex-1 flex items-center justify-center gap-2 px-6 py-3 bg-gradient-to-r from-green-500 to-emerald-600 text-white rounded-xl font-semibold hover:shadow-lg hover:scale-105 transition-all disabled:opacity-50"
                            >
                                <Save className="w-5 h-5" />
                                {createMutation.isLoading ? 'Creating...' : 'Create Review'}
                            </button>
                            <button
                                onClick={() => setShowCreateModal(false)}
                                className="px-6 py-3 bg-slate-700 text-white rounded-xl font-semibold hover:bg-slate-600 transition-colors"
                            >
                                Cancel
                            </button>
                        </div>
                    </div>
                </div>
            )}

            {/* Edit Modal */}
            {editingReview && (
                <div className="fixed inset-0 bg-black/70 flex items-center justify-center z-50 p-4">
                    <div className="bg-slate-900 rounded-2xl p-8 max-w-2xl w-full border border-slate-700">
                        <div className="flex items-center justify-between mb-6">
                            <h3 className="text-2xl font-bold text-white">Edit Review</h3>
                            <button onClick={() => setEditingReview(null)} className="text-slate-400 hover:text-white">
                                <X className="w-6 h-6" />
                            </button>
                        </div>

                        <div className="space-y-4">
                            <div>
                                <label className="block text-sm font-medium text-slate-300 mb-2">Review Text *</label>
                                <textarea
                                    value={editingReview.text}
                                    onChange={(e) => setEditingReview({ ...editingReview, text: e.target.value })}
                                    className="w-full px-4 py-2 bg-slate-800 border border-slate-600 rounded-lg text-white focus:ring-2 focus:ring-blue-500 h-32"
                                />
                            </div>

                            <div>
                                <label className="block text-sm font-medium text-slate-300 mb-2">Product Name</label>
                                <input
                                    type="text"
                                    value={editingReview.product_name}
                                    onChange={(e) => setEditingReview({ ...editingReview, product_name: e.target.value })}
                                    className="w-full px-4 py-2 bg-slate-800 border border-slate-600 rounded-lg text-white focus:ring-2 focus:ring-blue-500"
                                />
                            </div>

                            <div>
                                <label className="block text-sm font-medium text-slate-300 mb-2">Channel</label>
                                <select
                                    value={editingReview.channel}
                                    onChange={(e) => setEditingReview({ ...editingReview, channel: e.target.value })}
                                    className="w-full px-4 py-2 bg-slate-800 border border-slate-600 rounded-lg text-white focus:ring-2 focus:ring-blue-500"
                                >
                                    <option value="web">Web</option>
                                    <option value="app">App</option>
                                    <option value="email">Email</option>
                                    <option value="callcenter">Call Center</option>
                                    <option value="social">Social Media</option>
                                </select>
                            </div>

                            <div className="bg-blue-500/10 border border-blue-500/30 rounded-lg p-4">
                                <p className="text-sm text-blue-300">
                                    💡 Note: Editing the review text will automatically re-predict the sentiment using our ML model.
                                </p>
                            </div>
                        </div>

                        <div className="flex gap-4 mt-6">
                            <button
                                onClick={handleUpdate}
                                disabled={updateMutation.isLoading}
                                className="flex-1 flex items-center justify-center gap-2 px-6 py-3 bg-gradient-to-r from-blue-500 to-cyan-600 text-white rounded-xl font-semibold hover:shadow-lg hover:scale-105 transition-all disabled:opacity-50"
                            >
                                <Save className="w-5 h-5" />
                                {updateMutation.isLoading ? 'Updating...' : 'Update Review'}
                            </button>
                            <button
                                onClick={() => setEditingReview(null)}
                                className="px-6 py-3 bg-slate-700 text-white rounded-xl font-semibold hover:bg-slate-600 transition-colors"
                            >
                                Cancel
                            </button>
                        </div>
                    </div>
                </div>
            )}
        </div>
    );
}

export default ReviewsManager;
