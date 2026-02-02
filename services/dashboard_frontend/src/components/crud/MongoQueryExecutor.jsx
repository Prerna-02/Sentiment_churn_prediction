// src/components/crud/MongoQueryExecutor.jsx
import { useState } from 'react';
import { useMutation } from '@tanstack/react-query';
import { Play, BookOpen, Copy, Check } from 'lucide-react';

const API_BASE = 'http://localhost:3001/api';

// Execute MongoDB query
const executeQuery = async (query) => {
    const response = await fetch(`${API_BASE}/execute-query`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ query }),
    });
    if (!response.ok) throw new Error('Failed to execute query');
    return response.json();
};

// Fetch query examples
const fetchExamples = async () => {
    const response = await fetch(`${API_BASE}/query-examples`);
    if (!response.ok) throw new Error('Failed to fetch examples');
    return response.json();
};

function MongoQueryExecutor() {
    const [query, setQuery] = useState('db.reviews_enriched.find({})');
    const [result, setResult] = useState(null);
    const [showExamples, setShowExamples] = useState(false);
    const [examples, setExamples] = useState([]);
    const [copiedExample, setCopiedExample] = useState(null);

    // Mutation for executing query
    const executeMutation = useMutation({
        mutationFn: executeQuery,
        onSuccess: (data) => {
            setResult(data);
        },
        onError: (error) => {
            setResult({
                success: false,
                error: error.message,
                result: null,
                count: 0,
            });
        },
    });

    const handleExecute = () => {
        if (!query.trim()) {
            alert('Please enter a query');
            return;
        }
        executeMutation.mutate(query);
    };

    const handleLoadExamples = async () => {
        if (examples.length === 0) {
            const data = await fetchExamples();
            setExamples(data.examples);
        }
        setShowExamples(!showExamples);
    };

    const handleCopyExample = (exampleQuery) => {
        setQuery(exampleQuery);
        setCopiedExample(exampleQuery);
        setTimeout(() => setCopiedExample(null), 2000);
    };

    return (
        <div className="glass-card p-8">
            {/* Header */}
            <div className="mb-8">
                <h2 className="text-3xl font-bold text-white mb-2">MongoDB Query Executor</h2>
                <p className="text-slate-300">Write and execute raw MongoDB queries directly</p>
            </div>

            {/* Info Box */}
            <div className="bg-blue-500/10 border border-blue-500/30 rounded-lg p-4 mb-6">
                <h3 className="text-blue-300 font-semibold mb-2">💡 How It Works</h3>
                <p className="text-sm text-blue-200 mb-2">
                    Type your MongoDB query below and click "Execute". The query will be sent to the API,
                    which will execute it on the MongoDB database and return the results.
                </p>
                <p className="text-xs text-blue-300">
                    <strong>Collection:</strong> reviews_enriched | <strong>Limit:</strong> 100 documents per query
                </p>
            </div>

            {/* Examples Button */}
            <div className="mb-4">
                <button
                    onClick={handleLoadExamples}
                    className="flex items-center gap-2 px-4 py-2 bg-purple-500/20 border border-purple-500/40 text-purple-300 rounded-lg hover:bg-purple-500/30 transition-colors"
                >
                    <BookOpen className="w-4 h-4" />
                    {showExamples ? 'Hide Examples' : 'Show Query Examples'}
                </button>
            </div>

            {/* Examples Panel */}
            {showExamples && (
                <div className="bg-slate-800/50 border border-slate-700 rounded-lg p-4 mb-6 max-h-96 overflow-y-auto">
                    <h3 className="text-white font-semibold mb-4">Query Examples</h3>
                    <div className="space-y-3">
                        {examples.map((example, idx) => (
                            <div key={idx} className="bg-slate-900/50 rounded-lg p-3 border border-slate-700">
                                <div className="flex items-start justify-between mb-2">
                                    <div>
                                        <h4 className="text-cyan-300 font-medium text-sm">{example.name}</h4>
                                        <p className="text-xs text-slate-400 mt-1">{example.description}</p>
                                    </div>
                                    <button
                                        onClick={() => handleCopyExample(example.query)}
                                        className="flex items-center gap-1 px-3 py-1 bg-green-500/20 text-green-300 rounded text-xs hover:bg-green-500/30 transition-colors"
                                    >
                                        {copiedExample === example.query ? (
                                            <>
                                                <Check className="w-3 h-3" />
                                                Copied!
                                            </>
                                        ) : (
                                            <>
                                                <Copy className="w-3 h-3" />
                                                Use This
                                            </>
                                        )}
                                    </button>
                                </div>
                                <code className="text-xs text-green-400 bg-black/30 px-2 py-1 rounded block overflow-x-auto">
                                    {example.query}
                                </code>
                            </div>
                        ))}
                    </div>
                </div>
            )}

            {/* Query Input */}
            <div className="mb-6">
                <label className="block text-sm font-medium text-slate-300 mb-2">
                    MongoDB Query
                </label>
                <div className="relative">
                    <textarea
                        value={query}
                        onChange={(e) => setQuery(e.target.value)}
                        className="w-full px-4 py-3 bg-slate-900 border border-slate-600 rounded-lg text-white font-mono text-sm focus:ring-2 focus:ring-cyan-500 focus:border-cyan-500 h-32"
                        placeholder='db.reviews_enriched.find({"sentiment_label": "positive"})'
                    />
                </div>
                <p className="text-xs text-slate-400 mt-2">
                    Supported operations: find, countDocuments, aggregate, insertOne, updateOne, updateMany, deleteOne, deleteMany
                </p>
            </div>

            {/* Execute Button */}
            <button
                onClick={handleExecute}
                disabled={executeMutation.isLoading}
                className="flex items-center gap-2 px-6 py-3 bg-gradient-to-r from-cyan-500 to-blue-600 text-white rounded-xl font-semibold hover:shadow-lg hover:scale-105 transition-all disabled:opacity-50 disabled:cursor-not-allowed mb-6"
            >
                <Play className="w-5 h-5" />
                {executeMutation.isLoading ? 'Executing...' : 'Execute Query'}
            </button>

            {/* Results */}
            {result && (
                <div className="mt-6">
                    <h3 className="text-xl font-bold text-white mb-4">Query Results</h3>

                    {/* Success/Error Banner */}
                    <div className={`rounded-lg p-4 mb-4 ${result.success
                        ? 'bg-green-500/10 border border-green-500/30'
                        : 'bg-red-500/10 border border-red-500/30'
                        }`}>
                        <div className="flex items-center justify-between">
                            <div>
                                <p className={`font-semibold ${result.success ? 'text-green-300' : 'text-red-300'}`}>
                                    {result.success ? '✅ Query Executed Successfully' : '❌ Query Failed'}
                                </p>
                                <p className="text-xs text-slate-400 mt-1">
                                    Query: <code className="text-cyan-300">{result.query_executed}</code>
                                </p>
                            </div>
                            {result.success && (
                                <div className="text-right">
                                    <p className="text-2xl font-bold text-green-300">{result.count}</p>
                                    <p className="text-xs text-slate-400">documents</p>
                                </div>
                            )}
                        </div>
                    </div>

                    {/* Error Message */}
                    {result.error && (
                        <div className="bg-red-900/20 border border-red-500/40 rounded-lg p-4 mb-4">
                            <p className="text-red-300 font-semibold mb-2">Error Details:</p>
                            <code className="text-sm text-red-200 block bg-black/30 p-3 rounded overflow-x-auto">
                                {result.error}
                            </code>
                        </div>
                    )}

                    {/* Result Data */}
                    {result.success && result.result && (
                        <div className="bg-slate-900 border border-slate-700 rounded-lg p-4 overflow-x-auto">
                            <pre className="text-sm text-green-400 font-mono whitespace-pre-wrap">
                                {JSON.stringify(result.result, null, 2)}
                            </pre>
                        </div>
                    )}
                </div>
            )}

            {/* Tips */}
            <div className="mt-8 bg-yellow-500/10 border border-yellow-500/30 rounded-lg p-4">
                <h3 className="text-yellow-300 font-semibold mb-2">💡 Tips</h3>
                <ul className="text-sm text-yellow-200 space-y-1 list-disc list-inside">
                    <li>Use double quotes for strings in JSON: <code className="text-cyan-300">{`{"sentiment_label": "positive"}`}</code></li>
                    <li>Queries are limited to 100 documents for safety</li>
                    <li>All changes are immediately reflected in MongoDB</li>
                    <li>Use <code className="text-cyan-300">countDocuments</code> to check totals before deleting</li>
                    <li>Click "Show Query Examples" above for common query patterns</li>
                </ul>
            </div>
        </div>
    );
}

export default MongoQueryExecutor;
