// src/components/common/QueryModal.jsx
import { X, Copy, Check, Database } from 'lucide-react';
import { useState } from 'react';

function QueryModal({ isOpen, onClose, title, query, executionTime, explanation }) {
  const [copied, setCopied] = useState(false);

  if (!isOpen) return null;

  const handleCopy = () => {
    navigator.clipboard.writeText(query);
    setCopied(true);
    setTimeout(() => setCopied(false), 2000);
  };

  return (
    <div className="fixed inset-0 bg-black bg-opacity-50 flex items-center justify-center z-50 p-4">
      <div className="bg-white rounded-lg shadow-2xl max-w-4xl w-full max-h-[90vh] overflow-hidden">
        {/* Header */}
        <div className="bg-gradient-to-r from-blue-600 to-blue-700 text-white p-6 flex items-center justify-between">
          <div className="flex items-center gap-3">
            <Database className="w-6 h-6" />
            <div>
              <h2 className="text-xl font-bold">MongoDB Query</h2>
              <p className="text-blue-100 text-sm mt-1">{title}</p>
            </div>
          </div>
          <button
            onClick={onClose}
            className="p-2 hover:bg-blue-800 rounded-full transition-colors"
          >
            <X className="w-5 h-5" />
          </button>
        </div>

        {/* Content */}
        <div className="p-6 overflow-y-auto max-h-[calc(90vh-200px)]">
          {/* Explanation */}
          {explanation && (
            <div className="mb-4 p-4 bg-blue-50 border border-blue-200 rounded-lg">
              <p className="text-sm text-blue-900">
                <strong>What this query does:</strong> {explanation}
              </p>
            </div>
          )}

          {/* Query Code */}
          <div className="relative">
            <pre className="bg-gray-900 text-gray-100 p-6 rounded-lg overflow-x-auto text-sm font-mono leading-relaxed">
              <code>{query}</code>
            </pre>
            <button
              onClick={handleCopy}
              className={`absolute top-4 right-4 flex items-center gap-2 px-3 py-2 rounded-md transition-all ${
                copied
                  ? 'bg-green-600 text-white'
                  : 'bg-gray-700 text-gray-200 hover:bg-gray-600'
              }`}
            >
              {copied ? (
                <>
                  <Check className="w-4 h-4" />
                  <span className="text-sm">Copied!</span>
                </>
              ) : (
                <>
                  <Copy className="w-4 h-4" />
                  <span className="text-sm">Copy Query</span>
                </>
              )}
            </button>
          </div>

          {/* Execution Time */}
          {executionTime && (
            <div className="mt-4 flex items-center gap-2 text-sm text-gray-600">
              <span className="font-medium">⏱️ Execution Time:</span>
              <span className="px-2 py-1 bg-green-100 text-green-800 rounded">
                {executionTime}
              </span>
            </div>
          )}

          {/* MongoDB Info */}
          <div className="mt-6 p-4 bg-gray-50 rounded-lg border border-gray-200">
            <h3 className="font-semibold text-gray-900 mb-2 flex items-center gap-2">
              <Database className="w-4 h-4" />
              MongoDB Aggregation Pipeline
            </h3>
            <p className="text-sm text-gray-700 leading-relaxed">
              This query uses MongoDB's aggregation framework to process data in stages.
              You can copy this query and run it directly in MongoDB shell or Compass to test it.
            </p>
          </div>
        </div>

        {/* Footer */}
        <div className="bg-gray-50 px-6 py-4 flex justify-end gap-3 border-t border-gray-200">
          <button
            onClick={onClose}
            className="px-4 py-2 bg-gray-200 text-gray-800 rounded-md hover:bg-gray-300 transition-colors"
          >
            Close
          </button>
        </div>
      </div>
    </div>
  );
}

export default QueryModal;


