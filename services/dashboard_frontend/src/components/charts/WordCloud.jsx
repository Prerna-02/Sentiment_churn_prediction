// src/components/charts/WordCloud.jsx
import { useState } from 'react';
import { useQuery } from '@tanstack/react-query';
import { MessageCircle, Info } from 'lucide-react';
import { dashboardAPI } from '../../api/dashboard';
import QueryModal from '../common/QueryModal';
import { MONGO_QUERIES } from '../../data/mongoQueries';

function WordCloud({ filters }) {
  const [modalOpen, setModalOpen] = useState(false);

  const { data, isLoading, isError } = useQuery({
    queryKey: ['wordCloud', filters],
    queryFn: async () => {
      // For now, we'll extract keywords from the product names in negative reviews
      // In a full implementation, you'd add a dedicated backend endpoint
      const productData = await dashboardAPI.getProductSentimentDistribution({
        ...filters,
        limit: 100
      });
      
      // Extract words from product names of high-negative products
      const keywords = {};
      
      productData.products
        .filter(p => p.sentiment_percentages.negative > 40)
        .forEach(p => {
          const words = p.product_name
            .toLowerCase()
            .split(/\s+/)
            .filter(w => w.length > 3 && !['this', 'that', 'with', 'from', 'have'].includes(w));
          
          words.forEach(word => {
            keywords[word] = (keywords[word] || 0) + p.sentiment_distribution.negative;
          });
        });
      
      // Convert to array and sort
      const topKeywords = Object.entries(keywords)
        .map(([word, count]) => ({ word, count }))
        .sort((a, b) => b.count - a.count)
        .slice(0, 30);
      
      return topKeywords;
    },
  });

  if (isError) {
    return (
      <div className="glass-card p-6">
        <h2 className="text-xl font-bold text-white mb-4">Top Negative Keywords</h2>
        <div className="text-red-300 font-medium bg-red-500/20 backdrop-blur-sm p-4 rounded-lg border border-red-400/30">
          Failed to load keyword data.
        </div>
      </div>
    );
  }

  if (isLoading) {
    return (
      <div className="glass-card p-6">
        <h2 className="text-xl font-bold text-white mb-4">Top Negative Keywords</h2>
        <div className="h-80 bg-white/10 rounded-xl animate-pulse shimmer"></div>
      </div>
    );
  }

  const keywords = data || [];
  const maxCount = keywords[0]?.count || 1;

  return (
    <>
      <div className="glass-card p-6 relative group">
        <button
          onClick={() => setModalOpen(true)}
          className="absolute top-4 right-4 p-2 text-white/70 hover:text-white hover:bg-white/20 rounded-full transition-all opacity-0 group-hover:opacity-100 z-10"
          title="View MongoDB Query"
        >
          <Info className="w-5 h-5" />
        </button>

        <div className="flex items-center gap-2 mb-4">
          <div className="p-2 bg-red-500/30 backdrop-blur-sm rounded-lg">
            <MessageCircle className="w-5 h-5 text-white" />
          </div>
          <h2 className="text-xl font-bold text-white drop-shadow-lg">Top Negative Keywords</h2>
        </div>

        {keywords.length === 0 ? (
          <div className="h-80 flex items-center justify-center">
            <p className="text-white/70 font-medium">No keyword data available</p>
          </div>
        ) : (
          <>
            <div className="bg-white/10 backdrop-blur-sm rounded-xl p-6 border border-white/20">
              <div className="flex flex-wrap gap-3 justify-center items-center min-h-[320px]">
                {keywords.map((kw, idx) => {
                  // Calculate font size based on frequency
                  const sizeRatio = kw.count / maxCount;
                  const fontSize = 14 + (sizeRatio * 32); // 14px to 46px
                  const opacity = 0.6 + (sizeRatio * 0.4); // 0.6 to 1.0
                  
                  return (
                    <span
                      key={idx}
                      className="text-white font-bold hover:text-red-300 transition-all duration-300 cursor-default hover:scale-110"
                      style={{
                        fontSize: `${fontSize}px`,
                        opacity: opacity,
                        textShadow: '0 2px 10px rgba(239, 68, 68, 0.5)',
                      }}
                      title={`${kw.count} negative mentions`}
                    >
                      {kw.word}
                    </span>
                  );
                })}
              </div>
            </div>

            <div className="mt-4 p-3 bg-red-500/20 backdrop-blur-md rounded-lg border border-red-400/30">
              <p className="text-sm text-white font-medium">
                <strong>💬 Word Cloud:</strong> Keywords from products with high negative sentiment. Larger = more complaints. Hover for count.
              </p>
            </div>
          </>
        )}
      </div>

      <QueryModal
        isOpen={modalOpen}
        onClose={() => setModalOpen(false)}
        title={MONGO_QUERIES.wordCloud.title}
        query={MONGO_QUERIES.wordCloud.query}
        executionTime={MONGO_QUERIES.wordCloud.executionTime}
        explanation={MONGO_QUERIES.wordCloud.explanation}
      />
    </>
  );
}

export default WordCloud;


