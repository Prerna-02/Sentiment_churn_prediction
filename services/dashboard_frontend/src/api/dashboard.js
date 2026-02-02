// src/api/dashboard.js
import apiClient from './client';

export const dashboardAPI = {
  // Health check
  getHealth: () => apiClient.get('/health'),

  // KPIs
  getKPIs: (params = {}) => {
    const { timeWindow = '24h', productId, channel } = params;
    return apiClient.get('/kpis', {
      params: {
        time_window: timeWindow,
        product_id: productId,
        channel,
      },
    });
  },

  // Sentiment Trend
  getSentimentTrend: (params = {}) => {
    const { timeWindow = '24h', productId, channel } = params;
    return apiClient.get('/sentiment-trend', {
      params: {
        time_window: timeWindow,
        product_id: productId,
        channel,
      },
    });
  },

  // Fix-First Ranking
  getFixFirstRanking: (params = {}) => {
    const { timeWindow = '7d', limit = 20, sortBy = 'priority_score' } = params;
    return apiClient.get('/fix-first-ranking', {
      params: {
        time_window: timeWindow,
        limit,
        sort_by: sortBy,
      },
    });
  },

  // Alerts
  getAlerts: (params = {}) => {
    const { timeWindow = '24h', severity = 'all', limit = 50 } = params;
    return apiClient.get('/alerts', {
      params: {
        time_window: timeWindow,
        severity,
        limit,
      },
    });
  },

  // Channel Breakdown
  getChannelBreakdown: (params = {}) => {
    const { timeWindow = '24h', productId, channel } = params;
    return apiClient.get('/channel-breakdown', {
      params: {
        time_window: timeWindow,
        product_id: productId,
        channel: channel,
      },
    });
  },

  // Product Sentiment Distribution
  getProductSentimentDistribution: (params = {}) => {
    const { timeWindow = '7d', limit = 20, channel } = params;
    return apiClient.get('/product-sentiment-distribution', {
      params: {
        time_window: timeWindow,
        limit,
        channel,
      },
    });
  },

  // Churn over time
  getChurnOverTime: (params = {}) => {
    const { timeWindow = '24h', productId, channel } = params;
    return apiClient.get('/churn-over-time', {
      params: {
        time_window: timeWindow,
        product_id: productId,
        channel,
      },
    });
  },
};

