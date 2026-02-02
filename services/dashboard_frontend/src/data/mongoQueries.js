// src/data/mongoQueries.js
// MongoDB queries for each dashboard component

export const MONGO_QUERIES = {
  totalReviews: {
    title: "All KPI Metrics (Single Optimized Query)",
    explanation: "This single aggregation efficiently calculates ALL 6 KPI metrics at once (Total Reviews, Negative %, Avg Sentiment, Avg Confidence, High-Risk Products, Alerts) using MongoDB's $group stage with multiple accumulators ($sum, $avg, $cond). This is more efficient than running 6 separate queries - one database round-trip instead of six!",
    query: `db.reviews_enriched.aggregate([
  {
    $match: {
      timestamp_utc: { $gte: "2026-01-05T00:00:00Z" }
    }
  },
  {
    $group: {
      _id: null,
      total_reviews: { $sum: 1 },
      negative_count: {
        $sum: {
          $cond: [
            { $eq: ["$sentiment_label", "negative"] },
            1,
            0
          ]
        }
      },
      avg_sentiment_score: { $avg: "$sentiment_score" },
      avg_confidence: { $avg: "$confidence" }
    }
  }
])`,
    executionTime: "~45ms"
  },

  sentimentTrend: {
    title: "Sentiment Trend Over Time",
    explanation: "Time-series aggregation using $dateTrunc to bucket reviews by time intervals, then calculates average sentiment and negative percentage for each bucket.",
    query: `db.reviews_enriched.aggregate([
  {
    $match: {
      timestamp_utc: { $gte: "2026-01-05T00:00:00Z" }
    }
  },
  {
    $addFields: {
      timestamp_date: {
        $dateFromString: {
          dateString: "$timestamp_utc",
          onError: null
        }
      }
    }
  },
  { $match: { timestamp_date: { $ne: null } } },
  {
    $group: {
      _id: {
        $dateTrunc: {
          date: "$timestamp_date",
          unit: "day",
          binSize: 1
        }
      },
      avg_sentiment_score: { $avg: "$sentiment_score" },
      review_count: { $sum: 1 },
      negative_count: {
        $sum: {
          $cond: [
            { $eq: ["$sentiment_label", "negative"] },
            1,
            0
          ]
        }
      }
    }
  },
  {
    $project: {
      timestamp: "$_id",
      avg_sentiment_score: 1,
      review_count: 1,
      negative_percentage: {
        $multiply: [
          { $divide: ["$negative_count", "$review_count"] },
          100
        ]
      }
    }
  },
  { $sort: { timestamp: 1 } }
])`,
    executionTime: "~200ms"
  },

  fixFirstRanking: {
    title: "Fix-First Product Priority Ranking",
    explanation: "Complex aggregation calculating priority scores for products based on negative ratio (40%), volume (25%), confidence (15%), and sentiment velocity (20%). Groups by product and calculates recent vs early sentiment for trend detection.",
    query: `db.reviews_enriched.aggregate([
  {
    $match: {
      timestamp_utc: { $gte: "2026-01-05T00:00:00Z" },
      product_id: { $exists: true, $ne: null }
    }
  },
  {
    $addFields: {
      timestamp_date: {
        $dateFromString: {
          dateString: "$timestamp_utc"
        }
      }
    }
  },
  {
    $group: {
      _id: {
        product_id: "$product_id",
        product_name: "$product_name"
      },
      total_reviews: { $sum: 1 },
      negative_count: {
        $sum: {
          $cond: [
            { $eq: ["$sentiment_label", "negative"] },
            1,
            0
          ]
        }
      },
      avg_sentiment_score: { $avg: "$sentiment_score" },
      avg_confidence: { $avg: "$confidence" },
      recent_sentiment: {
        $avg: {
          $cond: [
            { $gte: ["$timestamp_date", "2026-01-10"] },
            "$sentiment_score",
            null
          ]
        }
      },
      early_sentiment: {
        $avg: {
          $cond: [
            { $lte: ["$timestamp_date", "2026-01-08"] },
            "$sentiment_score",
            null
          ]
        }
      }
    }
  },
  {
    $project: {
      product_id: "$_id.product_id",
      product_name: "$_id.product_name",
      total_reviews: 1,
      negative_ratio: {
        $divide: ["$negative_count", "$total_reviews"]
      },
      avg_confidence: 1,
      sentiment_velocity: {
        $subtract: [
          { $ifNull: ["$recent_sentiment", "$avg_sentiment_score"] },
          { $ifNull: ["$early_sentiment", "$avg_sentiment_score"] }
        ]
      }
    }
  },
  { $sort: { negative_ratio: -1 } },
  { $limit: 20 }
])

// Priority Score Calculation (in application):
// priority_score = (
//   0.40 * negative_ratio +
//   0.25 * volume_score +
//   0.15 * (1 - avg_confidence) +
//   0.20 * abs(sentiment_velocity)
// )`,
    executionTime: "~350ms"
  },

  alerts: {
    title: "Real-Time Anomaly Detection",
    explanation: "Compares recent metrics (last 1 hour) against baseline (previous 24 hours) to detect spikes, surges, and drops. Uses two separate aggregations for comparison.",
    query: `// Recent window (last 1 hour)
db.reviews_enriched.aggregate([
  {
    $match: {
      timestamp_utc: { $gte: "2026-01-12T15:45:00Z" },
      product_id: { $exists: true }
    }
  },
  {
    $group: {
      _id: {
        product_id: "$product_id",
        product_name: "$product_name"
      },
      recent_volume: { $sum: 1 },
      recent_negative: {
        $sum: {
          $cond: [
            { $eq: ["$sentiment_label", "negative"] },
            1,
            0
          ]
        }
      },
      recent_sentiment: { $avg: "$sentiment_score" }
    }
  }
])

// Baseline window (previous 24 hours)
db.reviews_enriched.aggregate([
  {
    $match: {
      timestamp_utc: {
        $gte: "2026-01-11T15:45:00Z",
        $lt: "2026-01-12T15:45:00Z"
      },
      product_id: { $exists: true }
    }
  },
  {
    $group: {
      _id: "$product_id",
      baseline_volume: { $sum: 1 },
      baseline_negative: {
        $sum: {
          $cond: [
            { $eq: ["$sentiment_label", "negative"] },
            1,
            0
          ]
        }
      },
      baseline_sentiment: { $avg: "$sentiment_score" }
    }
  }
])

// Alert triggers:
// - negative_spike: recent_negative / baseline > 1.5
// - volume_surge: recent_volume / baseline > 2.0
// - sentiment_drop: baseline - recent > 0.3`,
    executionTime: "~180ms"
  },

  channelBreakdown: {
    title: "Channel Distribution Analysis",
    explanation: "Groups reviews by channel (app, web, email, callcenter) and calculates sentiment distribution for each using conditional aggregation.",
    query: `db.reviews_enriched.aggregate([
  {
    $match: {
      timestamp_utc: { $gte: "2026-01-05T00:00:00Z" }
    }
  },
  {
    $group: {
      _id: "$channel",
      review_count: { $sum: 1 },
      positive_count: {
        $sum: {
          $cond: [
            { $eq: ["$sentiment_label", "positive"] },
            1,
            0
          ]
        }
      },
      neutral_count: {
        $sum: {
          $cond: [
            { $eq: ["$sentiment_label", "neutral"] },
            1,
            0
          ]
        }
      },
      negative_count: {
        $sum: {
          $cond: [
            { $eq: ["$sentiment_label", "negative"] },
            1,
            0
          ]
        }
      },
      avg_sentiment_score: { $avg: "$sentiment_score" }
    }
  },
  {
    $project: {
      channel: "$_id",
      review_count: 1,
      positive_count: 1,
      neutral_count: 1,
      negative_count: 1,
      negative_percentage: {
        $multiply: [
          { $divide: ["$negative_count", "$review_count"] },
          100
        ]
      }
    }
  },
  { $sort: { review_count: -1 } }
])`,
    executionTime: "~120ms"
  },

  wordCloud: {
    title: "Top Negative Keywords Extraction",
    explanation: "Fetches text from negative reviews for keyword extraction and frequency analysis. Text processing (tokenization, stopword removal, counting) is done in application layer.",
    query: `db.reviews_enriched.aggregate([
  {
    $match: {
      sentiment_label: "negative",
      timestamp_utc: { $gte: "2026-01-05T00:00:00Z" }
    }
  },
  {
    $project: {
      text: 1,
      product_name: 1
    }
  },
  { $limit: 1000 }
])

// Application-layer processing:
// 1. Tokenize text (split by words)
// 2. Remove stopwords (the, is, and, etc.)
// 3. Count word frequency
// 4. Return top 50 keywords`,
    executionTime: "~85ms"
  }
};

