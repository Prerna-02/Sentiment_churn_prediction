# services/dashboard_api/app/routers/sentiment_trend.py

from fastapi import APIRouter, Query, HTTPException
from datetime import datetime, timezone
from typing import Optional

from app.utils.mongodb import mongo_conn
from app.utils.time_utils import get_time_filter, parse_time_window


router = APIRouter()


@router.get("/sentiment-trend")
async def get_sentiment_trend(
    time_window: str = Query("24h", regex="^(1h|24h|7d|30d)$"),
    product_id: Optional[str] = None,
    channel: Optional[str] = None
):
    """
    Get sentiment trend data over time with automatic granularity.
    
    Time windows and granularities:
    - 1h: 5-minute buckets
    - 24h: 1-hour buckets
    - 7d: 1-day buckets
    - 30d: 1-day buckets
    
    Returns time-series data with:
    - timestamp
    - avg_sentiment_score
    - negative_percentage
    - review_count
    """
    try:
        collection = mongo_conn.get_collection()
        start_time, granularity = parse_time_window(time_window)
        filter_query = get_time_filter(time_window, product_id, channel)
        
        # Determine bucket size in milliseconds
        if granularity == "5m":
            bucket_ms = 5 * 60 * 1000
        elif granularity == "1h":
            bucket_ms = 60 * 60 * 1000
        else:  # "1d"
            bucket_ms = 24 * 60 * 60 * 1000
        
        # MongoDB aggregation pipeline for time-series bucketing
        pipeline = [
            {"$match": filter_query},
            {
                "$addFields": {
                    "timestamp_date": {
                        "$dateFromString": {
                            "dateString": "$timestamp_utc",
                            "onError": None
                        }
                    }
                }
            },
            {"$match": {"timestamp_date": {"$ne": None}}},
            {
                "$group": {
                    "_id": {
                        "$dateTrunc": {
                            "date": "$timestamp_date",
                            "unit": "minute" if granularity == "5m" else ("hour" if granularity == "1h" else "day"),
                            "binSize": 5 if granularity == "5m" else 1
                        }
                    },
                    "avg_sentiment_score": {"$avg": "$sentiment_score"},
                    "review_count": {"$sum": 1},
                    "negative_count": {
                        "$sum": {"$cond": [{"$eq": ["$sentiment_label", "negative"]}, 1, 0]}
                    }
                }
            },
            {
                "$project": {
                    "timestamp": "$_id",
                    "avg_sentiment_score": 1,
                    "review_count": 1,
                    "negative_percentage": {
                        "$multiply": [
                            {"$divide": ["$negative_count", "$review_count"]},
                            100
                        ]
                    }
                }
            },
            {"$sort": {"timestamp": 1}}
        ]
        
        results = list(collection.aggregate(pipeline))
        
        # Format response
        data = []
        for item in results:
            data.append({
                "timestamp": item["timestamp"].isoformat() if isinstance(item["timestamp"], datetime) else item["timestamp"],
                "avg_sentiment_score": round(item["avg_sentiment_score"], 3),
                "negative_percentage": round(item["negative_percentage"], 2),
                "review_count": item["review_count"]
            })
        
        return {
            "time_window": time_window,
            "granularity": granularity,
            "data": data
        }
    
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error fetching sentiment trend: {str(e)}")

