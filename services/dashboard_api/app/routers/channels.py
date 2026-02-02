# services/dashboard_api/app/routers/channels.py

from fastapi import APIRouter, Query, HTTPException
from typing import Optional

from app.utils.mongodb import mongo_conn
from app.utils.time_utils import get_time_filter


router = APIRouter()


@router.get("/channel-breakdown")
async def get_channel_breakdown(
    time_window: str = Query("24h", regex="^(1h|24h|7d|30d|all)$"),
    product_id: Optional[str] = None,
    channel: Optional[str] = None
):
    """
    Get review breakdown by channel with sentiment distribution.
    
    Returns for each channel:
    - review_count
    - percentage of total reviews
    - sentiment_distribution (positive, neutral, negative counts)
    - avg_sentiment_score
    - negative_percentage
    """
    try:
        collection = mongo_conn.get_collection()
        filter_query = get_time_filter(time_window, product_id=product_id, channel=channel)
        
        # MongoDB aggregation pipeline
        pipeline = [
            {"$match": filter_query},
            {
                "$group": {
                    "_id": "$channel",
                    "review_count": {"$sum": 1},
                    "positive_count": {
                        "$sum": {"$cond": [{"$eq": ["$sentiment_label", "positive"]}, 1, 0]}
                    },
                    "neutral_count": {
                        "$sum": {"$cond": [{"$eq": ["$sentiment_label", "neutral"]}, 1, 0]}
                    },
                    "negative_count": {
                        "$sum": {"$cond": [{"$eq": ["$sentiment_label", "negative"]}, 1, 0]}
                    },
                    "avg_sentiment_score": {"$avg": "$sentiment_score"}
                }
            },
            {
                "$project": {
                    "channel": "$_id",
                    "review_count": 1,
                    "positive_count": 1,
                    "neutral_count": 1,
                    "negative_count": 1,
                    "avg_sentiment_score": 1,
                    "negative_percentage": {
                        "$multiply": [
                            {"$divide": ["$negative_count", "$review_count"]},
                            100
                        ]
                    }
                }
            },
            {"$sort": {"review_count": -1}}
        ]
        
        results = list(collection.aggregate(pipeline))
        
        # Calculate total reviews for percentage
        total_reviews = sum(item["review_count"] for item in results)
        
        # Format response
        channels = []
        for item in results:
            channels.append({
                "channel": item["channel"] or "unknown",
                "review_count": item["review_count"],
                "percentage": round((item["review_count"] / total_reviews * 100) if total_reviews > 0 else 0, 2),
                "sentiment_distribution": {
                    "positive": item["positive_count"],
                    "neutral": item["neutral_count"],
                    "negative": item["negative_count"]
                },
                "avg_sentiment_score": round(item["avg_sentiment_score"] or 0.0, 3),
                "negative_percentage": round(item["negative_percentage"], 2)
            })
        
        return {
            "time_window": time_window,
            "total_reviews": total_reviews,
            "channels": channels
        }
    
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error fetching channel breakdown: {str(e)}")

