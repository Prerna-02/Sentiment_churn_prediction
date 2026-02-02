# services/dashboard_api/app/routers/fix_first.py

from fastapi import APIRouter, Query, HTTPException
from typing import Optional
from datetime import datetime, timezone

from app.utils.mongodb import mongo_conn
from app.utils.time_utils import get_time_filter, parse_time_window


router = APIRouter()


def calculate_priority_score(
    negative_ratio: float,
    total_reviews: int,
    avg_confidence: float,
    sentiment_velocity: float,
    max_reviews: int = 1000
) -> float:
    """
    Calculate priority score for a product.
    
    Formula:
    priority_score = (
        0.40 * negative_ratio +           # 40%: % negative reviews
        0.25 * volume_score +              # 25%: normalized review count
        0.15 * (1 - avg_confidence) +     # 15%: low confidence = higher priority
        0.20 * abs(sentiment_velocity)    # 20%: rapid sentiment change
    )
    
    Args:
        negative_ratio: Ratio of negative reviews (0.0 to 1.0)
        total_reviews: Total review count
        avg_confidence: Average confidence (0.0 to 1.0)
        sentiment_velocity: Sentiment change rate (-1.0 to 1.0)
        max_reviews: Normalization factor for volume
    
    Returns:
        Priority score (0.0 to 1.0)
    """
    # Normalize review count (0 to 1, capped at max_reviews)
    volume_score = min(total_reviews / max_reviews, 1.0)
    
    # Calculate weighted score
    score = (
        0.40 * negative_ratio +
        0.25 * volume_score +
        0.15 * (1.0 - avg_confidence) +
        0.20 * abs(sentiment_velocity)
    )
    
    return min(max(score, 0.0), 1.0)  # Clamp to [0, 1]


@router.get("/fix-first-ranking")
async def get_fix_first_ranking(
    time_window: str = Query("7d", regex="^(1h|24h|7d|30d|all)$"),
    limit: int = Query(20, ge=1, le=100),
    sort_by: str = Query("priority_score", regex="^(priority_score|negative_percentage|review_count)$")
):
    """
    Get product ranking for "Fix First" prioritization.
    
    Ranks products based on a priority score that considers:
    - Negative review ratio (40% weight)
    - Review volume (25% weight)
    - Low confidence (15% weight)
    - Sentiment velocity / change (20% weight)
    
    Returns:
        List of products sorted by priority score (descending)
    """
    try:
        collection = mongo_conn.get_collection()
        start_time, _ = parse_time_window(time_window)
        filter_query = get_time_filter(time_window)
        
        # Only include products with product_id
        filter_query["product_id"] = {"$exists": True, "$ne": None}
        
        # Calculate time split for velocity (first 25% vs last 25% of time window)
        time_range = datetime.now(timezone.utc) - start_time
        velocity_window = time_range * 0.25  # 25% of total window
        recent_start = datetime.now(timezone.utc) - velocity_window
        early_end = start_time + velocity_window
        
        # Main aggregation pipeline
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
                        "product_id": "$product_id",
                        "product_name": "$product_name"
                    },
                    "total_reviews": {"$sum": 1},
                    "negative_count": {
                        "$sum": {"$cond": [{"$eq": ["$sentiment_label", "negative"]}, 1, 0]}
                    },
                    "avg_sentiment_score": {"$avg": "$sentiment_score"},
                    "avg_confidence": {"$avg": "$confidence"},
                    # Recent sentiment (last 25%)
                    "recent_sentiment": {
                        "$avg": {
                            "$cond": [
                                {"$gte": ["$timestamp_date", recent_start]},
                                "$sentiment_score",
                                None
                            ]
                        }
                    },
                    # Early sentiment (first 25%)
                    "early_sentiment": {
                        "$avg": {
                            "$cond": [
                                {"$lte": ["$timestamp_date", early_end]},
                                "$sentiment_score",
                                None
                            ]
                        }
                    }
                }
            },
            {
                "$project": {
                    "product_id": "$_id.product_id",
                    "product_name": "$_id.product_name",
                    "total_reviews": 1,
                    "negative_count": 1,
                    "negative_ratio": {
                        "$divide": ["$negative_count", "$total_reviews"]
                    },
                    "avg_sentiment_score": 1,
                    "avg_confidence": 1,
                    "recent_sentiment": 1,
                    "early_sentiment": 1,
                    "sentiment_velocity": {
                        "$subtract": [
                            {"$ifNull": ["$recent_sentiment", "$avg_sentiment_score"]},
                            {"$ifNull": ["$early_sentiment", "$avg_sentiment_score"]}
                        ]
                    }
                }
            }
        ]
        
        results = list(collection.aggregate(pipeline))
        
        # Calculate priority scores and determine max_reviews for normalization
        max_reviews = max([r["total_reviews"] for r in results]) if results else 1
        
        products = []
        for item in results:
            negative_ratio = item["negative_ratio"]
            total_reviews = item["total_reviews"]
            avg_confidence = item["avg_confidence"] or 0.8  # Default if None
            sentiment_velocity = item["sentiment_velocity"] or 0.0
            
            priority_score = calculate_priority_score(
                negative_ratio, total_reviews, avg_confidence, sentiment_velocity, max_reviews
            )
            
            # Determine trend
            if sentiment_velocity > 0.1:
                trend = "up"
            elif sentiment_velocity < -0.1:
                trend = "down"
            else:
                trend = "stable"
            
            products.append({
                "product_id": item["product_id"],
                "product_name": item["product_name"] or "Unknown Product",
                "priority_score": round(priority_score, 3),
                "negative_percentage": round(negative_ratio * 100, 2),
                "review_count": total_reviews,
                "avg_sentiment_score": round(item["avg_sentiment_score"] or 0.0, 3),
                "avg_confidence": round(avg_confidence, 3),
                "trend": trend,
                "sentiment_velocity": round(sentiment_velocity, 3)
            })
        
        # Sort products
        if sort_by == "priority_score":
            products.sort(key=lambda x: x["priority_score"], reverse=True)
        elif sort_by == "negative_percentage":
            products.sort(key=lambda x: x["negative_percentage"], reverse=True)
        elif sort_by == "review_count":
            products.sort(key=lambda x: x["review_count"], reverse=True)
        
        # Add priority rank
        for idx, product in enumerate(products[:limit], start=1):
            product["priority_rank"] = idx
        
        return {
            "time_window": time_window,
            "products": products[:limit]
        }
    
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error fetching fix-first ranking: {str(e)}")

