# services/dashboard_api/app/routers/products.py

from fastapi import APIRouter, Query, HTTPException
from typing import Optional

from app.utils.mongodb import mongo_conn
from app.utils.time_utils import get_time_filter


router = APIRouter()


@router.get("/product-sentiment-distribution")
async def get_product_sentiment_distribution(
    time_window: str = Query("7d", regex="^(1h|24h|7d|30d|all)$"),
    limit: int = Query(20, ge=1, le=100),
    channel: Optional[str] = None
):
    """
    Get sentiment distribution for each product.
    
    Shows for each product:
    - total_reviews
    - sentiment_distribution (counts)
    - sentiment_percentages
    - avg_sentiment_score
    
    Sorted by total review count (descending).
    """
    try:
        collection = mongo_conn.get_collection()
        filter_query = get_time_filter(time_window, channel=channel)
        
        # Only include products with product_id
        filter_query["product_id"] = {"$exists": True, "$ne": None}
        
        # MongoDB aggregation pipeline
        pipeline = [
            {"$match": filter_query},
            {
                "$group": {
                    "_id": {
                        "product_id": "$product_id",
                        "product_name": "$product_name"
                    },
                    "total_reviews": {"$sum": 1},
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
                    "product_id": "$_id.product_id",
                    "product_name": "$_id.product_name",
                    "total_reviews": 1,
                    "positive_count": 1,
                    "neutral_count": 1,
                    "negative_count": 1,
                    "avg_sentiment_score": 1,
                    "positive_percentage": {
                        "$multiply": [
                            {"$divide": ["$positive_count", "$total_reviews"]},
                            100
                        ]
                    },
                    "neutral_percentage": {
                        "$multiply": [
                            {"$divide": ["$neutral_count", "$total_reviews"]},
                            100
                        ]
                    },
                    "negative_percentage": {
                        "$multiply": [
                            {"$divide": ["$negative_count", "$total_reviews"]},
                            100
                        ]
                    }
                }
            },
            {"$sort": {"total_reviews": -1}},
            {"$limit": limit}
        ]
        
        results = list(collection.aggregate(pipeline))
        
        # Format response
        products = []
        for item in results:
            products.append({
                "product_id": item["product_id"],
                "product_name": item["product_name"] or "Unknown Product",
                "total_reviews": item["total_reviews"],
                "sentiment_distribution": {
                    "positive": item["positive_count"],
                    "neutral": item["neutral_count"],
                    "negative": item["negative_count"]
                },
                "sentiment_percentages": {
                    "positive": round(item["positive_percentage"], 2),
                    "neutral": round(item["neutral_percentage"], 2),
                    "negative": round(item["negative_percentage"], 2)
                },
                "avg_sentiment_score": round(item["avg_sentiment_score"] or 0.0, 3)
            })
        
        return {
            "time_window": time_window,
            "products": products
        }
    
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error fetching product sentiment distribution: {str(e)}")


