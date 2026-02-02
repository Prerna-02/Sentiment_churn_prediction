# services/dashboard_api/app/routers/kpis.py

from fastapi import APIRouter, Query, HTTPException
from datetime import datetime, timezone
from typing import Optional

from app.utils.mongodb import mongo_conn
from app.utils.time_utils import get_time_filter


router = APIRouter()


@router.get("/kpis")
async def get_kpis(
    time_window: str = Query("24h", regex="^(1h|24h|7d|30d|all)$"),
    product_id: Optional[str] = None,
    channel: Optional[str] = None
):
    """
    Get key performance indicators (KPIs) for the dashboard.
    
    Returns:
        - total_reviews: Total number of reviews in time window
        - negative_percentage: Percentage of negative reviews
        - avg_sentiment_score: Average sentiment score (-1 to 1)
        - avg_confidence: Average model confidence (0 to 1)
        - high_risk_products: Count of products with priority score > 0.7
        - alerts_triggered: Count of recent alerts (placeholder for now)
    """
    try:
        collection = mongo_conn.get_collection()
        filter_query = get_time_filter(time_window, product_id, channel)
        
        # MongoDB aggregation pipeline
        pipeline = [
            {"$match": filter_query},
            {
                "$group": {
                    "_id": None,
                    "total_reviews": {"$sum": 1},
                    "negative_count": {
                        "$sum": {"$cond": [{"$eq": ["$sentiment_label", "negative"]}, 1, 0]}
                    },
                    "avg_sentiment_score": {"$avg": "$sentiment_score"},
                    "avg_confidence": {"$avg": "$confidence"}
                }
            }
        ]
        
        result = list(collection.aggregate(pipeline))
        
        if not result:
            # No data in time window
            return {
                "total_reviews": 0,
                "negative_percentage": 0.0,
                "avg_sentiment_score": 0.0,
                "avg_confidence": 0.0,
                "high_risk_products": 0,
                "alerts_triggered": 0,
                "time_window": time_window,
                "timestamp": datetime.now(timezone.utc).isoformat()
            }
        
        data = result[0]
        total = data["total_reviews"]
        negative_count = data["negative_count"]
        
        # Calculate negative percentage
        negative_percentage = (negative_count / total * 100) if total > 0 else 0.0
        
        # Calculate high-risk products (products with high negative ratio)
        # For now, we'll count products with >50% negative reviews
        high_risk_pipeline = [
            {"$match": filter_query},
            {"$match": {"product_id": {"$exists": True, "$ne": None}}},
            {
                "$group": {
                    "_id": "$product_id",
                    "total": {"$sum": 1},
                    "negative": {
                        "$sum": {"$cond": [{"$eq": ["$sentiment_label", "negative"]}, 1, 0]}
                    }
                }
            },
            {
                "$project": {
                    "negative_ratio": {"$divide": ["$negative", "$total"]}
                }
            },
            {"$match": {"negative_ratio": {"$gt": 0.5}}},
            {"$count": "high_risk_count"}
        ]
        
        high_risk_result = list(collection.aggregate(high_risk_pipeline))
        high_risk_products = high_risk_result[0]["high_risk_count"] if high_risk_result else 0
        
        # Count alerts triggered in last 24 hours
        # Use same logic as alerts endpoint: compare last 1h vs previous 24h
        now = datetime.now(timezone.utc)
        from datetime import timedelta
        
        recent_start = now - timedelta(hours=1)
        baseline_end = recent_start
        baseline_start = baseline_end - timedelta(hours=24)
        
        base_filter = {"product_id": {"$exists": True, "$ne": None}}
        
        # Get recent metrics (last 1h)
        recent_pipeline = [
            {
                "$match": {
                    **base_filter,
                    "timestamp_utc": {"$gte": recent_start.isoformat()}
                }
            },
            {
                "$group": {
                    "_id": "$product_id",
                    "recent_volume": {"$sum": 1},
                    "recent_negative": {
                        "$sum": {"$cond": [{"$eq": ["$sentiment_label", "negative"]}, 1, 0]}
                    },
                    "recent_sentiment": {"$avg": "$sentiment_score"}
                }
            }
        ]
        
        # Get baseline metrics (previous 24h)
        baseline_pipeline = [
            {
                "$match": {
                    **base_filter,
                    "timestamp_utc": {
                        "$gte": baseline_start.isoformat(),
                        "$lt": baseline_end.isoformat()
                    }
                }
            },
            {
                "$group": {
                    "_id": "$product_id",
                    "baseline_volume": {"$sum": 1},
                    "baseline_negative": {
                        "$sum": {"$cond": [{"$eq": ["$sentiment_label", "negative"]}, 1, 0]}
                    },
                    "baseline_sentiment": {"$avg": "$sentiment_score"}
                }
            }
        ]
        
        recent_results = {item["_id"]: item for item in collection.aggregate(recent_pipeline)}
        baseline_results = {item["_id"]: item for item in collection.aggregate(baseline_pipeline)}
        
        # Count alerts using same detection logic as alerts endpoint
        alerts_count = 0
        for product_id, recent in recent_results.items():
            baseline = baseline_results.get(product_id, {
                "baseline_volume": 1,
                "baseline_negative": 1,
                "baseline_sentiment": 0.0
            })
            
            # Check negative spike
            if baseline["baseline_negative"] > 0:
                spike_ratio = recent["recent_negative"] / baseline["baseline_negative"]
                if spike_ratio >= 1.5 and recent["recent_negative"] >= 3:
                    alerts_count += 1
            
            # Check volume surge
            if baseline["baseline_volume"] > 0:
                surge_ratio = recent["recent_volume"] / baseline["baseline_volume"]
                if surge_ratio >= 2.0 and recent["recent_volume"] >= 10:
                    alerts_count += 1
            
            # Check sentiment drop
            if recent["recent_sentiment"] is not None and baseline["baseline_sentiment"] is not None:
                sentiment_drop = baseline["baseline_sentiment"] - recent["recent_sentiment"]
                if sentiment_drop >= 0.3:
                    alerts_count += 1
        
        return {
            "total_reviews": total,
            "negative_percentage": round(negative_percentage, 2),
            "avg_sentiment_score": round(data["avg_sentiment_score"] or 0.0, 3),
            "avg_confidence": round(data["avg_confidence"] or 0.0, 3),
            "high_risk_products": high_risk_products,
            "alerts_triggered": alerts_count,
            "time_window": time_window,
            "timestamp": datetime.now(timezone.utc).isoformat()
        }
    
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error fetching KPIs: {str(e)}")


