# services/dashboard_api/app/routers/alerts.py

from fastapi import APIRouter, Query, HTTPException
from datetime import datetime, timezone, timedelta
from typing import Optional, List, Dict
import hashlib

from app.utils.mongodb import mongo_conn
from app.utils.time_utils import get_time_filter


router = APIRouter()


def detect_negative_spike(product_id: str, product_name: str, recent_negative: int, baseline_negative: float) -> Optional[Dict]:
    """Detect sudden spike in negative reviews."""
    if baseline_negative == 0:
        baseline_negative = 1  # Avoid division by zero
    
    spike_ratio = recent_negative / baseline_negative
    
    if spike_ratio >= 1.5 and recent_negative >= 3:  # 50% increase + minimum volume
        alert_id = hashlib.md5(f"negative_spike_{product_id}_{datetime.now().date()}".encode()).hexdigest()[:12]
        return {
            "alert_id": alert_id,
            "product_id": product_id,
            "product_name": product_name,
            "alert_type": "negative_spike",
            "severity": "high" if spike_ratio >= 2.0 else "medium",
            "reason": f"Negative reviews increased by {int((spike_ratio - 1) * 100)}% in last 1h",
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "metric_value": recent_negative,
            "threshold_value": baseline_negative
        }
    return None


def detect_volume_surge(product_id: str, product_name: str, recent_volume: int, baseline_volume: float) -> Optional[Dict]:
    """Detect unusual review volume surge."""
    if baseline_volume == 0:
        baseline_volume = 1
    
    surge_ratio = recent_volume / baseline_volume
    
    if surge_ratio >= 2.0 and recent_volume >= 10:  # 100% increase + minimum volume
        alert_id = hashlib.md5(f"volume_surge_{product_id}_{datetime.now().date()}".encode()).hexdigest()[:12]
        return {
            "alert_id": alert_id,
            "product_id": product_id,
            "product_name": product_name,
            "alert_type": "volume_surge",
            "severity": "medium" if surge_ratio < 3.0 else "high",
            "reason": f"Review volume increased by {int((surge_ratio - 1) * 100)}% in last 1h",
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "metric_value": recent_volume,
            "threshold_value": baseline_volume
        }
    return None


def detect_sentiment_drop(product_id: str, product_name: str, recent_sentiment: float, baseline_sentiment: float) -> Optional[Dict]:
    """Detect sharp drop in average sentiment score."""
    sentiment_drop = baseline_sentiment - recent_sentiment
    
    if sentiment_drop >= 0.3:  # Drop of 0.3 or more on [-1, 1] scale
        alert_id = hashlib.md5(f"sentiment_drop_{product_id}_{datetime.now().date()}".encode()).hexdigest()[:12]
        return {
            "alert_id": alert_id,
            "product_id": product_id,
            "product_name": product_name,
            "alert_type": "sentiment_drop",
            "severity": "high" if sentiment_drop >= 0.5 else "medium",
            "reason": f"Sentiment score dropped by {round(sentiment_drop, 2)} in last 1h",
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "metric_value": round(recent_sentiment, 3),
            "threshold_value": round(baseline_sentiment, 3)
        }
    return None


@router.get("/alerts")
async def get_alerts(
    time_window: str = Query("24h", regex="^(1h|24h|7d|30d)$"),
    severity: str = Query("all", regex="^(high|medium|low|all)$"),
    limit: int = Query(50, ge=1, le=200)
):
    """
    Detect and return real-time alerts for anomalies.
    
    Alert types:
    1. negative_spike: Sudden increase in negative reviews
    2. volume_surge: Unusual review volume spike
    3. sentiment_drop: Sharp drop in avg sentiment score
    
    Detection windows:
    - Recent: Last 1 hour
    - Baseline: Previous 24 hours (before recent window)
    """
    try:
        collection = mongo_conn.get_collection()
        now = datetime.now(timezone.utc)
        
        # Define time windows
        recent_start = now - timedelta(hours=1)
        baseline_end = recent_start
        baseline_start = baseline_end - timedelta(hours=24)
        
        # Only check products with product_id
        base_filter = {"product_id": {"$exists": True, "$ne": None}}
        
        # Aggregation for recent metrics (last 1h)
        recent_pipeline = [
            {
                "$match": {
                    **base_filter,
                    "timestamp_utc": {"$gte": recent_start.isoformat()}
                }
            },
            {
                "$group": {
                    "_id": {
                        "product_id": "$product_id",
                        "product_name": "$product_name"
                    },
                    "recent_volume": {"$sum": 1},
                    "recent_negative": {
                        "$sum": {"$cond": [{"$eq": ["$sentiment_label", "negative"]}, 1, 0]}
                    },
                    "recent_sentiment": {"$avg": "$sentiment_score"}
                }
            }
        ]
        
        # Aggregation for baseline metrics (previous 24h)
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
        
        recent_results = {
            item["_id"]["product_id"]: item
            for item in collection.aggregate(recent_pipeline)
        }
        
        baseline_results = {
            item["_id"]: item
            for item in collection.aggregate(baseline_pipeline)
        }
        
        # Detect alerts
        alerts = []
        
        for product_id, recent in recent_results.items():
            product_name = recent["_id"]["product_name"] or "Unknown Product"
            baseline = baseline_results.get(product_id, {
                "baseline_volume": 1,
                "baseline_negative": 1,
                "baseline_sentiment": 0.0
            })
            
            # Check for negative spike
            alert = detect_negative_spike(
                product_id, product_name,
                recent["recent_negative"],
                baseline["baseline_negative"]
            )
            if alert:
                alerts.append(alert)
            
            # Check for volume surge
            alert = detect_volume_surge(
                product_id, product_name,
                recent["recent_volume"],
                baseline["baseline_volume"]
            )
            if alert:
                alerts.append(alert)
            
            # Check for sentiment drop
            if recent["recent_sentiment"] is not None and baseline["baseline_sentiment"] is not None:
                alert = detect_sentiment_drop(
                    product_id, product_name,
                    recent["recent_sentiment"],
                    baseline["baseline_sentiment"]
                )
                if alert:
                    alerts.append(alert)
        
        # Filter by severity
        if severity != "all":
            alerts = [a for a in alerts if a["severity"] == severity]
        
        # Sort by severity (high first) and timestamp
        severity_order = {"high": 3, "medium": 2, "low": 1}
        alerts.sort(
            key=lambda x: (severity_order.get(x["severity"], 0), x["timestamp"]),
            reverse=True
        )
        
        return {
            "alerts": alerts[:limit],
            "total_alerts": len(alerts),
            "time_window": time_window
        }
    
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error fetching alerts: {str(e)}")


