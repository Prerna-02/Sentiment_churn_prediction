# services/dashboard_api/app/routers/churn_over_time.py

from fastapi import APIRouter, Query, HTTPException
from datetime import datetime
from typing import Optional

from app.utils.mongodb import mongo_conn
from app.utils.time_utils import get_time_filter, parse_time_window


router = APIRouter()


@router.get("/churn-over-time")
async def get_churn_over_time(
    time_window: str = Query("24h", regex="^(1h|24h|7d|30d|all)$"),
    product_id: Optional[str] = None,
    channel: Optional[str] = None,
):
    """
    Churn risk over time (proxy: negative % and review volume by time bucket).
    Same time bucketing as sentiment trend. churn_risk = negative_percentage / 100.
    """
    try:
        collection = mongo_conn.get_collection()
        start_time, granularity = parse_time_window(time_window)
        filter_query = get_time_filter(time_window, product_id, channel)

        if granularity == "5m":
            unit, bin_size = "minute", 5
        elif granularity == "1h":
            unit, bin_size = "hour", 1
        else:
            unit, bin_size = "day", 1

        pipeline = [
            {"$match": filter_query},
            {
                "$addFields": {
                    "timestamp_date": {
                        "$dateFromString": {
                            "dateString": "$timestamp_utc",
                            "onError": None,
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
                            "unit": unit,
                            "binSize": bin_size,
                        }
                    },
                    "review_count": {"$sum": 1},
                    "negative_count": {
                        "$sum": {"$cond": [{"$eq": ["$sentiment_label", "negative"]}, 1, 0]},
                    },
                }
            },
            {
                "$project": {
                    "timestamp": "$_id",
                    "review_count": 1,
                    "negative_percentage": {
                        "$multiply": [
                            {"$divide": ["$negative_count", "$review_count"]},
                            100,
                        ]
                    },
                }
            },
            {"$sort": {"timestamp": 1}},
        ]

        results = list(collection.aggregate(pipeline))
        data = []
        for item in results:
            ts = item["timestamp"]
            np = round(item["negative_percentage"], 2)
            data.append({
                "timestamp": ts.isoformat() if isinstance(ts, datetime) else ts,
                "negative_percentage": np,
                "churn_risk": round(np / 100.0, 4),
                "review_count": item["review_count"],
            })

        return {
            "time_window": time_window,
            "granularity": granularity,
            "data": data,
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error fetching churn over time: {str(e)}")
