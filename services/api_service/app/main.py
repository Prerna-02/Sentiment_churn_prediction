# services/api_service/app/main.py
# Phase 7A: Dashboard Backend API

import os
from datetime import datetime, timedelta, timezone
from typing import Optional, List, Dict, Any
from enum import Enum

from fastapi import FastAPI, Query, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from pymongo import MongoClient
from dateutil import parser as date_parser


# -------------------------
# Environment Config
# -------------------------
MONGO_URI = os.getenv("MONGO_URI", "mongodb://mongo:27017")
MONGO_DB = os.getenv("MONGO_DB", "itd")
MONGO_COLLECTION = os.getenv("MONGO_COLLECTION", "reviews_enriched")


# -------------------------
# FastAPI App
# -------------------------
app = FastAPI(
    title="Sentiment Dashboard API",
    description="Backend API for Executive Dashboard (Phase 7A)",
    version="1.0.0"
)

# Enable CORS for frontend
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # In production, specify exact origins
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


# -------------------------
# Database Connection
# -------------------------
mongo_client = None
db = None
collection = None


@app.on_event("startup")
async def startup_db():
    """Connect to MongoDB on startup."""
    global mongo_client, db, collection
    mongo_client = MongoClient(MONGO_URI)
    db = mongo_client[MONGO_DB]
    collection = db[MONGO_COLLECTION]
    print(f"✅ Connected to MongoDB: {MONGO_URI}/{MONGO_DB}/{MONGO_COLLECTION}")


@app.on_event("shutdown")
async def shutdown_db():
    """Close MongoDB connection on shutdown."""
    global mongo_client
    if mongo_client:
        mongo_client.close()
        print("✅ MongoDB connection closed")


# -------------------------
# Enums and Models
# -------------------------
class TimeWindow(str, Enum):
    ONE_HOUR = "1h"
    TWENTY_FOUR_HOURS = "24h"
    SEVEN_DAYS = "7d"
    THIRTY_DAYS = "30d"


class SentimentLabel(str, Enum):
    POSITIVE = "positive"
    NEUTRAL = "neutral"
    NEGATIVE = "negative"


# Response Models
class KPIResponse(BaseModel):
    total_reviews: int
    negative_percentage: float
    avg_sentiment_score: float
    avg_confidence: float
    high_risk_products: int
    alerts_triggered: int
    time_window: str


class SentimentTrendPoint(BaseModel):
    timestamp: str
    avg_sentiment_score: float
    negative_percentage: float
    review_count: int


class SentimentTrendResponse(BaseModel):
    data_points: List[SentimentTrendPoint]
    time_window: str


class FixFirstProduct(BaseModel):
    product_id: str
    product_name: str
    priority_rank: int
    priority_score: float
    negative_percentage: float
    review_count: int
    avg_confidence: float
    sentiment_velocity: float  # Recent sentiment change
    trend: str  # "↑" or "↓" or "→"


class FixFirstResponse(BaseModel):
    products: List[FixFirstProduct]
    total_products: int


class Alert(BaseModel):
    alert_id: str
    product_id: str
    product_name: str
    reason: str
    severity: str  # "high" | "medium" | "low"
    timestamp: str
    details: Dict[str, Any]


class AlertFeedResponse(BaseModel):
    alerts: List[Alert]
    total_alerts: int


class ChannelBreakdown(BaseModel):
    channel: str
    review_count: int
    positive_count: int
    neutral_count: int
    negative_count: int
    avg_sentiment_score: float


class ChannelBreakdownResponse(BaseModel):
    channels: List[ChannelBreakdown]


class ProductSentiment(BaseModel):
    product_id: str
    product_name: str
    positive_count: int
    neutral_count: int
    negative_count: int
    total_reviews: int


class ProductSentimentDistributionResponse(BaseModel):
    products: List[ProductSentiment]
    total_products: int


# -------------------------
# Utility Functions
# -------------------------
def get_time_filter(time_window: TimeWindow) -> datetime:
    """Convert time window enum to datetime filter."""
    now = datetime.now(timezone.utc)
    
    if time_window == TimeWindow.ONE_HOUR:
        return now - timedelta(hours=1)
    elif time_window == TimeWindow.TWENTY_FOUR_HOURS:
        return now - timedelta(days=1)
    elif time_window == TimeWindow.SEVEN_DAYS:
        return now - timedelta(days=7)
    elif time_window == TimeWindow.THIRTY_DAYS:
        return now - timedelta(days=30)
    else:
        return now - timedelta(days=1)  # Default to 24h


def parse_timestamp(ts_str: str) -> datetime:
    """Parse ISO timestamp string to datetime object."""
    try:
        dt = date_parser.parse(ts_str)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt
    except Exception:
        return datetime.now(timezone.utc)


# -------------------------
# Health Check
# -------------------------
@app.get("/health")
async def health():
    """Health check endpoint."""
    try:
        # Test MongoDB connection
        count = collection.count_documents({})
        return {
            "status": "ok",
            "mongo_connected": True,
            "total_documents": count
        }
    except Exception as e:
        return {
            "status": "error",
            "mongo_connected": False,
            "error": str(e)
        }


# -------------------------
# API Endpoints
# -------------------------

@app.get("/api/kpis", response_model=KPIResponse)
async def get_kpis(
    time_window: TimeWindow = Query(TimeWindow.TWENTY_FOUR_HOURS, description="Time window for KPIs"),
    product_id: Optional[str] = Query(None, description="Filter by product_id"),
    channel: Optional[str] = Query(None, description="Filter by channel")
):
    """
    Get key performance indicators (KPIs) for the dashboard.
    
    Returns:
    - total_reviews: Total number of reviews in time window
    - negative_percentage: Percentage of negative reviews
    - avg_sentiment_score: Average sentiment score (1, 0, -1)
    - avg_confidence: Average model confidence
    - high_risk_products: Number of products with >30% negative reviews
    - alerts_triggered: Number of alerts in last 24h
    """
    try:
        # Build time filter
        time_threshold = get_time_filter(time_window)
        
        # Build query filter
        query_filter = {}
        
        # Add product filter if specified
        if product_id:
            query_filter["product_id"] = product_id
        
        # Add channel filter if specified
        if channel:
            query_filter["channel"] = channel
        
        # Get all reviews in time window
        reviews = list(collection.find(query_filter))
        
        # Filter by timestamp (since timestamp_utc is stored as string)
        filtered_reviews = []
        for review in reviews:
            ts_str = review.get("timestamp_utc", "")
            if ts_str:
                review_time = parse_timestamp(ts_str)
                if review_time >= time_threshold:
                    filtered_reviews.append(review)
        
        total_reviews = len(filtered_reviews)
        
        if total_reviews == 0:
            return KPIResponse(
                total_reviews=0,
                negative_percentage=0.0,
                avg_sentiment_score=0.0,
                avg_confidence=0.0,
                high_risk_products=0,
                alerts_triggered=0,
                time_window=time_window.value
            )
        
        # Calculate metrics
        negative_count = sum(1 for r in filtered_reviews if r.get("sentiment_label") == "negative")
        negative_percentage = (negative_count / total_reviews) * 100
        
        # Calculate average sentiment score
        sentiment_scores = [r.get("sentiment_score", 0) for r in filtered_reviews if r.get("sentiment_score") is not None]
        avg_sentiment_score = sum(sentiment_scores) / len(sentiment_scores) if sentiment_scores else 0.0
        
        # Calculate average confidence
        confidences = [r.get("confidence", 0) for r in filtered_reviews if r.get("confidence") is not None]
        avg_confidence = sum(confidences) / len(confidences) if confidences else 0.0
        
        # Calculate high-risk products (products with >30% negative reviews and at least 5 reviews)
        product_stats = {}
        for review in filtered_reviews:
            pid = review.get("product_id", "unknown")
            if pid not in product_stats:
                product_stats[pid] = {"total": 0, "negative": 0}
            product_stats[pid]["total"] += 1
            if review.get("sentiment_label") == "negative":
                product_stats[pid]["negative"] += 1
        
        high_risk_products = 0
        for pid, stats in product_stats.items():
            if stats["total"] >= 5:  # Minimum 5 reviews
                neg_pct = (stats["negative"] / stats["total"]) * 100
                if neg_pct > 30:
                    high_risk_products += 1
        
        # Calculate alerts (products with >50% negative in last 24h with at least 3 reviews)
        # Or sudden spike in negatives
        alerts_triggered = 0
        last_24h = datetime.now(timezone.utc) - timedelta(days=1)
        recent_reviews = [r for r in reviews if parse_timestamp(r.get("timestamp_utc", "")) >= last_24h]
        
        recent_product_stats = {}
        for review in recent_reviews:
            pid = review.get("product_id", "unknown")
            if pid not in recent_product_stats:
                recent_product_stats[pid] = {"total": 0, "negative": 0}
            recent_product_stats[pid]["total"] += 1
            if review.get("sentiment_label") == "negative":
                recent_product_stats[pid]["negative"] += 1
        
        for pid, stats in recent_product_stats.items():
            if stats["total"] >= 3:
                neg_pct = (stats["negative"] / stats["total"]) * 100
                if neg_pct > 50:
                    alerts_triggered += 1
        
        return KPIResponse(
            total_reviews=total_reviews,
            negative_percentage=round(negative_percentage, 2),
            avg_sentiment_score=round(avg_sentiment_score, 3),
            avg_confidence=round(avg_confidence, 3),
            high_risk_products=high_risk_products,
            alerts_triggered=alerts_triggered,
            time_window=time_window.value
        )
    
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to calculate KPIs: {str(e)}")

