# services/dashboard_api/app/main.py

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from datetime import datetime, timezone

from app.utils.mongodb import mongo_conn
from app.routers import kpis, sentiment_trend, fix_first, alerts, channels, products, churn_over_time, reviews, mongo_query


# -------------------------
# FastAPI App Initialization
# -------------------------
app = FastAPI(
    title="Dashboard API",
    description="Real-time sentiment & churn prediction dashboard API for Phase 7",
    version="1.0.0",
    docs_url="/docs",
    redoc_url="/redoc"
)


# -------------------------
# CORS Configuration
# -------------------------
# Allow all origins for development (restrict in production)
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # TODO: Restrict to specific origins in production
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


# -------------------------
# Startup/Shutdown Events
# -------------------------
@app.on_event("startup")
async def startup_event():
    """Initialize MongoDB connection and indexes on startup."""
    print("🚀 Starting Dashboard API...")
    if mongo_conn.connect():
        mongo_conn.ensure_indexes()
        print("✅ Dashboard API ready")
    else:
        print("⚠️ Dashboard API started but MongoDB connection failed")


@app.on_event("shutdown")
async def shutdown_event():
    """Close MongoDB connection on shutdown."""
    mongo_conn.close()
    print("👋 Dashboard API shutdown complete")


# -------------------------
# Health Check Endpoint
# -------------------------
@app.get("/api/health")
async def health_check():
    """Health check endpoint."""
    try:
        collection = mongo_conn.get_collection()
        total_reviews = collection.count_documents({})
        mongodb_connected = True
    except Exception as e:
        total_reviews = 0
        mongodb_connected = False
    
    return {
        "status": "ok" if mongodb_connected else "degraded",
        "service": "dashboard_api",
        "mongodb_connected": mongodb_connected,
        "total_reviews": total_reviews,
        "timestamp": datetime.now(timezone.utc).isoformat()
    }


# -------------------------
# Include Routers
# -------------------------
app.include_router(kpis.router, prefix="/api", tags=["KPIs"])
app.include_router(sentiment_trend.router, prefix="/api", tags=["Trends"])
app.include_router(fix_first.router, prefix="/api", tags=["Fix-First"])
app.include_router(alerts.router, prefix="/api", tags=["Alerts"])
app.include_router(channels.router, prefix="/api", tags=["Channels"])
app.include_router(products.router, prefix="/api", tags=["Products"])
app.include_router(churn_over_time.router, prefix="/api", tags=["Churn"])
app.include_router(reviews.router, prefix="/api", tags=["CRUD Operations"])
app.include_router(mongo_query.router, prefix="/api", tags=["MongoDB Query Executor"])


# -------------------------
# Root Endpoint
# -------------------------
@app.get("/")
async def root():
    """Root endpoint with API information."""
    return {
        "service": "Dashboard API",
        "version": "1.0.0",
        "docs": "/docs",
        "health": "/api/health"
    }


