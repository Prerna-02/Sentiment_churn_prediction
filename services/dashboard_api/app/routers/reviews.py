# services/dashboard_api/app/routers/reviews.py

from fastapi import APIRouter, HTTPException, Query
from bson import ObjectId
from bson.errors import InvalidId
from datetime import datetime, timezone
from typing import Optional, List
import uuid
import requests

from app.utils.mongodb import mongo_conn
from app.models.review import ReviewCreate, ReviewUpdate, ReviewResponse


router = APIRouter()


# Model service URL for sentiment prediction
MODEL_SERVICE_URL = "http://model_service:8000"


def get_sentiment_prediction(text: str, event_id: str, customer_id: str) -> dict:
    """
    Call model service to get sentiment prediction for review text.
    
    Returns:
        dict with sentiment_label, sentiment_score, confidence, model_version
    """
    try:
        response = requests.post(
            f"{MODEL_SERVICE_URL}/predict",
            json={
                "text": text,
                "event_id": event_id,
                "customer_id": customer_id
            },
            timeout=5
        )
        response.raise_for_status()
        return response.json()
    except Exception as e:
        print(f"⚠️ Sentiment prediction failed: {e}")
        # Return default values if model service is unavailable
        return {
            "sentiment_label": "unknown",
            "sentiment_score": 0,
            "confidence": 0.0,
            "model_version": "unavailable"
        }


# ==================== CREATE ====================

@router.post("/reviews", status_code=201)
async def create_review(review: ReviewCreate):
    """
    Create a new review and automatically predict sentiment.
    
    Returns:
        - success: bool
        - review_id: MongoDB ObjectId as string
        - message: str
    """
    try:
        collection = mongo_conn.get_collection()
        
        # Generate unique event_id
        event_id = str(uuid.uuid4())
        
        # Get sentiment prediction from model service
        sentiment = get_sentiment_prediction(
            text=review.text,
            event_id=event_id,
            customer_id=review.customer_id
        )
        
        # Create review document
        review_doc = {
            "event_id": event_id,
            "customer_id": review.customer_id,
            "product_id": review.product_id,
            "product_name": review.product_name,
            "text": review.text,
            "channel": review.channel,
            "timestamp_utc": datetime.now(timezone.utc).isoformat(),
            "metadata": review.metadata or {"source": "manual_api_entry"},
            # Sentiment fields from model service
            "sentiment_label": sentiment["sentiment_label"],
            "sentiment_score": sentiment["sentiment_score"],
            "confidence": sentiment["confidence"],
            "model_version": sentiment["model_version"]
        }
        
        # Insert into MongoDB
        result = collection.insert_one(review_doc)
        
        return {
            "success": True,
            "review_id": str(result.inserted_id),
            "event_id": event_id,
            "sentiment": sentiment,
            "message": "Review created successfully"
        }
    
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error creating review: {str(e)}")


# ==================== READ (Single) ====================

@router.get("/reviews/{review_id}")
async def get_review(review_id: str):
    """
    Get a single review by MongoDB ObjectId.
    
    Args:
        review_id: MongoDB ObjectId as string
    
    Returns:
        Review document
    """
    try:
        collection = mongo_conn.get_collection()
        
        # Convert string to ObjectId
        try:
            object_id = ObjectId(review_id)
        except InvalidId:
            raise HTTPException(status_code=400, detail="Invalid review ID format")
        
        # Find review
        review = collection.find_one({"_id": object_id})
        
        if not review:
            raise HTTPException(status_code=404, detail="Review not found")
        
        # Convert ObjectId to string for JSON serialization
        review["_id"] = str(review["_id"])
        
        return review
    
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error fetching review: {str(e)}")


# ==================== READ (List with Pagination) ====================

@router.get("/reviews")
async def list_reviews(
    page: int = Query(1, ge=1, description="Page number (1-indexed)"),
    limit: int = Query(20, ge=1, le=100, description="Items per page (max 100)"),
    sentiment: Optional[str] = Query(None, regex="^(positive|negative|neutral)$", description="Filter by sentiment"),
    channel: Optional[str] = Query(None, description="Filter by channel"),
    product_id: Optional[str] = Query(None, description="Filter by product ID"),
    customer_id: Optional[str] = Query(None, description="Filter by customer ID")
):
    """
    List reviews with pagination and optional filters.
    
    Returns:
        - total: Total number of reviews matching filters
        - page: Current page number
        - limit: Items per page
        - total_pages: Total number of pages
        - reviews: List of review documents
    """
    try:
        collection = mongo_conn.get_collection()
        
        # Build filter query
        filter_query = {}
        if sentiment:
            filter_query["sentiment_label"] = sentiment
        if channel:
            filter_query["channel"] = channel
        if product_id:
            filter_query["product_id"] = product_id
        if customer_id:
            filter_query["customer_id"] = customer_id
        
        # Get total count
        total = collection.count_documents(filter_query)
        
        # Calculate pagination
        skip = (page - 1) * limit
        total_pages = (total + limit - 1) // limit  # Ceiling division
        
        # Fetch reviews
        reviews = list(
            collection.find(filter_query)
            .sort("timestamp_utc", -1)  # Most recent first
            .skip(skip)
            .limit(limit)
        )
        
        # Convert ObjectId to string
        for review in reviews:
            review["_id"] = str(review["_id"])
        
        return {
            "total": total,
            "page": page,
            "limit": limit,
            "total_pages": total_pages,
            "reviews": reviews
        }
    
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error listing reviews: {str(e)}")


# ==================== UPDATE ====================

@router.put("/reviews/{review_id}")
async def update_review(review_id: str, review_update: ReviewUpdate):
    """
    Update an existing review.
    
    If text is updated, sentiment will be re-predicted automatically.
    
    Args:
        review_id: MongoDB ObjectId as string
        review_update: Fields to update
    
    Returns:
        - success: bool
        - modified_count: int
        - message: str
    """
    try:
        collection = mongo_conn.get_collection()
        
        # Convert string to ObjectId
        try:
            object_id = ObjectId(review_id)
        except InvalidId:
            raise HTTPException(status_code=400, detail="Invalid review ID format")
        
        # Check if review exists
        existing_review = collection.find_one({"_id": object_id})
        if not existing_review:
            raise HTTPException(status_code=404, detail="Review not found")
        
        # Build update document
        update_doc = {}
        
        # If text is updated, re-predict sentiment
        if review_update.text is not None:
            update_doc["text"] = review_update.text
            
            # Get new sentiment prediction
            sentiment = get_sentiment_prediction(
                text=review_update.text,
                event_id=existing_review.get("event_id", "unknown"),
                customer_id=existing_review.get("customer_id", "unknown")
            )
            
            update_doc["sentiment_label"] = sentiment["sentiment_label"]
            update_doc["sentiment_score"] = sentiment["sentiment_score"]
            update_doc["confidence"] = sentiment["confidence"]
            update_doc["model_version"] = sentiment["model_version"]
        
        # Update other fields
        if review_update.product_name is not None:
            update_doc["product_name"] = review_update.product_name
        if review_update.channel is not None:
            update_doc["channel"] = review_update.channel
        if review_update.metadata is not None:
            update_doc["metadata"] = review_update.metadata
        
        # Add updated timestamp
        update_doc["updated_at"] = datetime.now(timezone.utc).isoformat()
        
        # Perform update
        result = collection.update_one(
            {"_id": object_id},
            {"$set": update_doc}
        )
        
        return {
            "success": True,
            "modified_count": result.modified_count,
            "message": "Review updated successfully",
            "sentiment_updated": review_update.text is not None
        }
    
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error updating review: {str(e)}")


# ==================== DELETE ====================

@router.delete("/reviews/{review_id}")
async def delete_review(review_id: str):
    """
    Delete a review by MongoDB ObjectId.
    
    Args:
        review_id: MongoDB ObjectId as string
    
    Returns:
        - success: bool
        - deleted_count: int
        - message: str
    """
    try:
        collection = mongo_conn.get_collection()
        
        # Convert string to ObjectId
        try:
            object_id = ObjectId(review_id)
        except InvalidId:
            raise HTTPException(status_code=400, detail="Invalid review ID format")
        
        # Delete review
        result = collection.delete_one({"_id": object_id})
        
        if result.deleted_count == 0:
            raise HTTPException(status_code=404, detail="Review not found")
        
        return {
            "success": True,
            "deleted_count": result.deleted_count,
            "message": "Review deleted successfully"
        }
    
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error deleting review: {str(e)}")


# ==================== BULK DELETE (Bonus) ====================

@router.delete("/reviews")
async def bulk_delete_reviews(
    sentiment: Optional[str] = Query(None, regex="^(positive|negative|neutral)$"),
    channel: Optional[str] = Query(None),
    product_id: Optional[str] = Query(None),
    confirm: bool = Query(False, description="Must be true to confirm bulk delete")
):
    """
    Bulk delete reviews matching filters.
    
    CAUTION: This will delete multiple reviews at once!
    
    Args:
        sentiment: Filter by sentiment
        channel: Filter by channel
        product_id: Filter by product
        confirm: Must be true to proceed
    
    Returns:
        - success: bool
        - deleted_count: int
        - message: str
    """
    if not confirm:
        raise HTTPException(
            status_code=400,
            detail="Bulk delete requires confirm=true parameter"
        )
    
    try:
        collection = mongo_conn.get_collection()
        
        # Build filter query
        filter_query = {}
        if sentiment:
            filter_query["sentiment_label"] = sentiment
        if channel:
            filter_query["channel"] = channel
        if product_id:
            filter_query["product_id"] = product_id
        
        if not filter_query:
            raise HTTPException(
                status_code=400,
                detail="At least one filter (sentiment, channel, or product_id) is required for bulk delete"
            )
        
        # Delete matching reviews
        result = collection.delete_many(filter_query)
        
        return {
            "success": True,
            "deleted_count": result.deleted_count,
            "message": f"Deleted {result.deleted_count} reviews",
            "filters": filter_query
        }
    
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error in bulk delete: {str(e)}")
