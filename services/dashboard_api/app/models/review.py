# services/dashboard_api/app/models/review.py

from pydantic import BaseModel, Field
from typing import Optional, Dict
from datetime import datetime


class ReviewCreate(BaseModel):
    """Schema for creating a new review."""
    customer_id: str = Field(..., description="Customer identifier")
    product_id: str = Field(..., description="Product identifier")
    product_name: str = Field(..., description="Product name")
    text: str = Field(..., min_length=1, description="Review text content")
    channel: str = Field("web", description="Channel: app, web, email, callcenter, social")
    metadata: Optional[Dict] = Field(default_factory=dict, description="Additional metadata")
    
    class Config:
        schema_extra = {
            "example": {
                "customer_id": "C123",
                "product_id": "AMZN-ABC123",
                "product_name": "Amazing Product",
                "text": "This product is great!",
                "channel": "web",
                "metadata": {"source": "manual_entry"}
            }
        }


class ReviewUpdate(BaseModel):
    """Schema for updating an existing review."""
    text: Optional[str] = Field(None, min_length=1, description="Updated review text")
    product_name: Optional[str] = Field(None, description="Updated product name")
    channel: Optional[str] = Field(None, description="Updated channel")
    metadata: Optional[Dict] = Field(None, description="Updated metadata")
    
    class Config:
        schema_extra = {
            "example": {
                "text": "Updated review text",
                "product_name": "Updated Product Name"
            }
        }


class ReviewResponse(BaseModel):
    """Schema for review response."""
    id: str = Field(..., alias="_id", description="MongoDB ObjectId as string")
    event_id: str
    customer_id: str
    product_id: str
    product_name: str
    text: str
    channel: str
    timestamp_utc: str
    sentiment_label: Optional[str] = None
    sentiment_score: Optional[int] = None
    confidence: Optional[float] = None
    model_version: Optional[str] = None
    metadata: Optional[Dict] = None
    
    class Config:
        allow_population_by_field_name = True
