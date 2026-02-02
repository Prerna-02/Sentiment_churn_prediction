# services/model_service/app/main.py

import os
from typing import Optional
from pathlib import Path

from fastapi import FastAPI, HTTPException
from pydantic import BaseModel

# Import the shared model wrappers
from shared.sentiment_model import SentimentModel3Class
from shared.tfidf_sentiment_model import TfidfSentimentModel


# -------------------------
# Environment config
# -------------------------
MODEL_PATH = os.getenv("MODEL_PATH", "/models/sentiment_model_tfidf_lr.pkl")
MODEL_VERSION_ENV = os.getenv("MODEL_VERSION", "amazon_tfidf_lr_v1")


# -------------------------
# FastAPI app + startup
# -------------------------
app = FastAPI(
    title="Sentiment Model Service",
    description="Real-time sentiment inference API for ITD pipeline (Phase 6+)",
    version="2.0.0"
)

# Global model instance (loaded once at startup)
model = None


@app.on_event("startup")
async def load_model():
    """Load the pickled model once when the service starts."""
    global model
    model_file = Path(MODEL_PATH)
    if not model_file.exists():
        raise FileNotFoundError(f"Model file not found: {MODEL_PATH}")
    
    # Try loading as TF-IDF model first (new format)
    try:
        model = TfidfSentimentModel.load(MODEL_PATH)
        print(f"✅ TF-IDF Model loaded from {MODEL_PATH}")
        print(f"   Model version: {model.model_version}")
    except (KeyError, AttributeError):
        # Fall back to old format (SentimentModel3Class)
        print(f"⚠️  New format failed, trying old format...")
        model = SentimentModel3Class.load(MODEL_PATH)
        print(f"✅ Legacy Model loaded from {MODEL_PATH}")
        print(f"   Model version: {model.model_version}")


# -------------------------
# Request/Response schemas
# -------------------------
class PredictRequest(BaseModel):
    text: str
    event_id: Optional[str] = None
    customer_id: Optional[str] = None


class PredictResponse(BaseModel):
    sentiment_label: str  # "positive" | "negative" | "neutral"
    sentiment_score: int  # 1 | -1 | 0
    confidence: float     # 0.0 - 1.0
    model_version: str


# -------------------------
# Endpoints
# -------------------------
@app.get("/health")
async def health():
    """Health check endpoint."""
    return {
        "status": "ok",
        "model_loaded": model is not None,
        "model_path": MODEL_PATH
    }


@app.post("/predict", response_model=PredictResponse)
async def predict(request: PredictRequest):
    """
    Predict sentiment for a given text.
    
    Returns:
      - sentiment_label: "positive" | "negative" | "neutral"
      - sentiment_score: 1 (positive), -1 (negative), 0 (neutral)
      - confidence: single float (0.0-1.0)
      - model_version: str
    """
    if model is None:
        raise HTTPException(status_code=503, detail="Model not loaded")
    
    text = request.text or ""
    
    try:
        # Call the model wrapper's predict_one method
        # Returns: (label: str, score: int, confidence: float, probs: dict)
        label, score, confidence, probs = model.predict_one(text)
        
        return PredictResponse(
            sentiment_label=label,
            sentiment_score=score,
            confidence=confidence,
            model_version=model.model_version
        )
    except Exception as e:
        # Log error and return clear HTTP 500
        print(f"❌ Prediction error: {type(e).__name__}: {e}")
        raise HTTPException(
            status_code=500, 
            detail=f"Model inference failed: {type(e).__name__}: {str(e)}"
        )

