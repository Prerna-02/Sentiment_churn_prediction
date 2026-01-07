# services/model_service/app/main.py

import os
from typing import Optional
from pathlib import Path

from fastapi import FastAPI, HTTPException
from pydantic import BaseModel

# Import the shared model wrapper
from shared.sentiment_model import SentimentModel3Class


# -------------------------
# Environment config
# -------------------------
MODEL_PATH = os.getenv("MODEL_PATH", "/models/sentiment_model_3class.pkl")
MODEL_VERSION_ENV = os.getenv("MODEL_VERSION", "hashing_sgd_3class_v1")


# -------------------------
# FastAPI app + startup
# -------------------------
app = FastAPI(
    title="Sentiment Model Service",
    description="Real-time sentiment inference API for ITD pipeline (Phase 6)",
    version="1.0.0"
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
    
    model = SentimentModel3Class.load(MODEL_PATH)
    print(f"✅ Model loaded from {MODEL_PATH}")
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
    
    # Call the model wrapper's predict_one method
    # Returns: (label: str, score: int, confidence: float, probs: dict)
    label, score, confidence, probs = model.predict_one(text)
    
    return PredictResponse(
        sentiment_label=label,
        sentiment_score=score,
        confidence=confidence,
        model_version=model.model_version
    )

