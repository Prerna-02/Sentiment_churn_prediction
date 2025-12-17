from pydantic import BaseModel, Field
from typing import Optional, Dict, Any
from datetime import datetime

class ReviewEvent(BaseModel):
    event_id: str
    customer_id: str
    text: str
    channel: str = Field(default="app")  # app/web/email/callcenter/social
    timestamp_utc: datetime
    metadata: Optional[Dict[str, Any]] = None

class EnrichedReviewEvent(ReviewEvent):
    sentiment_label: str               # negative/neutral/positive
    sentiment_score: float             # e.g., -1.0 to +1.0 (or probability)
    churn_risk_score: float            # 0.0 to 1.0
    churn_risk_label: str              # low/medium/high
    model_version: str                 # e.g., "sent-v1|churn-v1"
