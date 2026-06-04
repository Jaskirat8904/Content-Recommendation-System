from pydantic import BaseModel
from typing import Optional, List

class WatchEvent(BaseModel):
    user_id: int
    content_id: int
    watch_status: str
    watch_duration_mins: int
    total_duration_mins: int
    device: Optional[str] = "mobile"
    timestamp: str

class RatingEvent(BaseModel):
    user_id: int
    content_id: int
    rating: float
    review_text: Optional[str] = None
    timestamp: str

class RecommendationResponse(BaseModel):
    user_id: int
    recommendations: List[dict]
    source: str