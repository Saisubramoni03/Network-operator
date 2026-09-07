from pydantic import BaseModel
from typing import List
from typing import Optional



class NetworkSummary(BaseModel):
    total_activity: float
    active_grids: int
    peak_hour: int
    top_grid: int
    as_of: str


class GridActivityPoint(BaseModel):
    timestamp: str
    sms_in: float
    sms_out: float
    call_in: float
    call_out: float
    internet_activity: float
    total_activity: float


class GridActivityResponse(BaseModel):
    grid_id: int
    as_of: str
    points: List[GridActivityPoint]


class HotspotItem(BaseModel):
    grid_id: int
    timestamp: str
    total_activity: float
    severity: str                          # "high" or "medium"
    reason: str
    risk_score: Optional[float] = None     # reserved for future ML — additive-safe
    risk_level: Optional[str] = None       # reserved for future ML
    model_version: Optional[str] = None    # reserved for future ML


class HotspotResponse(BaseModel):
    as_of: str
    count: int
    items: List[HotspotItem]


class AlertItem(BaseModel):
    grid_id: int
    timestamp: str
    alert_type: str
    current_activity: float
    baseline_activity: float
    reason: str
    risk_score: Optional[float] = None
    risk_level: Optional[str] = None
    model_version: Optional[str] = None


class AlertResponse(BaseModel):
    as_of: str
    count: int
    items: List[AlertItem]


class GridFeatureItem(BaseModel):
    feature_timestamp: str
    avg_activity: float
    activity_growth: float
    active_hours: int
    peak_ratio: float
    variability: float
    internet_share: float


class FeatureDataQuality(BaseModel):
    status: str
    feature_count: int


class FeatureFreshness(BaseModel):
    latest_feature_timestamp: str
    requested_as_of: str
    is_fresh: bool


class GridFeatures(BaseModel):
    grid_id: int
    feature_timestamp: str
    avg_activity: float
    activity_growth: float
    active_hours: int
    peak_ratio: float
    variability: float
    internet_share: float
    data_quality: str       # "OK" for now — reserved for future null/partial-feature flags
    freshness: str          # human-readable, e.g. "as of feature_timestamp"

class PredictRiskRequest(BaseModel):
    grid_id: int
    as_of: Optional[str] = None


class PredictRiskResponse(BaseModel):
    grid_id: int
    as_of: str
    risk_score: float
    risk_level: str
    model_version: str
    explanation_note: str


class PipelineStatus(BaseModel):
    run_id: str
    run_timestamp: str
    task_status: dict
    rows_in: Optional[int] = None
    rows_rejected: Optional[int] = None
    nulls_handled: Optional[int] = None
    rows_published: Optional[int] = None
    as_of: Optional[str] = None
    freshness: str
    healthy: bool
    reasons: List[str]


class GridLocation(BaseModel):
    grid_id: int
    centroid_lat: float
    centroid_lon: float
    polygon_ref: str

class PredictRiskResponse(BaseModel):
    grid_id: int
    as_of: str
    feature_timestamp: str          # ← only this line is new
    risk_score: float
    risk_level: str
    model_version: str
    top_features: Optional[List[str]] = None   # ← and this line
    explanation_note: str