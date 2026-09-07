from fastapi import FastAPI, HTTPException, Query
from typing import Optional
from .schemas import NetworkSummary, GridActivityResponse, HotspotResponse, AlertResponse, GridFeatures, PredictRiskRequest, PredictRiskResponse, PipelineStatus, GridLocation
from .service import get_network_summary, get_grid_activity, get_hotspots, get_alerts, get_grid_features, predict_risk, get_pipeline_status, get_grid_location

from fastapi.middleware.cors import CORSMiddleware
from .model_loader import load_model

app = FastAPI(title="Network Operations Predictive Intelligence API")


app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:3000"],  # React's default dev server
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.get("/network/summary", response_model=NetworkSummary)
def network_summary(as_of: Optional[str] = Query(None, description="Reporting timestamp, e.g. '2013-11-07 23:00:00'. Defaults to MAX(timestamp) in the analytics layer.")):
    try:
        result = get_network_summary(as_of)
        return NetworkSummary(**result)
    except FileNotFoundError as e:
        raise HTTPException(status_code=500, detail=str(e))
    except ValueError as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/network/hotspots", response_model=HotspotResponse)
def hotspots(
    as_of: Optional[str] = Query(None, description="Reporting timestamp. Defaults to MAX(timestamp)."),
    limit: int = Query(10, ge=1, le=100, description="Maximum number of hotspots to return."),
    severity: Optional[str] = Query(None, description="Filter by severity: 'high' or 'medium'."),
):
    try:
        result = get_hotspots(as_of, limit, severity)
        return HotspotResponse(**result)
    except FileNotFoundError as e:
        raise HTTPException(status_code=500, detail=str(e))
    except ValueError as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/network/alerts", response_model=AlertResponse)
def alerts(
    as_of: Optional[str] = Query(None, description="Reporting timestamp. Defaults to MAX(timestamp)."),
    limit: int = Query(10, ge=1, le=100, description="Maximum number of alerts to return."),
    severity: Optional[str] = Query(None, description="Filter by severity: 'high' or 'medium'."),
):
    try:
        result = get_alerts(as_of, limit, severity)
        return AlertResponse(**result)
    except FileNotFoundError as e:
        raise HTTPException(status_code=500, detail=str(e))
    except ValueError as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/network/grid/{grid_id}", response_model=GridActivityResponse)
def grid_activity(
    grid_id: int,
    as_of: Optional[str] = Query(None, description="Reporting timestamp. Defaults to MAX(timestamp) in the analytics layer."),
    date: Optional[str] = Query(None, description="Explicit date filter, e.g. '2013-11-05'. Overrides the trailing-24-hour default."),
    hour: Optional[int] = Query(None, description="Explicit hour filter (0-23), only used together with date."),
):
    try:
        result = get_grid_activity(grid_id, as_of, date, hour)
        if result is None:
            raise HTTPException(status_code=404, detail=f"Grid {grid_id} not found. Valid range is 1-10000.")
        return GridActivityResponse(**result)
    except FileNotFoundError as e:
        raise HTTPException(status_code=500, detail=str(e))
    except ValueError as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/network/grid/{grid_id}/features", response_model=GridFeatures)
def grid_features(grid_id: int):
    try:
        result = get_grid_features(grid_id)
        if result is None:
            raise HTTPException(
                status_code=404,
                detail=f"No stored features found for grid {grid_id}. "
                       f"Feature table is populated by ml/features.py (ML2), which has not yet been run for this grid."
            )
        return GridFeatures(**result)
    except FileNotFoundError as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/network/predict-risk", response_model=PredictRiskResponse)
def predict_risk_endpoint(request: PredictRiskRequest):
    try:
        result = predict_risk(request.grid_id, request.as_of)
        if result is None:
            raise HTTPException(status_code=404, detail=f"Grid {request.grid_id} not found. Valid range is 1-10000.")
        return PredictRiskResponse(**result)
    except FileNotFoundError as e:
        raise HTTPException(status_code=500, detail=str(e))
    except ValueError as e:
        raise HTTPException(status_code=500, detail=str(e))
    
@app.get("/pipeline/status", response_model=PipelineStatus)
def pipeline_status():
    try:
        result = get_pipeline_status()
        return PipelineStatus(**result)
    except FileNotFoundError as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/network/grid/{grid_id}/location", response_model=GridLocation)
def grid_location(grid_id: int):
    result = get_grid_location(grid_id)
    if result is None:
        raise HTTPException(status_code=404, detail=f"Grid {grid_id} not found. Valid range is 1-10000.")
    return GridLocation(**result)

@app.on_event("startup")
def _load_risk_model():
    load_model()
