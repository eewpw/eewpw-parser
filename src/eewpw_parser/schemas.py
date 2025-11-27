from typing import List, Dict, Any, Optional
from pydantic import BaseModel, Field

class Annotation(BaseModel):
    timestamp: str  # ISO-8601 Z
    pattern: str
    line: str  # int
    text: str
    pattern_id: Optional[str] = None

class GMObs(BaseModel):
    orig_sys: str
    SNCL: str
    value: str  # float
    lat: str  # float
    lon: str  # float
    time: str  # ISO-8601 Z

class DetectionCore(BaseModel):
    id: str
    mag: str # float
    lat: str # float
    lon: str # float
    depth: str # float
    orig_time: str  # ISO-8601 Z
    likelihood: Optional[str] = None
    vs_median_single_station_mag: Optional[str] = None

class FaultVertex(BaseModel):
    # float values, but we keep as str to preserve formatting
    lat: str 
    lon: str
    depth: str

class Detection(BaseModel):
    timestamp: str           # ISO-8601 Z (emission time for this detection)
    event_id: str            # IDs must be strings
    category: str
    instance: str
    orig_sys: str
    version: str
    core_info: DetectionCore
    fault_info: List[FaultVertex] = Field(default_factory=list)
    gm_info: Dict[str, List[GMObs]] = Field(default_factory=lambda: {"pgv_obs": [], "pga_obs": []})

class Meta(BaseModel):
    algo: str
    dialect: str
    files: Optional[List[str]] = None
    started_at: Optional[str] = None
    finished_at: Optional[str] = None
    playback_time: Optional[str] = None
    extras: Dict[str, Any] = Field(default_factory=dict)
    stats_total: Dict[str, int] = Field(default_factory=dict)

class FinalDoc(BaseModel):
    meta: Meta
    annotations: Dict[str, List[Annotation]]
    detections: List[Detection]
    
