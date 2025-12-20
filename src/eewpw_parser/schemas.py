# -*- coding: utf-8 -*-
"""
Frozen EEWPW schema shared by all parsers and consumers.
Backward compatible: legacy JSON/JSONL must parse without edits.
Algorithm-agnostic fields stay in DetectionCore; algorithm-specific data is namespaced.
Ground-motion data is station-centric under GMInfo.
Schema evolution is tracked only via meta.schema_version (default preserved on read).

All fields are strings unless otherwise noted, to preserve original formatting.

FinalDoc shape (JSON):
{
  "meta": {"algo": "...", "dialect": "...", "schema_version": "...", "extras": {}, "stats_total": {}},
  "annotations": {"<profile>": [Annotation, ...]},
  "detections": [
    {
      "timestamp": "...", "event_id": "...", "category": "...", "instance": "...", "orig_sys": "...", "version": "...",
      "core_info": {...},
      "fault_info": [FaultVertex, ...],
      "gm_info": {"pga_obs": [GMObs, ...], "pgv_obs": [...], "pgd_obs": [...], "gmcontour_pred": [...], "extra": {}},
      "finder_details": {...} | null,
      "vs_details": {...} | null,
      "extras": {}
    },
    ...
  ]
}
"""

from typing import List, Dict, Any, Optional
from pydantic import BaseModel, Field, validator

# Default schema version for back-compatibility. Needed for the 
# legacy files without schema_version. The version format is opaque.
# For now, the adapted logic is "year.increment" (e.g., "2025.0", "2025.1").
DEFAULT_SCHEMA_VERSION = "2025.0"

class Annotation(BaseModel):
    timestamp: str  # ISO-8601 Z
    pattern: str
    line: str  # Normally could be int, but everything else is str so we keep str for consistency.
    text: str
    pattern_id: Optional[str] = None

class GMObs(BaseModel):
    """
    Single-station ground-motion observation; one station + one measure. 
    Algorithm-specific station metadata belongs in extra under a namespaced 
    key (e.g. extra["vs"]).
    """
    orig_sys: Optional[str] = None
    SNCL: str
    value: str  # float
    lat: str  # float
    lon: str  # float
    time: str  # ISO-8601 Z
    extra: Dict[str, Any] = Field(default_factory=dict)

class DetectionCore(BaseModel):
    """Algorithm-agnostic event solution (location, magnitude, origin time)."""
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
    depth: Optional[str] = None


class MMIContour(BaseModel):
    """Predicted MMI contour; polygon stored as provided (typically [[lon, lat], ...]) 
    and not normalized."""
    MMI: str
    polygon: Any


class FinderDetails(BaseModel):
    """Finder-specific solution details (normalized get_* metrics, 
    solution fields, flags)."""
    solution_metrics: Dict[str, str] = Field(default_factory=dict)
    origin_time_epoch: Optional[str] = None
    solution: Dict[str, str] = Field(default_factory=dict)
    finder_flags: Optional[Dict[str, str]] = None
    extra: Dict[str, Any] = Field(default_factory=dict)


class VSDetails(BaseModel):
    """VS-specific detection details not tied to individual stations."""
    summary: Dict[str, str] = Field(default_factory=dict)
    stations_not_used: List[str] = Field(default_factory=list)
    extra: Dict[str, Any] = Field(default_factory=dict)


class GMInfo(BaseModel):
    """Ground-motion data grouped by measure plus optional predicted 
    products; extensions go to extra."""
    pga_obs: List[GMObs] = Field(default_factory=list)
    pgv_obs: List[GMObs] = Field(default_factory=list)
    pgd_obs: List[GMObs] = Field(default_factory=list)
    gmcontour_pred: List[MMIContour] = Field(default_factory=list)
    extra: Dict[str, Any] = Field(default_factory=dict)

class Detection(BaseModel):
    """Single detection combining core event info, ground motion, optional fault 
    geometry, and algorithm-specific detail blocks."""
    timestamp: str           # ISO-8601 Z (emission time for this detection)
    event_id: str            # IDs must be strings
    category: str
    instance: str
    orig_sys: str
    version: str
    core_info: DetectionCore
    fault_info: List[FaultVertex] = Field(default_factory=list)
    gm_info: GMInfo = Field(default_factory=GMInfo)
    finder_details: Optional[FinderDetails] = None
    vs_details: Optional[VSDetails] = None
    extras: Dict[str, Any] = Field(default_factory=dict)

    @validator("fault_info", pre=True)
    def _coerce_fault_info(cls, v):
        # Back-compat: fault_info {} -> [].
        # Is fault_info null or None (from legacy files)? Coerce to [].
        if v is None or v == []:
            return []
        # Is fault_info {} (from legacy files)? Coerce to [].
        if v == {}:
            return []
        # Otherwise, preserve as-is.
        return v

    @validator("gm_info", pre=True)
    def _coerce_gm_info(cls, v):
        # Back-compat: accept dict payloads and preserve unknown keys in extra.
        if v is None or v == {}:
            return GMInfo()
        if isinstance(v, dict):
            known_keys = {"pga_obs", "pgv_obs", "pgd_obs", "gmcontour_pred", "extra"}
            gm_info_data = {k: v[k] for k in known_keys if k in v}
            gm_info = GMInfo(**gm_info_data)
            extras_payload = {k: val for k, val in v.items() if k not in known_keys}
            if extras_payload:
                gm_info.extra.update(extras_payload)
            return gm_info
        return v

    @validator("gm_info")
    def _fill_obs_orig_sys(cls, v, values):
        # Consumer fallback: propagate detection orig_sys to missing GMObs.orig_sys.
        det_orig_sys = values.get("orig_sys")
        if det_orig_sys and isinstance(v, GMInfo):
            for obs_list in (v.pga_obs, v.pgv_obs, v.pgd_obs):
                for obs in obs_list:
                    if obs.orig_sys is None:
                        obs.orig_sys = det_orig_sys
        return v

class Meta(BaseModel):
    algo: str
    dialect: str
    schema_version: str = DEFAULT_SCHEMA_VERSION
    files: Optional[List[str]] = None
    started_at: Optional[str] = None
    finished_at: Optional[str] = None
    playback_time: Optional[str] = None
    extras: Dict[str, Any] = Field(default_factory=dict)
    stats_total: Dict[str, int] = Field(default_factory=dict)

    @validator("schema_version", pre=True, always=True)
    def _default_schema_version(cls, v):
        # Back-compat: fill missing schema_version on read.
        return v or DEFAULT_SCHEMA_VERSION

class FinalDoc(BaseModel):
    meta: Meta
    annotations: Dict[str, List[Annotation]]
    detections: List[Detection]
    
