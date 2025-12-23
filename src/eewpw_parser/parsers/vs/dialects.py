# -*- coding: utf-8 -*-
import re
from dataclasses import dataclass, field
from typing import List, Tuple, Dict, Any, Optional

from eewpw_parser.schemas import (
    Annotation,
    Detection,
    DetectionCore,
    FaultVertex,
    GMObs,
    VSDetails,
)
from eewpw_parser.utils import to_iso_utc_z
from eewpw_parser.config import load_profile
from collections import deque

VS_RECENT_LINES_MAX = 2000


def _safe_float(val: str) -> Optional[float]:
    val = val.strip()
    if not val or val.lower() == "nan":
        return None
    try:
        f = float(val)
    except ValueError:
        return None
    if f == -1.0:
        return None
    return f


def parse_optional_float(value: Optional[str]) -> Optional[float]:
    """
    Parse optional float with nan tolerated as None.
    """
    if value is None:
        return None
    raw = value.strip()
    if not raw:
        return None
    if raw.lower() == "nan":
        return None
    return float(raw)


def _parse_gm_field(raw: str):
    txt = raw.strip()
    if txt.lower() == "nan":
        return None, None
    if txt == "-1.00e+00":
        return None, "-1.00e+00"
    return float(txt), None


@dataclass
class VSEventState:
    event_id: Optional[str] = None
    update_number: Optional[int] = None
    vs_mag: Optional[float] = None
    median_mag: Optional[float] = None
    lat: Optional[float] = None
    lon: Optional[float] = None
    depth: Optional[float] = None
    creation_time: Optional[str] = None
    origin_time: Optional[str] = None
    likelihood: Optional[float] = None
    last_ts_iso: Optional[str] = None
    stations: List[Dict[str, Any]] = field(default_factory=list)
    current_station: Optional[Dict[str, Any]] = None
    stations_not_used: List[str] = field(default_factory=list)
    summary_fields: Dict[str, str] = field(default_factory=dict)

    def start_station(self, sncl: str, wavetype: str, soil_class: str, magnitude: Optional[float]):
        if self.current_station:
            self.flush_station()
        self.current_station = {
            "sncl": sncl.strip(),
            "component": None,
            "wavetype": wavetype.strip(),
            "soil": soil_class.strip(),
            "magnitude": magnitude,
            "lat": None,
            "lon": None,
            "pga_h": None,
            "pgv_h": None,
            "pgd_h": None,
            "pga_z": None,
            "pgv_z": None,
            "pgd_z": None,
            "pga_h_val": None,
            "pgv_h_val": None,
            "pgd_h_val": None,
            "pga_z_val": None,
            "pgv_z_val": None,
            "pgd_z_val": None,
            "pga_h_sentinel_raw": None,
            "pgv_h_sentinel_raw": None,
            "pgd_h_sentinel_raw": None,
            "pga_z_sentinel_raw": None,
            "pgv_z_sentinel_raw": None,
            "pgd_z_sentinel_raw": None,
            "epi_dist_km": None,
        }
        parts = sncl.split(".")
        if len(parts) >= 3 and parts[2]:
            self.current_station["component"] = parts[2]

    def flush_station(self):
        if not self.current_station:
            return
        self.stations.append(self.current_station)
        self.current_station = None

    def to_detection(
        self,
        version_by_event: Dict[str, int],
    ) -> Detection:
        # Ensure any open station is recorded
        self.flush_station()

        version = (
            int(self.update_number)
            if self.update_number is not None
            else version_by_event.get(self.event_id or "", 0)
        )
        if self.event_id:
            version_by_event[self.event_id] = version

        orig_time = (
            self.origin_time
            or self.creation_time
            or self.last_ts_iso
            or to_iso_utc_z("1970-01-01T00:00:00Z")
        )
        timestamp = self.creation_time or self.last_ts_iso or orig_time

        core = DetectionCore(
            id=str(self.event_id or "0"),
            mag=str(self.vs_mag) if self.vs_mag is not None else "0.0",
            lat=str(self.lat) if self.lat is not None else "0.0",
            lon=str(self.lon) if self.lon is not None else "0.0",
            depth=str(self.depth) if self.depth is not None else "0.0",
            orig_time=orig_time,
            likelihood=str(self.likelihood) if self.likelihood is not None else None,
            vs_median_single_station_mag=str(self.median_mag) if self.median_mag is not None else None,
        )

        pga_list: List[GMObs] = []
        pgv_list: List[GMObs] = []
        pgd_list: List[GMObs] = []
        for st in self.stations:
            lat = st.get("lat")
            lon = st.get("lon")
            if lat is None or lon is None:
                continue
            vs_meta_base = {
                "component": None,
                "station_magnitude": str(st.get("magnitude")) if st.get("magnitude") is not None else None,
                "wavetype": st.get("wavetype"),
                "soil_class": st.get("soil"),
                "epi_dist_km": str(st.get("epi_dist_km")) if st.get("epi_dist_km") is not None else None,
            }

            def add_obs(val, sentinel_raw, lst: List[GMObs], component: str):
                meta = dict(vs_meta_base)
                meta["component"] = component
                if sentinel_raw is not None:
                    meta["is_sentinel"] = True
                    meta["raw_value"] = sentinel_raw
                    lst.append(
                        GMObs(
                            orig_sys="vs",
                            SNCL=st.get("sncl", ""),
                            value=sentinel_raw,
                            lat=str(lat),
                            lon=str(lon),
                            time=timestamp,
                            extra={"vs": meta},
                        )
                    )
                    return
                if val is None:
                    return
                lst.append(
                    GMObs(
                        orig_sys="vs",
                        SNCL=st.get("sncl", ""),
                        value=str(val),
                        lat=str(lat),
                        lon=str(lon),
                        time=timestamp,
                        extra={"vs": meta},
                    )
                )

            def val_pair(name: str):
                val = st.get(f"{name}_val")
                sentinel_raw = st.get(f"{name}_sentinel_raw")
                if val is None and sentinel_raw is None:
                    val = st.get(name)
                return val, sentinel_raw

            add_obs(*val_pair("pga_z"), pga_list, component="Z")
            add_obs(*val_pair("pga_h"), pga_list, component="H")
            add_obs(*val_pair("pgv_z"), pgv_list, component="Z")
            add_obs(*val_pair("pgv_h"), pgv_list, component="H")
            add_obs(*val_pair("pgd_z"), pgd_list, component="Z")
            add_obs(*val_pair("pgd_h"), pgd_list, component="H")

        summary = dict(self.summary_fields)
        if self.update_number is not None:
            summary.setdefault("update_number", str(self.update_number))
        if self.likelihood is not None:
            summary.setdefault("likelihood", str(self.likelihood))
        if self.vs_mag is not None:
            summary.setdefault("vs_mag", str(self.vs_mag))
        if self.median_mag is not None:
            summary.setdefault("median_single_station_mag", str(self.median_mag))

        vs_details = VSDetails(summary=summary, stations_not_used=list(self.stations_not_used))

        return Detection(
            timestamp=timestamp,
            event_id=str(self.event_id or "0"),
            category="live",
            instance="vs@unknown",
            orig_sys="vs",
            version=str(version),
            core_info=core,
            fault_info=[],
            gm_info={"pgv_obs": pgv_list, "pga_obs": pga_list, "pgd_obs": pgd_list},
            vs_details=vs_details,
        )


@dataclass
class VSStreamState:
    file_start_ts_iso: Optional[str] = None
    file_end_ts_iso: Optional[str] = None
    line_offset: int = 0
    current_event: Optional[VSEventState] = None
    version_by_event: Dict[str, int] = field(default_factory=dict)
    recent_lines: deque = field(default_factory=lambda: deque(maxlen=VS_RECENT_LINES_MAX))
    absolute_line_counter: int = 0


class VSDialect:
    """
    Parser for scvsmag processing/info logs (VS magnitude).
    Designed to work incrementally for live tailing.
    """

    PROFILE_NAME: str = "profiles/vs_time_vs_mag.json"

    P_PREFIX_TS = re.compile(
        r"^(\d{4}/\d{2}/\d{2} \d{2}:\d{2}:\d{2})\s+\[processing/info/VsMagnitude\]\s*(.*)$"
    )
    P_START = re.compile(r"Start logging for event:\s*(\S+)")
    P_END = re.compile(r"End logging for event:\s*(\S+)")
    P_UPDATE = re.compile(r"update number:\s*([-\d]+)")
    P_SENSOR = re.compile(
        r"Sensor:\s*([A-Za-z0-9.]+);\s*Wavetype:\s*([^;]+);\s*Soil class:\s*([^;]+);\s*Magnitude:\s*([^\s;]+)"
    )
    P_STATION_LOC = re.compile(
        r"station lat:\s*([-\d.]+);\s*station lon:\s*([-\d.]+);\s*epicentral distance:\s*([-\d.]+);"
    )
    P_PGA_Z = re.compile(
        r"PGA\(Z\):\s*([-\d.eE+]+);\s*PGV\(Z\):\s*([-\d.eE+]+);\s*PGD\(Z\):\s*([-\d.eE+]+)"
    )
    P_PGA_H = re.compile(
        r"PGA\(H\):\s*([-\d.eE+]+);\s*PGV\(H\):\s*([-\d.eE+]+);\s*PGD\(H\):\s*([-\d.eE+]+)"
    )
    P_VS_MAG = re.compile(
        r"VS-mag:\s*(?P<vs_mag>[-\d.eE+]+|nan)\s*;"
        r"\s*median single-station-mag:\s*(?P<median_mag>[-\d.eE+]+|nan)\s*;"
        r"\s*lat:\s*(?P<lat>[-\d.eE+]+)\s*;"
        r"\s*lon:\s*(?P<lon>[-\d.eE+]+)\s*;"
        r"\s*depth\s*:\s*(?P<depth>[-\d.eE+]+)"
        , re.IGNORECASE
    )
    P_TIMES = re.compile(
        r"creation time:\s*([^;]+);\s*origin time:\s*([^;]+);"
    )
    P_LIK = re.compile(r"likelihood:\s*([-\d.eE+]+)")
    P_UNUSED = re.compile(r"Stations not used for VS-mag:\s*(?P<list>.+)$")

    def __init__(self):
        self.verbose = False

    @property
    def profile(self) -> dict:
        if not hasattr(self, "_profile_cache"):
            self._profile_cache = load_profile(self.PROFILE_NAME)
        return self._profile_cache

    def parse_file(self, path: str) -> Tuple[List[Detection], List[Annotation], Dict[str, Any]]:
        dets: List[Detection] = []
        ann: List[Annotation] = []
        state = VSStreamState()

        with open(path, "r", encoding="utf-8", errors="ignore") as f:
            for line in f:
                d_chunk, a_chunk = self.feed_line(line, state)
                dets.extend(d_chunk)
                ann.extend(a_chunk)

        d_flush, a_flush = self.flush(state)
        dets.extend(d_flush)
        ann.extend(a_flush)

        extras: Dict[str, Any] = {
            "file": str(path),
            "playback_time": None,
            "started_at": state.file_start_ts_iso,
            "finished_at": state.file_end_ts_iso,
            "stats": {
                "detections": len(dets),
                "annotations": len(ann),
            },
        }
        return dets, ann, extras

    # --- streaming helpers ---

    def feed_line(
        self,
        line: str,
        state: VSStreamState,
    ) -> Tuple[List[Detection], List[Annotation]]:
        state.absolute_line_counter += 1
        state.recent_lines.append((state.absolute_line_counter, line))
        dets: List[Detection] = []
        ann: List[Annotation] = []
        state.line_offset += 1

        m_prefix = self.P_PREFIX_TS.match(line)
        if not m_prefix:
            return dets, ann

        ts_iso = to_iso_utc_z(m_prefix.group(1))
        message = m_prefix.group(2)

        if state.file_start_ts_iso is None:
            state.file_start_ts_iso = ts_iso
        state.file_end_ts_iso = ts_iso

        patterns_cfg = self.profile.get("patterns", {})
        for pid, pat in patterns_cfg.items():
            if re.search(pat, line):
                ann.append(
                    Annotation(
                        timestamp=ts_iso,
                        pattern=pat,
                        line=str(state.line_offset),
                        text=line.rstrip("\n"),
                        pattern_id=pid,
                    )
                )

        # Event boundaries
        m_start = self.P_START.search(message)
        if m_start:
            # Flush current event if any
            if state.current_event and state.current_event.event_id:
                dets.append(state.current_event.to_detection(state.version_by_event))
            state.current_event = VSEventState(event_id=m_start.group(1), last_ts_iso=ts_iso)
            if self.verbose:
                print(f"VS event start: event_id={state.current_event.event_id} ts={ts_iso}")
            return dets, ann

        m_end = self.P_END.search(message)
        if m_end and state.current_event:
            state.current_event.last_ts_iso = ts_iso
            state.current_event.flush_station()
            dets.append(state.current_event.to_detection(state.version_by_event))
            if self.verbose:
                ev = state.current_event
                print(
                    "VS event end: event_id={eid} updates={upd} mag={mag} stations={stations} ts={ts}".format(
                        eid=ev.event_id,
                        upd=ev.update_number if ev.update_number is not None else "-",
                        mag=ev.vs_mag if ev.vs_mag is not None else "-",
                        stations=len(ev.stations),
                        ts=ts_iso,
                    )
                )
            state.current_event = None
            return dets, ann

        ev = state.current_event
        if not ev:
            return dets, ann

        ev.last_ts_iso = ts_iso

        if (m := self.P_UPDATE.search(message)):
            try:
                ev.update_number = int(m.group(1))
            except ValueError:
                ev.update_number = None

        if (m := self.P_SENSOR.search(message)):
            sncl = m.group(1)
            wavetype = m.group(2)
            soil = m.group(3)
            mag = _safe_float(m.group(4))
            ev.start_station(sncl, wavetype, soil, mag)

        if (m := self.P_STATION_LOC.search(message)) and ev.current_station:
            ev.current_station["lat"] = _safe_float(m.group(1))
            ev.current_station["lon"] = _safe_float(m.group(2))
            ev.current_station["epi_dist_km"] = _safe_float(m.group(3))

        if (m := self.P_PGA_Z.search(message)) and ev.current_station:
            ev.current_station["pga_z_val"], ev.current_station["pga_z_sentinel_raw"] = _parse_gm_field(m.group(1))
            ev.current_station["pgv_z_val"], ev.current_station["pgv_z_sentinel_raw"] = _parse_gm_field(m.group(2))
            ev.current_station["pgd_z_val"], ev.current_station["pgd_z_sentinel_raw"] = _parse_gm_field(m.group(3))

        if (m := self.P_PGA_H.search(message)) and ev.current_station:
            ev.current_station["pga_h_val"], ev.current_station["pga_h_sentinel_raw"] = _parse_gm_field(m.group(1))
            ev.current_station["pgv_h_val"], ev.current_station["pgv_h_sentinel_raw"] = _parse_gm_field(m.group(2))
            ev.current_station["pgd_h_val"], ev.current_station["pgd_h_sentinel_raw"] = _parse_gm_field(m.group(3))

        if (m := self.P_VS_MAG.search(message)):
            ev.vs_mag = _safe_float(m.group("vs_mag"))
            ev.median_mag = parse_optional_float(m.group("median_mag"))
            ev.lat = _safe_float(m.group("lat"))
            ev.lon = _safe_float(m.group("lon"))
            ev.depth = _safe_float(m.group("depth"))

        if (m := self.P_TIMES.search(message)):
            try:
                ev.creation_time = to_iso_utc_z(m.group(1).replace(" ", "T"))
            except Exception:
                ev.creation_time = None
            try:
                ev.origin_time = to_iso_utc_z(m.group(2).replace(" ", "T"))
            except Exception:
                ev.origin_time = ev.creation_time

        if (m := self.P_LIK.search(message)):
            ev.likelihood = _safe_float(m.group(1))

        if (m := self.P_UNUSED.search(message)) and ev:
            raw = m.group("list")
            items = [tok for tok in raw.strip().split() if tok]
            ev.stations_not_used.extend(items)

        return dets, ann

    def flush(self, state: VSStreamState) -> Tuple[List[Detection], List[Annotation]]:
        dets: List[Detection] = []
        ann: List[Annotation] = []
        if state.current_event and state.current_event.event_id:
            dets.append(state.current_event.to_detection(state.version_by_event))
            state.current_event = None
        return dets, ann
