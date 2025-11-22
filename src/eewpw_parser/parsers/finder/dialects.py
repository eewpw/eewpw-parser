# -*- coding: utf-8 -*-
import re
import json
import xml.etree.ElementTree as ET
from pathlib import Path
from typing import List, Tuple, Dict, Any, Optional
from eewpw_parser.schemas import Detection, DetectionCore, FaultVertex, GMObs, Annotation
from eewpw_parser.utils import to_iso_utc_z, epoch_to_iso_z, trim
from eewpw_parser.config import load_profile


class FinderBaseDialect:
    """
    Parser for SeisComP scfinder runtime logs (Finder engine).
    Designed for complete files (offline). Multiple files can be merged by the orchestrator.
    """

    # Core patterns
    P_EVENT_ID = re.compile(r"\bevent_id\s*=\s*(\d+)")
    P_GET_MAG = re.compile(r"->\s*get_mag\s*=\s*([0-9.]+)")
    P_GET_LAT = re.compile(r"->\s*get_epicenter_lat\s*=\s*(-?[0-9.]+)")
    P_GET_LON = re.compile(r"->\s*get_epicenter_lon\s*=\s*(-?[0-9.]+)")
    P_GET_DEP = re.compile(r"->\s*get_depth\s*=\s*([0-9.]+)")
    P_GET_LIK = re.compile(r"->\s*get_likelihood\s*=\s*([0-9.]+)")
    P_GET_OTM = re.compile(r"->\s*get_origin_time\s*=\s*([0-9\.e\+\-]+)")
    P_GET_AZM = re.compile(r"->\s*get_azimuth\s*=\s*([0-9.]+)")
    P_GET_RUP = re.compile(r"get_rupture_list\s*=\s*(.*)")
    P_RUP_PT  = re.compile(r"([-\d.]+)\/([-\d.]+)\/([-\d.]+)")
    # Continuation lines that contain only lat/lon/depth triplets
    P_RUP_LINE = re.compile(r"^\s*([\-\\d.]+)\/([\-\\d.]+)\/([\-\\d.]+)\s*$")

    # Backward lookups
    P_SOL_TPL = re.compile(r"SOLUTION TEMPLATE:\s*Template file name\s*=\s*(\S+)")    
        
    # Stations header and row (include=1 only)
    P_STATION_HEADER = re.compile(r"The stations that exceeded the minimum threshold|Stations with PGA above the min threshold")
    P_STATION_ROW = re.compile(r"\s*([^\s,]+)\s*([-\d.]+)\/([-\d.]+)\s*--\s*([-\d.eE+]+)\s*(\d+\.\d*)\s*include\s*=\s*(\d)")

    # Log line indicating playback start (first timestamp)
    START_PLAYBACK_RE = re.compile(r"^(\d{4}[/\-]\d{2}[/\-]\d{2}\s+\d{2}:\d{2}:\d{2})\s+\[notice/Application\]\s+Starting scfinder")

    # Hooks to be overridden if needed in subclasses
    # Matches both old style:
    #   SOLUTION RUPTURE:  Version 0 Time since ... Thresh = 4.6, ...
    # and new style with an extra inner timestamp:
    #   2020-10-25 19:35:49:411 SOLUTION RUPTURE:  Version 0 ... Thresh = 4.6, ...
    P_SOL_RUP = re.compile(
        r"(?:\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}:\d{3}\s+)?"
        r"SOLUTION RUPTURE:.*Thresh\s*=\s*([0-9.]+)"
    )
    
    # Log prefix (for annotations + fallback ts)
    # Supports both:
    #   2020/10/25 19:35:49 [notice/Application] ...
    #   2025-10-21 05:21:58:419| INFO | ...
    # and similar variants with optional milliseconds and either / or - as date separators.
    P_PREFIX_TS = re.compile(
        r"^("                                  # 1: timestamp
        r"\d{4}[/-]\d{2}[/-]\d{2}"             #   date: YYYY-MM-DD or YYYY/MM/DD
        r"[ T]\d{2}:\d{2}:\d{2}"               #   time: HH:MM:SS
        r"(?:[:.]\d{1,6})?"                    #   optional ms: :419 or .003000
        r")\s*"
        r"(?:\[[^\]]*\]\s*|\|\s*[^|]*\|\s*)?"  # optional [notice/Application] or '| INFO |' block
        r"(.*)$"                               # 2: rest of line (message)
    )

    # The dialect parser profile JSON
    PROFILE_NAME: str = "profiles/finder_time_vs_mag.json"


    def parse_file(self, path: str, algo: str = "finder", dialect: str = "scfinder") -> Tuple[List[Detection], List[Annotation], Dict[str, Any]]:
        """
        Parse a single scfinder log file and return:
        - detections (list of Detection)
        - annotations (list of Annotation)
        - extras (per-file metadata dict)

        The extras block is strictly per-file and will later be placed under:
            meta.extras["files"] = [extras_per_file, ...]
        by the orchestrator.

        Shape of `extras`:
        {
            "file": "<path>",
            "playback_time": "<ISO8601 Z or None>",
            "started_at": "<first log timestamp in file or None>",
            "finished_at": "<last log timestamp in file or None>",
            "stats": {
                "detections": <int>,
                "annotations": <int>
            }
        }
        """
        dets: List[Detection] = []
        ann: List[Annotation] = []

        with open(path, "r", encoding="utf-8", errors="ignore") as f:
            lines = f.readlines()

        # First pass: annotations and file-level timestamps
        ann, file_start_ts_iso, file_end_ts_iso, playback_time_iso = self._parse_annotations(lines)

        # Second pass: detections
        dets = self._parse_detections(lines)

        # Build per-file extras block
        extras: Dict[str, Any] = {
            "file": str(path),
            "playback_time": playback_time_iso,
            "started_at": file_start_ts_iso,
            "finished_at": file_end_ts_iso,
            "stats": {
                "detections": len(dets),
                "annotations": len(ann),
            },
        }

        return dets, ann, extras
    

    def _parse_annotations(self, lines: List[str]) -> Tuple[List[Annotation], Optional[str], Optional[str], Optional[str]]:
        """
        First pass over the log lines to extract annotations and
        derive file-level timestamps (start, end, playback).
        """
        ann: List[Annotation] = []

        file_start_ts_iso: Optional[str] = None
        file_end_ts_iso: Optional[str] = None
        playback_time_iso: Optional[str] = None

        for i, line in enumerate(lines):
            m = self.P_PREFIX_TS.search(line)
            if not m:
                continue
            ts_raw, msg = m.group(1), m.group(2)

            ts_iso = to_iso_utc_z(ts_raw)
            if file_start_ts_iso is None:
                file_start_ts_iso = ts_iso
            file_end_ts_iso = ts_iso

            # Detect playback start line ("Starting scfinder")
            if playback_time_iso is None and self.START_PLAYBACK_RE and self.START_PLAYBACK_RE.search(line):
                playback_time_iso = ts_iso

            # Annotation rules are driven by the profile JSON.
            profile = load_profile(self.PROFILE_NAME)
            patterns_cfg = profile.get("patterns", {})

            for pid, pat in patterns_cfg.items():
                if pid == "timestamp_regex":
                    continue
                if re.search(pat, line):
                    ann.append(Annotation(
                        timestamp=ts_iso,
                        pattern=pat,
                        line=i + 1,
                        text=line.rstrip("\n"),
                        pattern_id=pid
                    ))

        return ann, file_start_ts_iso, file_end_ts_iso, playback_time_iso

    def _pick_detection_timestamp(
        self,
        block_lines: List[str],
        emission_ts_iso: Optional[str],
        core_orig_time: str,
    ) -> str:
        """
        Hook to decide which timestamp to use for a Detection.

        Default behaviour:
        - Prefer the wall-clock emission_ts_iso (taken from P_PREFIX_TS).
        - Fall back to the core_orig_time if no emission timestamp is available.

        Subclasses (e.g. legacy/native dialects) can override this to use
        epoch-style timestamps or other log-specific markers.
        """
        return emission_ts_iso or core_orig_time

    def _parse_detections(self, lines: List[str]) -> List[Detection]:
        """
        Second pass over the log lines to extract detections.
        """
        dets: List[Detection] = []
        i = 0
        version_by_event: Dict[str, int] = {}
        pending_station_list: Optional[List[tuple]] = None

        while i < len(lines):
            line = lines[i]

            # Station block capture
            if self.P_STATION_HEADER.search(line):
                # consume rows while they match
                j = i + 1
                stations = []
                while j < len(lines):
                    mrow = self.P_STATION_ROW.findall(lines[j])
                    if not mrow:
                        break
                    for match in mrow:
                        sta, lat, lon, pga, t_epoch, include = (
                            match[0],
                            match[1],
                            match[2],
                            match[3],
                            match[4],
                            match[5],
                        )
                        if int(include) == 1:
                            stations.append(
                                (
                                    float(lat),
                                    float(lon),
                                    trim(sta),
                                    float(t_epoch),  # epoch seconds
                                    float(pga),      # PGA amplitude
                                )
                            )
                    j += 1
                pending_station_list = stations
                i = j
                continue

            m_eid = self.P_EVENT_ID.search(line)
            if not m_eid:
                i += 1
                continue

            event_id = m_eid.group(1)

            # Look-ahead small window to gather fields
            get_mag = get_lat = get_lon = get_dep = get_lik = get_otm = None
            rupture_list: List[FaultVertex] = []
            emission_ts_iso: Optional[str] = None

            j = i + 1
            # Scan a reasonable window; we’ll stop once we hit a timestamped line
            while j < len(lines) and j < i + 200:
                s = lines[j]

                if not emission_ts_iso:
                    mp = self.P_PREFIX_TS.search(s)
                    if mp:
                        emission_ts_iso = to_iso_utc_z(mp.group(1))

                if (m := self.P_GET_MAG.search(s)):
                    get_mag = float(m.group(1))
                if (m := self.P_GET_LAT.search(s)):
                    get_lat = float(m.group(1))
                if (m := self.P_GET_LON.search(s)):
                    get_lon = float(m.group(1))
                if (m := self.P_GET_DEP.search(s)):
                    get_dep = float(m.group(1))
                if (m := self.P_GET_LIK.search(s)):
                    get_lik = float(m.group(1))
                if (m := self.P_GET_OTM.search(s)):
                    get_otm = m.group(1)
                # azimuth captured but not yet used
                # if (m := self.P_GET_AZM.search(s)): get_azm = float(m.group(1))

                if (m := self.P_GET_RUP.search(s)):
                    coords = self.P_RUP_PT.findall(m.group(1))
                    if coords:
                        rupture_list.extend(
                            [
                                FaultVertex(
                                    lat=float(a),
                                    lon=float(b),
                                    depth=float(c),
                                )
                                for a, b, c in coords
                            ]
                        )
                        # Continuations on subsequent lines: accept only pure coord lines
                        k = j + 1
                        while k < len(lines):
                            mc = self.P_RUP_LINE.match(lines[k])
                            if not mc:
                                break
                            a, b, c = mc.groups()
                            rupture_list.append(
                                FaultVertex(
                                    lat=float(a),
                                    lon=float(b),
                                    depth=float(c),
                                )
                            )
                            k += 1
                j += 1

            # Build DetectionCore (we always emit, even if some fields are missing)
            if get_otm is None:
                core = DetectionCore(
                    id=str(event_id),
                    mag=float(get_mag) if get_mag is not None else 0.0,
                    lat=float(get_lat) if get_lat is not None else 0.0,
                    lon=float(get_lon) if get_lon is not None else 0.0,
                    depth=float(get_dep) if get_dep is not None else 0.0,
                    orig_time=emission_ts_iso
                    or to_iso_utc_z("1970-01-01T00:00:00Z"),
                    likelihood=float(get_lik) if get_lik is not None else None,
                )
            else:
                core = DetectionCore(
                    id=str(event_id),
                    mag=float(get_mag) if get_mag is not None else 0.0,
                    lat=float(get_lat) if get_lat is not None else 0.0,
                    lon=float(get_lon) if get_lon is not None else 0.0,
                    depth=float(get_dep) if get_dep is not None else 0.0,
                    orig_time=epoch_to_iso_z(get_otm),
                    likelihood=float(get_lik) if get_lik is not None else None,
                )

            # Decide which timestamp to emit for this detection
            timestamp_iso = self._pick_detection_timestamp(
                block_lines=lines[i:j],
                emission_ts_iso=emission_ts_iso,
                core_orig_time=core.orig_time,
            )

            # Versioning per event_id
            v = version_by_event.get(event_id, 0) + 1
            version_by_event[event_id] = v

            # gm_info from captured station list
            pga_list: List[GMObs] = []
            if pending_station_list:
                for lat, lon, sncl, t_epoch, pga in pending_station_list:
                    pga_list.append(
                        GMObs(
                            orig_sys="finder",
                            SNCL=str(sncl),
                            value=float(pga),
                            lat=float(lat),
                            lon=float(lon),
                            time=epoch_to_iso_z(str(t_epoch)),
                        )
                    )
                # Station list is consumed once per detection set
                pending_station_list = None

            dets.append(
                Detection(
                    timestamp=timestamp_iso,
                    event_id=str(event_id),
                    category="live",
                    instance="finder@unknown",  # can be overridden later
                    orig_sys="finder",
                    version=int(v),
                    core_info=core,
                    fault_info=rupture_list,
                    gm_info={"pgv_obs": [], "pga_obs": pga_list},
                )
            )

            i = j + 1

        return dets
    

# === Specific Dialect Implementations === #

class SCFinderDialect(FinderBaseDialect):
    """
    SeisComP scfinder dialect parser. Inherits from FinderBaseDialect with no changes.
    FinderBaseDialect can effectively handle scfinder logs as is.
    """
    P_PREFIX_TS = re.compile(
        r"^("  # timestamp
        r"\d{4}[/-]\d{2}[/-]\d{2}"
        r"[ T]\d{2}:\d{2}:\d{2}"
        r"(?:[:.]\d{1,6})?"
        r")\s*"
        r"(?:\[[^\]]*\]\s*|\|\s*[^|]*\|\s*)?"
        r"(.*)$"
    )
    START_PLAYBACK_RE = re.compile(
        r"^(\d{4}[/\-]\d{2}[/\-]\d{2}\s+\d{2}:\d{2}:\d{2})\s+\[notice/Application\]\s+Starting scfinder"
    )
    PROFILE_NAME: str = "profiles/finder_time_vs_mag.json"


class ShakeAlertFinderDialect(FinderBaseDialect):
    """
    ShakeAlert dialect parser. 
    """
    P_PREFIX_TS = re.compile(
        r"^("  # timestamp
        r"\d{4}[/-]\d{2}[/-]\d{2}"
        r"[ T]\d{2}:\d{2}:\d{2}"
        r"(?:[:.]\d{1,6})?"
        r")\s*"
        r"(?:\[[^\]]*\]\s*|\|\s*[^|]*\|\s*)?"
        r"(.*)$"
    )
    START_PLAYBACK_RE = re.compile(
        r"^(\d{4}[/\-]\d{2}[/\-]\d{2}\s+\d{2}:\d{2}:\d{2})\s+\[notice/Application\]\s+Starting scfinder"
    )
    PROFILE_NAME: str = "profiles/finder_time_vs_mag.json"

    # Inline absolute timestamps such as:
    #   2025-11-07 00:00:00:081
    #   2025-11-07 00:00:01.076
    #   2025/11/07,00:00:00.0795
    P_INLINE_ABS_TS = re.compile(
        r"(\d{4}[/-]\d{2}[/-]\d{2}[\s,]\d{2}:\d{2}:\d{2}(?:[:.]\d{1,6})?)"
    )

    def _parse_annotations(self, lines: List[str]) -> Tuple[List[Annotation], Optional[str], Optional[str], Optional[str]]:
        ann: List[Annotation] = []
        file_start_ts_iso = None
        file_end_ts_iso = None
        playback_time_iso = None

        for i, line in enumerate(lines):
            # Look for embedded absolute timestamps, ignore the 00:00:00:xxx prefix
            m_inline = self.P_INLINE_ABS_TS.search(line)
            if not m_inline:
                continue

            ts_raw = m_inline.group(1)
            ts_raw_norm = ts_raw.replace(",", " ")
            ts_iso = to_iso_utc_z(ts_raw_norm)

            if file_start_ts_iso is None:
                file_start_ts_iso = ts_iso
            file_end_ts_iso = ts_iso

            profile = load_profile(self.PROFILE_NAME)
            patterns_cfg = profile.get("patterns", {})

            for pid, pat in patterns_cfg.items():
                if pid == "timestamp_regex":
                    continue
                if re.search(pat, line):
                    ann.append(
                        Annotation(
                            timestamp=ts_iso,
                            pattern=pat,
                            line=i + 1,
                            text=line.rstrip("\n"),
                            pattern_id=pid,
                        )
                    )

        return ann, file_start_ts_iso, file_end_ts_iso, playback_time_iso

    def _parse_detections(self, lines: List[str]) -> List[Detection]:
        """
        For ShakeAlert logs, detections are emitted as embedded XML <event_message>
        blocks. We ignore the elapsed-time log prefix and parse the XML payload
        directly into Detection objects.
        """
        dets: List[Detection] = []
        i = 0

        while i < len(lines):
            line = lines[i]
            # Strip the "00:01:48:137|DEBUG | " style prefix if present
            payload = line.split("|", 2)[-1].lstrip() if "|" in line else line

            if "<event_message" not in payload:
                i += 1
                continue

            # Buffer the XML for this event_message
            xml_lines: List[str] = [payload]
            j = i + 1
            while j < len(lines):
                line_j = lines[j]
                payload_j = line_j.split("|", 2)[-1].lstrip() if "|" in line_j else line_j
                xml_lines.append(payload_j)
                if "</event_message>" in payload_j:
                    break
                j += 1

            xml_str = "".join(l.rstrip("\n") for l in xml_lines)

            try:
                root = ET.fromstring(xml_str)
            except Exception:
                # Malformed XML; skip this block and continue
                i = j + 1
                continue

            # event_message attributes
            timestamp_attr = root.attrib.get("timestamp")
            category = root.attrib.get("category") or "live"
            instance = root.attrib.get("instance") or "finder@unknown"
            orig_sys = root.attrib.get("orig_sys") or "finder"
            version_attr = root.attrib.get("version") or "0"

            # core_info block
            core_el = root.find("core_info")
            event_id = core_el.attrib.get("id") if core_el is not None else "0"

            def _get_text_float(parent: Optional[ET.Element], tag: str) -> Optional[float]:
                if parent is None:
                    return None
                sub = parent.find(tag)
                if sub is None or sub.text is None:
                    return None
                try:
                    return float(sub.text)
                except ValueError:
                    return None

            def _get_text_str(parent: Optional[ET.Element], tag: str) -> Optional[str]:
                if parent is None:
                    return None
                sub = parent.find(tag)
                if sub is None or sub.text is None:
                    return None
                return sub.text.strip()

            mag = _get_text_float(core_el, "mag")
            lat = _get_text_float(core_el, "lat")
            lon = _get_text_float(core_el, "lon")
            depth = _get_text_float(core_el, "depth")
            likelihood = _get_text_float(core_el, "likelihood")
            orig_time_str = _get_text_str(core_el, "orig_time") or ""

            core = DetectionCore(
                id=str(event_id),
                mag=mag if mag is not None else 0.0,
                lat=lat if lat is not None else 0.0,
                lon=lon if lon is not None else 0.0,
                depth=depth if depth is not None else 0.0,
                orig_time=orig_time_str or (timestamp_attr or "1970-01-01T00:00:00Z"),
                likelihood=likelihood,
            )

            # fault_info vertices
            fault_vertices: List[FaultVertex] = []
            for v in root.findall("fault_info/finite_fault/segment/vertices/vertex"):
                v_lat = v.findtext("lat")
                v_lon = v.findtext("lon")
                v_dep = v.findtext("depth")
                try:
                    fault_vertices.append(
                        FaultVertex(
                            lat=float(v_lat) if v_lat is not None else 0.0,
                            lon=float(v_lon) if v_lon is not None else 0.0,
                            depth=float(v_dep) if v_dep is not None else 0.0,
                        )
                    )
                except ValueError:
                    continue

            # gm_info: pga_obs
            pga_list: List[GMObs] = []
            pga_root = root.find("gm_info/gmpoint_obs/pga_obs")
            if pga_root is not None:
                for obs in pga_root.findall("obs"):
                    sncl = (obs.findtext("SNCL") or "").strip()
                    value_txt = obs.findtext("value") or "0"
                    lat_txt = obs.findtext("lat") or "0"
                    lon_txt = obs.findtext("lon") or "0"
                    time_txt = (obs.findtext("time") or "").strip()
                    try:
                        value = float(value_txt)
                        lat_obs = float(lat_txt)
                        lon_obs = float(lon_txt)
                    except ValueError:
                        continue
                    pga_list.append(
                        GMObs(
                            orig_sys="finder",
                            SNCL=sncl,
                            value=value,
                            lat=lat_obs,
                            lon=lon_obs,
                            time=time_txt,
                        )
                    )

            timestamp_final = timestamp_attr or core.orig_time

            dets.append(
                Detection(
                    timestamp=timestamp_final,
                    event_id=str(event_id),
                    category=category,
                    instance=instance,
                    orig_sys=orig_sys,
                    version=int(version_attr) if version_attr.isdigit() else 0,
                    core_info=core,
                    fault_info=fault_vertices,
                    gm_info={"pgv_obs": [], "pga_obs": pga_list},
                )
            )

            # Continue scanning after the end of this XML block
            i = j + 1

        return dets


class NativeFinderDialect(FinderBaseDialect):
    """
    Native Finder dialect parser for direct FinDer log output.
    """
    # Just reuse the same P_PREFIX_TS
    P_PREFIX_TS = FinderBaseDialect.P_PREFIX_TS

    # No START_PLAYBACK_RE, or a different one, e.g. line that marks "Finder started"
    START_PLAYBACK_RE = None

    PROFILE_NAME: str = "profiles/finder_native_time_vs_mag.json"  


class NativeFinderLegacyDialect(FinderBaseDialect):
    # P_PREFIX_TS might be None or a no-op; annotations get no ts or use origin time.
    P_PREFIX_TS = re.compile(r"^$")
    START_PLAYBACK_RE = None

    # Legacy native logs usually carry only epoch-style timestamps inside the block,
    # e.g. "Timestamp = 1723616759" or
    #      "process: timestamp in process function = 1723616759"
    P_TS_EPOCH = re.compile(r"\bTimestamp\s*=\s*(\d+)")
    P_TS_PROCESS = re.compile(r"timestamp in process function\s*=\s*(\d+)")

    def _pick_detection_timestamp(
        self,
        block_lines: List[str],
        emission_ts_iso: Optional[str],
        core_orig_time: str,
    ) -> str:
        """
        For legacy native FinDer logs that do not have a wall-clock prefix per line.

        Strategy:
        - If a wall-clock emission timestamp somehow exists, keep the base behaviour.
        - Otherwise, look for epoch-style timestamps in the detection block:
            * "Timestamp = 1723616759"
            * "process: timestamp in process function = 1723616759"
        - Convert the first epoch we find to ISO Z.
        - If nothing is found, fall back to core_orig_time.
        """
        # 1) If the base machinery already found a wall-clock timestamp, use it.
        if emission_ts_iso:
            return emission_ts_iso

        # 2) Look for a plain "Timestamp = <epoch>" first.
        for line in block_lines:
            m = self.P_TS_EPOCH.search(line)
            if m:
                return epoch_to_iso_z(m.group(1))

        # 3) Then try "timestamp in process function = <epoch>".
        for line in block_lines:
            m = self.P_TS_PROCESS.search(line)
            if m:
                return epoch_to_iso_z(m.group(1))

        # 4) Last resort: use the origin time we already have in the core info.
        return core_orig_time