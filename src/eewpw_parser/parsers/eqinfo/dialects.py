# -*- coding: utf-8 -*-
import re
import xml.etree.ElementTree as ET
from dataclasses import dataclass, field
from datetime import date, datetime, timedelta, timezone
from typing import List, Dict, Any, Optional, Tuple

from eewpw_parser.schemas import (
    Annotation,
    Detection,
    DetectionCore,
    FaultVertex,
    GMObs,
    GMInfo,
)
from eewpw_parser.config import load_profile


_XML_DECL_RE = re.compile(r"<\?xml[^>]*\?>", flags=re.IGNORECASE)


def _strip_ns(tag: str) -> str:
    if "}" in tag:
        return tag.split("}", 1)[1]
    return tag


def _text(elem: Optional[ET.Element]) -> str:
    if elem is None or elem.text is None:
        return ""
    return elem.text.strip()


@dataclass
class EqinfoStreamState:
    start_date: Optional[date] = None
    current_date: Optional[date] = None
    prev_total_micros: Optional[int] = None
    file_start_ts_iso: Optional[str] = None
    file_end_ts_iso: Optional[str] = None
    rollover_count: int = 0
    in_block: bool = False
    buffer: List[str] = field(default_factory=list)
    # Absolute line counter is 1-based for consistency across annotations and tests.
    absolute_line_counter: int = 0


class EqinfoShakeAlertDialect:
    PROFILE_NAME: str = "profiles/eqinfo_time_vs_mag.json"
    DIALECT_ID: str = "shakealert"

    P_HEADER = re.compile(
        r"(\d{4})/(\d{2})/(\d{2}),\d{2}:\d{2}:\d{2}\.\d+.*Logfile initialized",
        re.IGNORECASE,
    )
    P_TIME_PREFIX = re.compile(r"^(\d{2}):(\d{2}):(\d{2}):(\d{3,6})\|")
    P_LEVEL = re.compile(r"^\s*([A-Za-z]+)\s*\|\s*(.*)$")

    @property
    def profile(self) -> dict:
        if not hasattr(self, "_profile_cache"):
            self._profile_cache = load_profile(self.PROFILE_NAME)
        return self._profile_cache

    @staticmethod
    def normalize_line(line: str) -> Optional[str]:
        m = EqinfoShakeAlertDialect.P_TIME_PREFIX.match(line)
        if not m:
            return None
        rest = line[m.end() :].lstrip()
        m_level = EqinfoShakeAlertDialect.P_LEVEL.match(rest)
        if m_level:
            rest = m_level.group(2).lstrip()
        return rest

    def _extract_start_date(self, lines: List[str]) -> Optional[date]:
        for line in lines:
            m = self.P_HEADER.search(line)
            if not m:
                continue
            return date(int(m.group(1)), int(m.group(2)), int(m.group(3)))
        return None

    def _update_wall_clock(self, line: str, state: EqinfoStreamState) -> Optional[str]:
        m = self.P_TIME_PREFIX.match(line)
        if not m:
            return None

        if state.current_date is None:
            if state.start_date is None:
                state.start_date = datetime.now().date()
            state.current_date = state.start_date

        hour = int(m.group(1))
        minute = int(m.group(2))
        second = int(m.group(3))
        frac = m.group(4)
        if len(frac) > 6:
            frac = frac[:6]
        micros = int(frac.ljust(6, "0"))
        total_micros = ((hour * 3600 + minute * 60 + second) * 1_000_000) + micros

        if state.prev_total_micros is not None:
            if state.prev_total_micros - total_micros > 6 * 3600 * 1_000_000:
                state.current_date = state.current_date + timedelta(days=1)
                state.rollover_count += 1
        state.prev_total_micros = total_micros

        dt = datetime(
            state.current_date.year,
            state.current_date.month,
            state.current_date.day,
            hour,
            minute,
            second,
            micros,
            tzinfo=timezone.utc,
        )
        ts_iso = dt.strftime("%Y-%m-%dT%H:%M:%S.%fZ")

        if state.file_start_ts_iso is None:
            state.file_start_ts_iso = ts_iso
        state.file_end_ts_iso = ts_iso
        return ts_iso

    def _parse_obs(self, obs_elem: ET.Element) -> GMObs:
        field_values: Dict[str, str] = {}
        for child in list(obs_elem):
            tag = _strip_ns(child.tag)
            field_values[tag] = _text(child)

        obs_extra: Dict[str, Any] = {}
        if obs_elem.attrib:
            obs_extra = {k: str(v) for k, v in obs_elem.attrib.items()}

        return GMObs(
            orig_sys="",
            SNCL=field_values.get("SNCL", ""),
            value=field_values.get("value", ""),
            lat=field_values.get("lat", ""),
            lon=field_values.get("lon", ""),
            time=field_values.get("time", ""),
            extra=obs_extra,
        )

    def _parse_event_message(self, xml_text: str) -> Optional[Detection]:
        cleaned = _XML_DECL_RE.sub("", xml_text).strip()
        event_idx = cleaned.find("<event_message")
        if event_idx > 0:
            cleaned = cleaned[event_idx:]
        end_idx = cleaned.find("</event_message>")
        if end_idx != -1:
            cleaned = cleaned[: end_idx + len("</event_message>")]

        try:
            root = ET.fromstring(cleaned)
        except ET.ParseError:
            return None

        attrs = dict(root.attrib)
        timestamp = attrs.pop("timestamp", "")
        category = attrs.pop("category", "")
        instance = attrs.pop("instance", "")
        orig_sys = attrs.pop("orig_sys", "")
        version = attrs.pop("version", "")

        core_info_elem: Optional[ET.Element] = None
        gm_info_elem: Optional[ET.Element] = None
        contributors_elem: Optional[ET.Element] = None
        fault_info_elem: Optional[ET.Element] = None

        for child in list(root):
            tag = _strip_ns(child.tag)
            if tag == "core_info":
                core_info_elem = child
            elif tag == "gm_info":
                gm_info_elem = child
            elif tag == "contributors":
                contributors_elem = child
            elif tag == "fault_info":
                fault_info_elem = child

        event_id = ""
        mag = ""
        lat = ""
        lon = ""
        depth = ""
        orig_time = ""
        likelihood = ""
        num_stations = ""

        if core_info_elem is not None:
            event_id = str(core_info_elem.attrib.get("id", ""))
            for child in list(core_info_elem):
                tag = _strip_ns(child.tag)
                text = _text(child)
                if tag == "mag":
                    mag = text
                elif tag == "lat":
                    lat = text
                elif tag == "lon":
                    lon = text
                elif tag == "depth":
                    depth = text
                elif tag == "orig_time":
                    orig_time = text
                elif tag == "likelihood":
                    likelihood = text
                elif tag == "num_stations":
                    num_stations = text

        core_info = DetectionCore(
            id=event_id,
            mag=mag,
            lat=lat,
            lon=lon,
            depth=depth,
            orig_time=orig_time,
            likelihood=likelihood,
        )

        fault_vertices: List[FaultVertex] = []
        if fault_info_elem is not None:
            for elem in fault_info_elem.iter():
                if _strip_ns(elem.tag) != "vertex":
                    continue
                v_lat = ""
                v_lon = ""
                v_depth = ""
                for child in list(elem):
                    tag = _strip_ns(child.tag)
                    if tag == "lat":
                        v_lat = _text(child)
                    elif tag == "lon":
                        v_lon = _text(child)
                    elif tag == "depth":
                        v_depth = _text(child)
                fault_vertices.append(FaultVertex(lat=v_lat, lon=v_lon, depth=v_depth))

        pga_obs: List[GMObs] = []
        pgv_obs: List[GMObs] = []
        pgd_obs: List[GMObs] = []

        if gm_info_elem is not None:
            for child in list(gm_info_elem):
                if _strip_ns(child.tag) != "gmpoint_obs":
                    continue
                for sub in list(child):
                    sub_tag = _strip_ns(sub.tag)
                    if sub_tag == "pga_obs":
                        for obs in list(sub):
                            if _strip_ns(obs.tag) == "obs":
                                pga_obs.append(self._parse_obs(obs))
                    elif sub_tag == "pgv_obs":
                        for obs in list(sub):
                            if _strip_ns(obs.tag) == "obs":
                                pgv_obs.append(self._parse_obs(obs))
                    elif sub_tag == "pgd_obs":
                        for obs in list(sub):
                            if _strip_ns(obs.tag) == "obs":
                                pgd_obs.append(self._parse_obs(obs))

        gm_info = GMInfo(pga_obs=pga_obs, pgv_obs=pgv_obs, pgd_obs=pgd_obs)

        contributors: List[Dict[str, str]] = []
        if contributors_elem is not None:
            for child in list(contributors_elem):
                if _strip_ns(child.tag) != "contributor":
                    continue
                contributors.append({k: str(v) for k, v in child.attrib.items()})

        det_extra: Dict[str, Any] = {"contributors": contributors}
        if num_stations != "":
            det_extra["num_stations"] = num_stations

        return Detection(
            timestamp=timestamp,
            event_id=event_id,
            category=category,
            instance=instance,
            orig_sys=orig_sys,
            version=version,
            core_info=core_info,
            fault_info=fault_vertices,
            gm_info=gm_info,
            extra=det_extra,
        )

    def _finalize_block(self, block: str) -> Optional[Detection]:
        if "<event_message" not in block:
            return None
        return self._parse_event_message(block)

    def parse_stream(
        self,
        lines: List[str],
        state: Optional[EqinfoStreamState] = None,
        finalize: bool = False,
    ) -> Tuple[List[Detection], List[Annotation], EqinfoStreamState]:
        if state is None:
            state = EqinfoStreamState()

        detections: List[Detection] = []
        annotations: List[Annotation] = []

        patterns_cfg = self.profile.get("patterns", {})
        pattern_items = list(patterns_cfg.items())

        for line in lines:
            state.absolute_line_counter += 1

            ts_iso = self._update_wall_clock(line, state)
            if ts_iso:
                for idx, (_, pat) in enumerate(pattern_items):
                    if re.search(pat, line):
                        annotations.append(
                            Annotation(
                                timestamp=ts_iso,
                                pattern=pat,
                                line=str(state.absolute_line_counter),
                                text=line.rstrip("\n"),
                                pattern_id=f"eqinfo/{self.DIALECT_ID}:{idx}",
                            )
                        )

            normalized = self.normalize_line(line)
            if normalized is None:
                continue

            stripped = normalized.strip()
            buffer_ok = stripped == "" or normalized.lstrip().startswith("<")
            has_start = "<?xml" in normalized or "<event_message" in normalized
            has_end = "</event_message>" in normalized

            if not state.in_block:
                if has_start:
                    state.in_block = True
                    state.buffer = []
                    if buffer_ok:
                        state.buffer.append(normalized)
                    if has_end:
                        block = "\n".join(state.buffer)
                        state.buffer = []
                        state.in_block = False
                        det = self._finalize_block(block)
                        if det is not None:
                            detections.append(det)
            else:
                if has_start:
                    state.buffer = []
                    state.in_block = True
                    if buffer_ok:
                        state.buffer.append(normalized)
                    if has_end:
                        block = "\n".join(state.buffer)
                        state.buffer = []
                        state.in_block = False
                        det = self._finalize_block(block)
                        if det is not None:
                            detections.append(det)
                    continue

                if stripped == "<":
                    state.buffer = []
                    state.in_block = False
                    continue

                if buffer_ok:
                    state.buffer.append(normalized)

                if has_end:
                    block = "\n".join(state.buffer)
                    state.buffer = []
                    state.in_block = False
                    det = self._finalize_block(block)
                    if det is not None:
                        detections.append(det)

        if finalize and state.in_block:
            state.buffer = []
            state.in_block = False

        return detections, annotations, state

    def parse_file(self, path: str) -> Tuple[List[Detection], List[Annotation], Dict[str, Any]]:
        with open(path, "r", encoding="utf-8", errors="ignore") as f:
            lines = f.readlines()

        state = EqinfoStreamState()
        state.start_date = self._extract_start_date(lines)

        detections, annotations, state = self.parse_stream(lines, state=state, finalize=True)

        extra: Dict[str, Any] = {
            "file": str(path),
            "started_at": state.file_start_ts_iso,
            "finished_at": state.file_end_ts_iso,
            "rollover_count": state.rollover_count,
            "stats": {
                "detections": len(detections),
                "annotations": len(annotations),
            },
        }

        return detections, annotations, extra
