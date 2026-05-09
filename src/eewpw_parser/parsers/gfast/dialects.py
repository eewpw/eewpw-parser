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
_INCOMPLETE_SENTINEL = "__incomplete_block__"


def _autoclose_event_message(xml_text: str) -> str:
    suffixes: List[str] = []

    if xml_text.count("<obs") > xml_text.count("</obs>"):
        suffixes.append("\n</obs>")
    if "<pga_obs" in xml_text and "</pga_obs>" not in xml_text:
        suffixes.append("\n</pga_obs>")
    if "<pgv_obs" in xml_text and "</pgv_obs>" not in xml_text:
        suffixes.append("\n</pgv_obs>")
    if "<gmpoint_obs" in xml_text and "</gmpoint_obs>" not in xml_text:
        suffixes.append("\n</gmpoint_obs>")
    if "<gm_info" in xml_text and "</gm_info>" not in xml_text:
        suffixes.append("\n</gm_info>")
    if "<contributors" in xml_text and "</contributors>" not in xml_text:
        suffixes.append("\n</contributors>")
    if "<core_info" in xml_text and "</core_info>" not in xml_text:
        suffixes.append("\n</core_info>")
    if "</event_message>" not in xml_text:
        suffixes.append("\n</event_message>")

    if not suffixes:
        return xml_text
    return f"{xml_text}{''.join(suffixes)}"


def _strip_ns(tag: str) -> str:
    if "}" in tag:
        return tag.split("}", 1)[1]
    return tag


def _text(elem: Optional[ET.Element]) -> str:
    if elem is None or elem.text is None:
        return ""
    return elem.text.strip()


@dataclass
class GfastStreamState:
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


class GfastShakeAlertDialect:
    PROFILE_NAME: str = "profiles/gfast_time_vs_mag.json"
    DIALECT_ID: str = "shakealert"

    P_HEADER = re.compile(
        r"(\d{4})/(\d{2})/(\d{2}),\d{2}:\d{2}:\d{2}\.\d+.*Change of Calendar Date",
        re.IGNORECASE,
    )
    P_TIME_PREFIX = re.compile(r"^(\d{2}):(\d{2}):(\d{2}):(\d{3,6})\|")
    P_LEVEL = re.compile(r"^\d{2}:\d{2}:\d{2}:\d{3,6}\|\s*[A-Z]+\s*\|\s*(.*)$")

    @property
    def profile(self) -> dict:
        if not hasattr(self, "_profile_cache"):
            self._profile_cache = load_profile(self.PROFILE_NAME)
        return self._profile_cache

    @staticmethod
    def normalize_line(line: str) -> Optional[str]:
        m = GfastShakeAlertDialect.P_LEVEL.match(line)
        if not m:
            return None
        return m.group(1).lstrip()

    def _extract_start_date(self, lines: List[str]) -> Optional[date]:
        for line in lines:
            m = self.P_HEADER.search(line)
            if not m:
                continue
            return date(int(m.group(1)), int(m.group(2)), int(m.group(3)))
        return None

    def _update_wall_clock(self, line: str, state: GfastStreamState) -> Optional[str]:
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
        field_units: Dict[str, str] = {}
        field_attrs: Dict[str, Dict[str, str]] = {}

        for child in list(obs_elem):
            tag = _strip_ns(child.tag)
            field_values[tag] = _text(child)
            if child.attrib:
                field_attrs[tag] = dict(child.attrib)
                if "units" in child.attrib:
                    field_units[tag] = child.attrib.get("units", "")

        obs_extra: Dict[str, Any] = {}
        assoc = obs_elem.attrib.get("assoc")
        if assoc is not None:
            obs_extra["assoc"] = assoc
        if obs_elem.attrib:
            obs_extra["attrs"] = dict(obs_elem.attrib)
        if field_units:
            obs_extra["units"] = field_units
        if field_attrs:
            obs_extra["field_attrs"] = field_attrs

        unknown_fields = {
            tag: text
            for tag, text in field_values.items()
            if tag not in {"SNCL", "value", "lat", "lon", "time"}
        }
        if unknown_fields:
            obs_extra["fields"] = unknown_fields

        return GMObs(
            orig_sys=obs_elem.attrib.get("orig_sys"),
            SNCL=field_values.get("SNCL", ""),
            value=field_values.get("value", ""),
            lat=field_values.get("lat", ""),
            lon=field_values.get("lon", ""),
            time=field_values.get("time", ""),
            extra=obs_extra,
        )

    def _parse_event_message(
        self, xml_text: str, incomplete: bool = False, incomplete_reason: str = ""
    ) -> Detection:
        cleaned = _XML_DECL_RE.sub("", xml_text).strip()
        sanitized = re.sub(r"[\x00-\x08\x0b\x0c\x0e-\x1f]", "", cleaned)
        event_idx = sanitized.find("<event_message")
        if event_idx > 0:
            sanitized = sanitized[event_idx:]
        if incomplete:
            sanitized = _autoclose_event_message(sanitized)
        try:
            root = ET.fromstring(sanitized)
        except ET.ParseError as err:
            raise

        attrs = dict(root.attrib)
        timestamp = attrs.pop("timestamp", "")
        category = attrs.pop("category", "")
        instance = attrs.pop("instance", "")
        orig_sys = attrs.pop("orig_sys", "")
        version = attrs.pop("version", "")

        event_message_attrs = dict(attrs)

        core_info_elem: Optional[ET.Element] = None
        gm_info_elem: Optional[ET.Element] = None
        contributors_elem: Optional[ET.Element] = None
        fault_info_elem: Optional[ET.Element] = None
        other_children: List[ET.Element] = []

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
            else:
                other_children.append(child)

        event_id = ""
        mag = ""
        lat = ""
        lon = ""
        depth = ""
        orig_time = ""
        likelihood: Optional[str] = None
        core_info_extra: Dict[str, Any] = {}

        if core_info_elem is not None:
            event_id = str(core_info_elem.attrib.get("id", ""))
            extra_core_attrs = {
                k: v for k, v in core_info_elem.attrib.items() if k != "id"
            }
            if extra_core_attrs:
                core_info_extra["__core_info__"] = {"text": "", "attrs": extra_core_attrs}

            for child in list(core_info_elem):
                tag = _strip_ns(child.tag)
                text = _text(child)
                attrs = dict(child.attrib) if child.attrib else {}
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
                    likelihood = text if text != "" else None

                if tag not in {"mag", "lat", "lon", "depth", "orig_time", "likelihood"} or attrs:
                    core_info_extra[tag] = {"text": text, "attrs": attrs}

        core_info = DetectionCore(
            id=event_id,
            mag=mag,
            lat=lat,
            lon=lon,
            depth=depth,
            orig_time=orig_time,
            likelihood=likelihood,
        )

        pga_obs: List[GMObs] = []
        pgv_obs: List[GMObs] = []
        gm_extra: Dict[str, Any] = {}

        if gm_info_elem is not None:
            if gm_info_elem.attrib:
                gm_extra["gm_info_attrs"] = dict(gm_info_elem.attrib)
            for child in list(gm_info_elem):
                tag = _strip_ns(child.tag)
                if tag == "gmpoint_obs":
                    if child.attrib:
                        gm_extra["gmpoint_obs_attrs"] = dict(child.attrib)
                    for sub in list(child):
                        sub_tag = _strip_ns(sub.tag)
                        if sub_tag == "pga_obs":
                            if sub.attrib:
                                gm_extra["pga_obs_attrs"] = dict(sub.attrib)
                            for obs in list(sub):
                                if _strip_ns(obs.tag) == "obs":
                                    pga_obs.append(self._parse_obs(obs))
                        elif sub_tag == "pgv_obs":
                            if sub.attrib:
                                gm_extra["pgv_obs_attrs"] = dict(sub.attrib)
                            for obs in list(sub):
                                if _strip_ns(obs.tag) == "obs":
                                    pgv_obs.append(self._parse_obs(obs))
                        else:
                            gm_extra.setdefault("gmpoint_other", []).append(
                                {"tag": sub_tag, "text": _text(sub), "attrs": dict(sub.attrib)}
                            )
                else:
                    gm_extra.setdefault("other", []).append(
                        {"tag": tag, "text": _text(child), "attrs": dict(child.attrib)}
                    )

        gm_info = GMInfo(pga_obs=pga_obs, pgv_obs=pgv_obs, extra=gm_extra)

        fault_vertices: List[FaultVertex] = []
        if fault_info_elem is not None:
            for elem in fault_info_elem.iter():
                if _strip_ns(elem.tag) != "vertex":
                    continue
                lat = ""
                lon = ""
                depth_value = ""
                for child in list(elem):
                    tag = _strip_ns(child.tag)
                    if tag == "lat":
                        lat = _text(child)
                    elif tag == "lon":
                        lon = _text(child)
                    elif tag == "depth":
                        depth_value = _text(child)
                fault_vertices.append(
                    FaultVertex(lat=lat, lon=lon, depth=depth_value)
                )

        contributors: List[Dict[str, str]] = []
        if contributors_elem is not None:
            for child in list(contributors_elem):
                if _strip_ns(child.tag) != "contributor":
                    continue
                contributors.append({k: str(v) for k, v in child.attrib.items()})

        det_extra: Dict[str, Any] = {
            "event_message_attrs": event_message_attrs,
            "core_info_extra": core_info_extra,
            "contributors": contributors,
        }
        if other_children:
            det_extra["event_message_children"] = [
                {"tag": _strip_ns(child.tag), "text": _text(child), "attrs": dict(child.attrib)}
                for child in other_children
            ]

        det = Detection(
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
        if incomplete:
            det.extra["xml_incomplete"] = "true"
            det.extra["xml_incomplete_reason"] = incomplete_reason
        return det

    def parse_stream(
        self,
        lines: List[str],
        state: Optional[GfastStreamState] = None,
        finalize: bool = False,
    ) -> Tuple[List[Detection], List[Annotation], GfastStreamState]:
        if state is None:
            state = GfastStreamState()

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
                                pattern_id=f"gfast/{self.DIALECT_ID}:{idx}",
                            )
                        )

            normalized = self.normalize_line(line)
            if normalized is None:
                continue

            stripped = normalized.strip()
            buffer_ok = stripped == "" or re.search(r"<\s*[/?!A-Za-z]", stripped)
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
                        if "<event_message" not in block:
                            state.buffer = []
                            state.in_block = False
                            continue
                        if block.find("<event_message") != block.rfind("<event_message"):
                            block = block[block.rfind("<event_message") :]
                        state.buffer = []
                        state.in_block = False
                        try:
                            detections.append(self._parse_event_message(block))
                        except ET.ParseError:
                            continue
            else:
                if state.buffer == [_INCOMPLETE_SENTINEL]:
                    if has_end:
                        state.buffer = []
                        state.in_block = False
                    continue
                xml_line = normalized.lstrip()
                is_xmlish = xml_line.startswith("<") or xml_line.strip() == ""
                is_truncated_xml = xml_line.strip() == "<"
                if is_truncated_xml:
                    block = "\n".join(state.buffer)
                    state.buffer = [_INCOMPLETE_SENTINEL]
                    state.in_block = True
                    if block.find("<event_message") != block.rfind("<event_message"):
                        block = block[block.rfind("<event_message") :]
                    try:
                        detections.append(
                            self._parse_event_message(
                                block,
                                incomplete=True,
                                incomplete_reason="truncated_xml_line",
                            )
                        )
                    except ET.ParseError:
                        continue
                    continue
                if is_xmlish and buffer_ok:
                    state.buffer.append(normalized)
                if has_end:
                    block = "\n".join(state.buffer)
                    if "<event_message" not in block:
                        state.buffer = []
                        state.in_block = False
                        continue
                    if block.find("<event_message") != block.rfind("<event_message"):
                        block = block[block.rfind("<event_message") :]
                    state.buffer = []
                    state.in_block = False
                    try:
                        detections.append(self._parse_event_message(block))
                    except ET.ParseError:
                        continue

        if finalize and state.in_block:
            state.buffer = []
            state.in_block = False

        return detections, annotations, state

    def parse_file(self, path: str) -> Tuple[List[Detection], List[Annotation], Dict[str, Any]]:
        with open(path, "r", encoding="utf-8", errors="ignore") as f:
            lines = f.readlines()

        state = GfastStreamState()
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
