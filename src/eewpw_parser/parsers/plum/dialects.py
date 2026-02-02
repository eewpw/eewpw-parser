# -*- coding: utf-8 -*-
import re
import xml.etree.ElementTree as ET
from dataclasses import dataclass, field
from typing import List, Dict, Any, Tuple, Optional
from dateutil import parser as dtp

from eewpw_parser.schemas import (
    Annotation,
    Detection,
    DetectionCore,
    GMObs,
    GMInfo,
    GMGridCell,
)


_NUM_RE = re.compile(r"^-?\d+(?:\.\d+)?(?:[eE][-+]?\d+)?$")
_XML_DECL_RE = re.compile(r"<\?xml[^>]*\?>", flags=re.IGNORECASE)


def _strip_ns(tag: str) -> str:
    if "}" in tag:
        return tag.split("}", 1)[1]
    return tag


def _text(elem: Optional[ET.Element]) -> str:
    if elem is None or elem.text is None:
        return ""
    return elem.text.strip()


def _is_numeric_row(tokens: List[str]) -> bool:
    if len(tokens) != 5:
        return False
    return all(_NUM_RE.match(t) for t in tokens)


def _parse_grid_data(text: str) -> List[GMGridCell]:
    cells: List[GMGridCell] = []
    for line in text.splitlines():
        raw = line.strip()
        if not raw:
            continue
        tokens = raw.split()
        if not _is_numeric_row(tokens):
            continue
        cells.append(
            GMGridCell(
                lon=tokens[0],
                lat=tokens[1],
                pga=tokens[2],
                pgv=tokens[3],
                mmi=tokens[4],
            )
        )
    return cells


class PlumDialect:
    def __init__(self):
        self.event_seq = 0
        self.seen_any_block = False

    def _iter_event_blocks(self, lines: List[str]):
        buf: List[str] = []
        in_block = False
        for line in lines:
            if not in_block:
                if "<event_message" in line:
                    in_block = True
                    start_idx = line.find("<event_message")
                    buf.append(line[start_idx:])
                    if "</event_message>" in line:
                        block = "".join(buf)
                        yield block
                        buf = []
                        in_block = False
            else:
                buf.append(line)
                if "</event_message>" in line:
                    block = "".join(buf)
                    yield block
                    buf = []
                    in_block = False

    def parse_stream(
        self,
        lines: List[str],
        state: Optional["PlumStreamState"] = None,
        finalize: bool = False,
    ) -> Tuple[List[Detection], List[Annotation], "PlumStreamState"]:
        if state is None:
            state = PlumStreamState()
        dets: List[Detection] = []
        anns: List[Annotation] = []

        for line in lines:
            if not state.in_block:
                if "<event_message" in line:
                    state.in_block = True
                    start_idx = line.find("<event_message")
                    state.buffer = [line[start_idx:]]
                    if "</event_message>" in line:
                        block = "".join(state.buffer)
                        state.buffer = []
                        state.in_block = False
                        dets.append(self._parse_event_message(block))
            else:
                state.buffer.append(line)
                if "</event_message>" in line:
                    block = "".join(state.buffer)
                    state.buffer = []
                    state.in_block = False
                    dets.append(self._parse_event_message(block))

        if finalize and state.in_block:
            state.buffer = []
            state.in_block = False

        return dets, anns, state

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

    def _parse_gm_info(self, gm_info_elem: Optional[ET.Element]) -> GMInfo:
        if gm_info_elem is None:
            return GMInfo()

        pga_obs: List[GMObs] = []
        pgv_obs: List[GMObs] = []
        grid_data: List[GMGridCell] = []
        gm_extra: Dict[str, Any] = {}

        for child in list(gm_info_elem):
            tag = _strip_ns(child.tag)

            if tag == "gmpoint_obs":
                gmpoint_extra: Dict[str, Any] = {}
                if child.attrib:
                    gmpoint_extra["attrs"] = dict(child.attrib)
                for sub in list(child):
                    sub_tag = _strip_ns(sub.tag)
                    if sub_tag == "pga_obs":
                        if sub.attrib:
                            gmpoint_extra["pga_obs_attrs"] = dict(sub.attrib)
                        for obs in list(sub):
                            if _strip_ns(obs.tag) == "obs":
                                pga_obs.append(self._parse_obs(obs))
                    elif sub_tag == "pgv_obs":
                        if sub.attrib:
                            gmpoint_extra["pgv_obs_attrs"] = dict(sub.attrib)
                        for obs in list(sub):
                            if _strip_ns(obs.tag) == "obs":
                                pgv_obs.append(self._parse_obs(obs))
                    else:
                        gmpoint_extra.setdefault("other", []).append(
                            {"tag": sub_tag, "text": _text(sub), "attrs": dict(sub.attrib)}
                        )
                if gmpoint_extra:
                    gm_extra["gmpoint_obs"] = gmpoint_extra

            elif tag == "gmmap_pred":
                gmmap_extra: Dict[str, Any] = {}
                if child.attrib:
                    gmmap_extra["attrs"] = dict(child.attrib)
                grid_fields: List[Dict[str, str]] = []
                for sub in list(child):
                    sub_tag = _strip_ns(sub.tag)
                    if sub_tag == "grid_field":
                        grid_fields.append(dict(sub.attrib))
                    elif sub_tag == "grid_data":
                        grid_data = _parse_grid_data(_text(sub))
                    else:
                        gmmap_extra.setdefault("other", []).append(
                            {"tag": sub_tag, "text": _text(sub), "attrs": dict(sub.attrib)}
                        )
                if grid_fields:
                    gmmap_extra["grid_fields"] = grid_fields
                if gmmap_extra:
                    gm_extra["gmmap_pred"] = gmmap_extra
            else:
                gm_extra.setdefault("other", []).append(
                    {"tag": tag, "text": _text(child), "attrs": dict(child.attrib)}
                )

        return GMInfo(
            pga_obs=pga_obs,
            pgv_obs=pgv_obs,
            grid_data=grid_data,
            extra=gm_extra,
        )

    def _parse_core_info(self, core_elem: Optional[ET.Element]) -> Tuple[DetectionCore, Dict[str, Any]]:
        known_fields = {"mag", "lat", "lon", "depth", "orig_time", "likelihood"}
        core_vals: Dict[str, str] = {
            "id": "",
            "mag": "",
            "lat": "",
            "lon": "",
            "depth": "",
            "orig_time": "",
            "likelihood": "",
        }
        core_info_extra: Dict[str, Any] = {}

        if core_elem is None:
            core = DetectionCore(
                id="",
                mag="",
                lat="",
                lon="",
                depth="",
                orig_time="",
                likelihood=None,
            )
            return core, core_info_extra

        if core_elem.attrib:
            attrs = {k: v for k, v in core_elem.attrib.items() if k != "id"}
            if attrs:
                core_info_extra["_attrs"] = attrs
        core_vals["id"] = core_elem.attrib.get("id", "")

        for child in list(core_elem):
            tag = _strip_ns(child.tag)
            text = _text(child)
            if tag in known_fields:
                core_vals[tag] = text
            attrs = dict(child.attrib) if child.attrib else {}
            if attrs or tag not in known_fields:
                core_info_extra[tag] = {"text": text, "attrs": attrs} if attrs else {"text": text}

        core = DetectionCore(
            id=core_vals["id"],
            mag=core_vals["mag"],
            lat=core_vals["lat"],
            lon=core_vals["lon"],
            depth=core_vals["depth"],
            orig_time=core_vals["orig_time"],
            likelihood=core_vals["likelihood"] or None,
        )
        return core, core_info_extra

    def _parse_event_message(self, block: str) -> Detection:
        cleaned = _XML_DECL_RE.sub("", block)
        root = ET.fromstring(cleaned)

        header_attrs = dict(root.attrib)
        timestamp = header_attrs.pop("timestamp", "")
        category = header_attrs.pop("category", "")
        instance = header_attrs.pop("instance", "")
        orig_sys = header_attrs.pop("orig_sys", "")
        version = header_attrs.pop("version", "")

        if str(version) == "0":
            if self.seen_any_block:
                self.event_seq += 1
        if not self.seen_any_block:
            self.seen_any_block = True

        event_id = str(self.event_seq)

        core_elem = None
        gm_info_elem = None
        for child in list(root):
            tag = _strip_ns(child.tag)
            if tag == "core_info":
                core_elem = child
            elif tag == "gm_info":
                gm_info_elem = child

        core_info, core_info_extra = self._parse_core_info(core_elem)
        gm_info = self._parse_gm_info(gm_info_elem)

        det_extra: Dict[str, Any] = {}
        if header_attrs:
            det_extra.update(header_attrs)
        if core_info_extra:
            det_extra["core_info_extra"] = core_info_extra

        return Detection(
            timestamp=timestamp,
            event_id=event_id,
            category=category,
            instance=instance,
            orig_sys=orig_sys,
            version=str(version),
            core_info=core_info,
            gm_info=gm_info,
            extra=det_extra,
        )

    def parse_file(self, path: str) -> Tuple[List[Detection], List[Annotation], Dict[str, Any]]:
        detections: List[Detection] = []
        annotations: List[Annotation] = []

        with open(path, "r", encoding="utf-8", errors="ignore") as f:
            lines = f.readlines()

        for block in self._iter_event_blocks(lines):
            detections.append(self._parse_event_message(block))

        timestamps = [d.timestamp for d in detections if d.timestamp]
        started_at = None
        finished_at = None
        if timestamps:
            start_dt = min(dtp.parse(ts) for ts in timestamps)
            end_dt = max(dtp.parse(ts) for ts in timestamps)
            started_at = start_dt.isoformat().replace("+00:00", "Z")
            finished_at = end_dt.isoformat().replace("+00:00", "Z")

        extra: Dict[str, Any] = {
            "file": str(path),
            "started_at": started_at,
            "finished_at": finished_at,
            "stats": {
                "detections": len(detections),
                "annotations": len(annotations),
            },
        }

        return detections, annotations, extra


@dataclass
class PlumStreamState:
    in_block: bool = False
    buffer: List[str] = field(default_factory=list)
