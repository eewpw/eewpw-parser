# -*- coding: utf-8 -*-
from __future__ import annotations
import json
from datetime import timezone
from pathlib import Path
from typing import Optional, TextIO

from dateutil import parser as dtp

from .schemas import Detection, Annotation, Meta
from .config import get_live_raw_dir, get_live_daily_jsonl_path
from .utils import to_iso_utc_z


class LiveWriter:
    """
    Legacy per-event writer kept for older tests; new live output flows use DailyAlgoWriter.
    """

    def __init__(self, path: Path, algo: str, dialect: str, instance: str, verbose: bool = False):
        self.path = Path(path)
        self.algo = algo
        self.dialect = dialect
        self.instance = instance
        self.verbose = verbose
        self._fh = self.path.open("a", encoding="utf-8")

    def _write_line(self, record_type: str, payload: dict, profile: Optional[str] = None) -> None:
        obj = {
            "record_type": record_type,
            "algo": self.algo,
            "dialect": self.dialect,
            "instance": self.instance,
            "payload": payload,
        }
        if profile is not None:
            obj["profile"] = profile
        line = json.dumps(obj, ensure_ascii=False, separators=(",", ":")) + "\n"
        self._fh.write(line)
        self._fh.flush()
        if self.verbose:
            ts = (
                payload.get("timestamp")
                or payload.get("orig_time")
                or payload.get("started_at")
                or "-"
            )
            print(f"[live-writer] {record_type} @ {ts} -> {self.path}", flush=True)

    def write_detection(self, det: Detection) -> None:
        self._write_line("detection", det.model_dump())

    def write_annotation(self, profile: str, ann: Annotation) -> None:
        self._write_line("annotation", ann.model_dump(), profile=profile)

    def write_meta(self, meta: Meta) -> None:
        self._write_line("meta", meta.model_dump())

    def close(self) -> None:
        try:
            self._fh.close()
        except Exception:
            pass


class DailyAlgoWriter:
    """
    Rolling daily writer that appends all records for a given algo into
    data_root/live/raw/<algo>/<YYYY-MM-DD>_<algo>.jsonl
    """

    def __init__(self, data_root: Path, algo: str, dialect: str, instance: str, verbose: bool = False):
        self.data_root = Path(data_root)
        self.algo = algo
        self.dialect = dialect
        self.instance = instance
        self.verbose = verbose
        self._dir = get_live_raw_dir(self.data_root, self.algo)
        self._current_date: Optional[str] = None
        self._fh: Optional[TextIO] = None

    def _ensure_handle(self, date_str: str) -> None:
        if self._current_date == date_str and self._fh:
            return
        if self._fh:
            try:
                self._fh.close()
            except Exception:
                pass
        path = get_live_daily_jsonl_path(self.data_root, self.algo, date_str)
        path.parent.mkdir(parents=True, exist_ok=True)
        self._fh = path.open("a", encoding="utf-8")
        self._current_date = date_str
        if self.verbose:
            print(f"[live-writer] open {path}", flush=True)

    def _write_line(
        self,
        record_type: str,
        payload: dict,
        timestamp_iso: str,
        event_id: str,
        profile: Optional[str],
    ) -> None:
        if self._fh is None:
            raise RuntimeError("Writer handle not initialized")
        obj = {
            "record_type": record_type,
            "algo": self.algo,
            "dialect": self.dialect,
            "instance": self.instance,
            "event_id": event_id,
            "timestamp": timestamp_iso,
            "payload": payload,
        }
        if profile is not None:
            obj["profile"] = profile
        line = json.dumps(obj, ensure_ascii=False, separators=(",", ":")) + "\n"
        self._fh.write(line)
        self._fh.flush()
        if self.verbose:
            print(f"[live-writer] {record_type} @ {timestamp_iso} -> {self._fh.name}", flush=True)

    def _date_from_iso(self, ts_iso: str) -> str:
        dt = dtp.parse(ts_iso)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        else:
            dt = dt.astimezone(timezone.utc)
        return dt.strftime("%Y-%m-%d")

    def write_detection(self, det: Detection) -> None:
        ts_iso = det.timestamp
        date_str = self._date_from_iso(ts_iso)
        self._ensure_handle(date_str)
        self._write_line("detection", det.model_dump(), ts_iso, str(det.event_id), None)

    def write_annotation(self, profile: str, ann: Annotation, event_id: str) -> None:
        ts_iso = ann.timestamp
        date_str = self._date_from_iso(ts_iso)
        self._ensure_handle(date_str)
        self._write_line("annotation", ann.model_dump(), ts_iso, event_id, profile)

    def write_meta(self, meta: Meta) -> None:
        ts_iso = meta.started_at or meta.finished_at or to_iso_utc_z("1970-01-01T00:00:00Z")
        date_str = self._date_from_iso(ts_iso)
        self._ensure_handle(date_str)
        self._write_line("meta", meta.model_dump(), ts_iso, "", None)

    def close(self) -> None:
        if self._fh:
            try:
                self._fh.close()
            except Exception:
                pass
        self._fh = None
