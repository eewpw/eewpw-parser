# -*- coding: utf-8 -*-
from typing import Protocol, Optional, Dict, List
from pathlib import Path
import json

from .schemas import Meta, Detection, Annotation, FinalDoc
from .dedup import deduplicate_detections


class BaseSink(Protocol):
    def start_run(self) -> None:
        """Initialize sink resources for a parsing run."""

    def emit_detection(self, det: Detection) -> None:
        """Handle a single Detection as it is produced."""

    def emit_annotation(self, profile: str, ann: Annotation) -> None:
        """Handle a single Annotation tied to a profile name."""

    def finalize(self, meta: Meta) -> Optional[FinalDoc]:
        """Flush and optionally return a FinalDoc."""


class FinalDocSink(BaseSink):
    def __init__(self):
        self._meta: Optional[Meta] = None
        self._detections: List[Detection] = []
        self._annotations: Dict[str, List[Annotation]] = {}

    def start_run(self) -> None:
        # No-op for batch sink; kept for interface symmetry.
        return None

    def emit_detection(self, det: Detection) -> None:
        self._detections.append(det)

    def emit_annotation(self, profile: str, ann: Annotation) -> None:
        self._annotations.setdefault(profile, []).append(ann)

    def finalize(self, meta: Meta) -> Optional[FinalDoc]:
        self._meta = meta
        dets = deduplicate_detections(self._detections)
        dets.sort(key=lambda x: x.timestamp)
        return FinalDoc(meta=meta, annotations=self._annotations, detections=dets)


class JsonlStreamSink(BaseSink):
    def __init__(self, path: Path, algo: str, dialect: str, instance: str, verbose: bool = False):
        """Stream detections/annotations to a JSONL file."""
        self._path = path
        self._algo = algo
        self._dialect = dialect
        self._instance = instance
        self.verbose = verbose
        self._fh = path.open("w", encoding="utf-8")

    def start_run(self) -> None:
        # No-op; file is opened on init.
        return None

    def _write_line(self, obj: dict) -> None:
        self._fh.write(json.dumps(obj, ensure_ascii=False, separators=(",", ":")) + "\n")
        self._fh.flush()
        if self.verbose:
            payload = obj.get("payload") or {}
            ts = payload.get("timestamp") or payload.get("orig_time") or payload.get("started_at") or "-"
            print(f"[stream] {obj.get('record_type')} @ {ts}", flush=True)

    def emit_detection(self, det: Detection) -> None:
        rec = {
            "record_type": "detection",
            "algo": self._algo,
            "dialect": self._dialect,
            "instance": self._instance,
            "payload": det.dict(),
        }
        self._write_line(rec)

    def emit_annotation(self, profile: str, ann: Annotation) -> None:
        rec = {
            "record_type": "annotation",
            "algo": self._algo,
            "dialect": self._dialect,
            "instance": self._instance,
            "profile": profile,
            "payload": ann.dict(),
        }
        self._write_line(rec)

    def finalize(self, meta: Meta) -> Optional[FinalDoc]:
        rec = {
            "record_type": "meta",
            "algo": self._algo,
            "dialect": self._dialect,
            "instance": self._instance,
            "payload": meta.dict(),
        }
        self._write_line(rec)
        self._fh.close()
        return None
