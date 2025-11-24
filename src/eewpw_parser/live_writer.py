# -*- coding: utf-8 -*-
from __future__ import annotations
import json
from pathlib import Path
from typing import Optional

from .schemas import Detection, Annotation, Meta


class LiveWriter:
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
        self._write_line("detection", det.dict())

    def write_annotation(self, profile: str, ann: Annotation) -> None:
        self._write_line("annotation", ann.dict(), profile=profile)

    def write_meta(self, meta: Meta) -> None:
        self._write_line("meta", meta.dict())

    def close(self) -> None:
        try:
            self._fh.close()
        except Exception:
            pass
