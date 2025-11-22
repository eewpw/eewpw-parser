# -*- coding: utf-8 -*-
from typing import List, Dict, Any
from pathlib import Path
from dateutil import parser as dtp
from eewpw_parser.schemas import FinalDoc, Meta
from eewpw_parser.parsers.finder.dialects import SCFinderDialect, NativeFinderDialect, NativeFinderLegacyDialect, ShakeAlertFinderDialect
from eewpw_parser.utils import to_iso_utc_z

class FinderParser:
    def __init__(self, cfg: Dict[str, Any]):
        self.cfg = cfg
        self.dialect = cfg.get("dialect", "scfinder")

    def parse(self, inputs: List[str]) -> FinalDoc:
        files = [str(Path(p)) for p in inputs]
        # Order is whatever user provided; this is safer than glob
        dets_all = []
        ann_all = []
        per_file_extras: List[Dict[str, Any]] = []

        if self.dialect == "scfinder":
            # Logs are from scfinder 
            worker = SCFinderDialect()

        elif self.dialect in ["native_finder", "native-finder", 
                              "nativefinder", "finder"]:
            # Logs are directly from Finder (with timestamp in each line)
            worker = NativeFinderDialect()

        elif self.dialect in ["native_finder_legacy", "native-finder-legacy", 
                              "nativefinderlegacy", "finder_legacy", 
                              "finder-legacy", "finderlegacy"]:
            # Logs are from older versions of Finder (no timestamps in lines)
            worker = NativeFinderLegacyDialect()

        elif self.dialect == "shakealert":
            # Logs are from ShakeAlert system using Finder internally
            worker = ShakeAlertFinderDialect()

        else:
            raise ValueError(f"Unsupported Finder dialect: {self.dialect}")

        for p in files:
            d, a, extras = worker.parse_file(p)
            dets_all.extend(d)
            ann_all.extend(a)
            per_file_extras.append(extras)

        # Sort detections by timestamp ascending
        dets_all.sort(key=lambda x: x.timestamp)

        # --- derive started_at, finished_at from per-file extras ---
        start_candidates = []
        end_candidates = []
        for fx in per_file_extras:
            s = fx.get("started_at")
            e = fx.get("finished_at")
            if s:
                start_candidates.append(dtp.parse(s))
            if e:
                end_candidates.append(dtp.parse(e))

        if start_candidates and end_candidates:
            started_at_iso = min(start_candidates).isoformat().replace("+00:00", "Z")
            finished_at_iso = max(end_candidates).isoformat().replace("+00:00", "Z")
        elif dets_all:
            started_at_iso = dets_all[0].timestamp
            finished_at_iso = dets_all[-1].timestamp
        else:
            started_at_iso = None
            finished_at_iso = None

        # --- global stats at meta level ---
        stats_total = {
            "detections": len(dets_all),
            "annotations": len(ann_all),
            "files": len(files),
        }

        # Per-file extras live under meta.extras["files"]
        extras = {
            "files": per_file_extras,
        }

        meta = Meta(
            algo="finder",
            dialect=self.dialect,
            started_at=started_at_iso,
            finished_at=finished_at_iso,
            extras=extras,
            stats_total=stats_total,
        )

        # Attach annotations under a single profile key for now
        annotations = {"time_vs_magnitude": ann_all}

        doc = FinalDoc(meta=meta, annotations=annotations, detections=dets_all)
        return doc