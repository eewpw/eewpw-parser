# -*- coding: utf-8 -*-
from typing import List, Dict, Any, Optional
from pathlib import Path
from dateutil import parser as dtp
from eewpw_parser.schemas import FinalDoc, Meta
from eewpw_parser.parsers.finder.dialects import (
    FinderStreamState,
    SCFinderDialect,
    NativeFinderDialect,
    NativeFinderLegacyDialect,
    ShakeAlertFinderDialect,
)
from eewpw_parser.utils import to_iso_utc_z
from eewpw_parser.dedup import deduplicate_detections, deduplicate_annotations
from eewpw_parser.sinks import BaseSink

class FinderParser:
    def __init__(self, cfg: Dict[str, Any]):
        self.cfg = cfg
        self.dialect = cfg.get("dialect", "scfinder")
        self.verbose = bool(cfg.get("verbose", False))

    def _get_worker(self):
        if hasattr(self, "_worker"):
            return self._worker

        if self.dialect == "scfinder":
            worker = SCFinderDialect()

        elif self.dialect in ["native_finder", "native-finder", 
                              "nativefinder", "finder"]:
            worker = NativeFinderDialect()

        elif self.dialect in ["native_finder_legacy", "native-finder-legacy", 
                              "nativefinderlegacy", "finder_legacy", 
                              "finder-legacy", "finderlegacy"]:
            worker = NativeFinderLegacyDialect()

        elif self.dialect == "shakealert":
            worker = ShakeAlertFinderDialect()

        else:
            raise ValueError(f"Unsupported Finder dialect: {self.dialect}")

        self._worker = worker
        if hasattr(self._worker, "verbose"):
            self._worker.verbose = self.verbose
        return worker

    def parse_stream(
        self,
        lines: List[str],
        state: Optional[FinderStreamState] = None,
        finalize: bool = False,
    ):
        """
        Streaming entry point that keeps state between chunks.
        """
        worker = self._get_worker()
        return worker.parse_stream(lines, state=state, finalize=finalize)

    def parse(self, inputs: List[str], sink: Optional[BaseSink] = None) -> Optional[FinalDoc]:
        files = [str(Path(p)) for p in inputs]
        # Order is whatever user provided; this is safer than glob
        dets_all = []
        ann_all = []
        per_file_extras: List[Dict[str, Any]] = []

        worker = self._get_worker()

        if sink:
            sink.start_run()

        if self.verbose:
            print("==== Finder Parse ====")
            print(f"Dialect: {self.dialect} Files: {len(files)}")

        for p in files:
            d, a, extras = worker.parse_file(p)
            dets_all.extend(d)
            ann_all.extend(a)
            per_file_extras.append(extras)

            if sink:
                for det in d:
                    sink.emit_detection(det)
                for ann in a:
                    sink.emit_annotation("time_vs_magnitude", ann)

            if self.verbose:
                print(
                    "file={path} det={dets} ann={anns} start={start} end={end} playback={playback}".format(
                        path=p,
                        dets=len(d),
                        anns=len(a),
                        start=extras.get("started_at") or "-",
                        end=extras.get("finished_at") or "-",
                        playback=extras.get("playback_time") or "-",
                    )
                )

        pre_det = len(dets_all)
        pre_ann = len(ann_all)

        dets_all = deduplicate_detections(dets_all)
        ann_all = deduplicate_annotations(ann_all)
        dup_det_removed = pre_det - len(dets_all)
        dup_ann_removed = pre_ann - len(ann_all)
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

        if sink:
            sink.finalize(meta)
            doc = None
        else:
            doc = FinalDoc(meta=meta, annotations=annotations, detections=dets_all)

        if self.verbose:
            print("---- Summary ----")
            print(
                "Detections: total={total} unique={uniq} removed={removed}".format(
                    total=pre_det,
                    uniq=len(dets_all),
                    removed=dup_det_removed,
                )
            )
            print(
                "Annotations: total={total} unique={uniq} removed={removed}".format(
                    total=pre_ann,
                    uniq=len(ann_all),
                    removed=dup_ann_removed,
                )
            )
            print(
                "Window: start={start} end={end}".format(
                    start=meta.started_at or "-",
                    end=meta.finished_at or "-",
                )
            )
        return doc
