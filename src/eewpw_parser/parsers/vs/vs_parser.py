# -*- coding: utf-8 -*-
from typing import List, Dict, Any, Optional
from pathlib import Path
from dateutil import parser as dtp

from eewpw_parser.schemas import FinalDoc, Meta
from eewpw_parser.parsers.vs.dialects import VSDialect
from eewpw_parser.dedup import deduplicate_detections, deduplicate_annotations
from eewpw_parser.sinks import BaseSink


class VSParser:
    def __init__(self, cfg: Dict[str, Any]):
        self.cfg = cfg
        self.dialect = cfg.get("dialect", "scvsmag")
        self.verbose = bool(cfg.get("verbose", False))
        self.verbose = bool(cfg.get("verbose", False))

    def parse(self, inputs: List[str], sink: Optional[BaseSink] = None) -> Optional[FinalDoc]:
        files = [str(Path(p)) for p in inputs]
        dets_all = []
        ann_all = []
        per_file_extras: List[Dict[str, Any]] = []

        worker = VSDialect()
        if hasattr(worker, "verbose"):
            worker.verbose = self.verbose

        if self.verbose:
            print("==== VS Parse ====")
            print(f"Dialect: {self.dialect} Files: {len(files)}")

        if sink:
            sink.start_run()

        for p in files:
            d, a, extras = worker.parse_file(p)
            dets_all.extend(d)
            ann_all.extend(a)
            per_file_extras.append(extras)

            if self.verbose:
                print(
                    "file={path} det={dets} ann={anns} start={start} end={end}".format(
                        path=p,
                        dets=len(d),
                        anns=len(a),
                        start=extras.get("started_at") or "-",
                        end=extras.get("finished_at") or "-",
                    )
                )

            if sink:
                for det in d:
                    sink.emit_detection(det)
                for ann in a:
                    sink.emit_annotation("time_vs_magnitude", ann)

        pre_det = len(dets_all)
        pre_ann = len(ann_all)

        dets_all = deduplicate_detections(dets_all)
        ann_all = deduplicate_annotations(ann_all)
        dup_det_removed = pre_det - len(dets_all)
        dup_ann_removed = pre_ann - len(ann_all)
        dets_all.sort(key=lambda x: x.timestamp)

        start_candidates = []
        end_candidates = []
        for fx in per_file_extras:
            if fx.get("started_at"):
                start_candidates.append(dtp.parse(fx["started_at"]))
            if fx.get("finished_at"):
                end_candidates.append(dtp.parse(fx["finished_at"]))

        if start_candidates:
            started_at_iso = min(start_candidates).isoformat().replace("+00:00", "Z")
        elif dets_all:
            started_at_iso = dets_all[0].timestamp
        else:
            started_at_iso = None

        if end_candidates:
            finished_at_iso = max(end_candidates).isoformat().replace("+00:00", "Z")
        elif dets_all:
            finished_at_iso = dets_all[-1].timestamp
        else:
            finished_at_iso = None

        stats_total = {
            "detections": len(dets_all),
            "annotations": len(ann_all),
            "files": len(files),
        }

        extras = {"files": per_file_extras}

        meta = Meta(
            algo="vs",
            dialect=self.dialect,
            started_at=started_at_iso,
            finished_at=finished_at_iso,
            extras=extras,
            stats_total=stats_total,
        )

        if sink:
            sink.finalize(meta)
            doc = None
        else:
            doc = FinalDoc(meta=meta, annotations={"time_vs_magnitude": ann_all}, detections=dets_all)
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
