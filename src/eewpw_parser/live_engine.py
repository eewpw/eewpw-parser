# -*- coding: utf-8 -*-
from __future__ import annotations
from pathlib import Path
from typing import Dict, Optional, Tuple, List

from .schemas import Detection, Annotation
from .utils import to_iso_utc_z
from .live_writer import LiveWriter

# Finder streaming
from .parsers.finder.finder_parser import FinderParser
from .parsers.finder.dialects import FinderStreamState
# VS streaming directly through dialect
from .parsers.vs.dialects import VSDialect, VSStreamState


def _sanitize_ts_for_filename(ts_iso: str) -> str:
    # Expect ISO like YYYY-MM-DDTHH:MM:SSZ (possibly with ms). Strip non-filename chars.
    # Keep Z if present; drop ms.
    ts = ts_iso
    if "." in ts:
        ts = ts.split(".", 1)[0] + ("Z" if ts.endswith("Z") else "")
    return (
        ts.replace(":", "")
        .replace("-", "")
        .replace("/", "")
        .replace(" ", "T")
    )


class LiveEngine:
    def __init__(
        self,
        source,
        parser,
        output_dir: Path,
        algo: str,
        dialect: str,
        instance: str,
        verbose: bool = False,
    ):
        self.source = source
        self.parser = parser
        self.output_dir = Path(output_dir)
        self.algo = algo
        self.dialect = dialect
        self.instance = instance
        self.verbose = verbose

        self.output_dir.mkdir(parents=True, exist_ok=True)

        # event_id -> (writer, first_ts_iso)
        self._writers: Dict[str, Tuple[LiveWriter, str]] = {}

        # parser state
        self._finder_state: Optional[FinderStreamState] = None
        self._vs_state: Optional[VSStreamState] = None

        # annotation profile key per algo
        self._ann_profile = {
            "finder": "time_vs_magnitude",
            "vs": "processing_info",
        }.get(self.algo, "annotations")

    def _get_writer(self, event_id: str, first_ts_iso: str) -> LiveWriter:
        if event_id in self._writers:
            return self._writers[event_id][0]
        ts_iso = to_iso_utc_z(first_ts_iso)
        ts_token = _sanitize_ts_for_filename(ts_iso)
        filename = f"{ts_token}_{self.algo}_{self.instance}.jsonl"
        path = self.output_dir / filename
        writer = LiveWriter(path, algo=self.algo, dialect=self.dialect, instance=self.instance, verbose=self.verbose)
        self._writers[event_id] = (writer, ts_iso)
        if self.verbose:
            print(f"[engine] New writer for event {event_id}: {path}")
        return writer

    def _emit(self, dets: List[Detection], anns: List[Annotation]) -> None:
        for det in dets:
            eid = str(det.event_id)
            writer = self._get_writer(eid, det.timestamp)
            writer.write_detection(det)
        for ann in anns:
            # Annotations are not event-scoped in dialects; write to all open writers or skip?
            # Strategy: write annotations to the most recent writer (last started).
            # This keeps real-time hints attached somewhere useful without buffering.
            if not self._writers:
                continue
            last_eid = next(reversed(self._writers.keys()))
            self._writers[last_eid][0].write_annotation(self._ann_profile, ann)

    def run_forever(self) -> None:
        try:
            # Initialize parser state according to the provided parser/dialect.
            if isinstance(self.parser, FinderParser):
                self._finder_state = self._finder_state or FinderStreamState()
                for line in self.source:
                    dets, anns, self._finder_state = self.parser.parse_stream([line], state=self._finder_state, finalize=False)
                    self._emit(dets, anns)
            elif isinstance(self.parser, VSDialect):
                self._vs_state = self._vs_state or VSStreamState()
                for line in self.source:
                    d, a = self.parser.feed_line(line, self._vs_state)
                    self._emit(d, a)
            else:
                raise ValueError("Unsupported parser type for live engine")
        finally:
            self.shutdown()

    def shutdown(self) -> None:
        # Flush parser and close all writers.
        try:
            if isinstance(self.parser, FinderParser):
                if self._finder_state is None:
                    self._finder_state = FinderStreamState()
                dets, anns, self._finder_state = self.parser.parse_stream([], state=self._finder_state, finalize=True)
                self._emit(dets, anns)
            elif isinstance(self.parser, VSDialect):
                if self._vs_state is None:
                    self._vs_state = VSStreamState()
                d, a = self.parser.flush(self._vs_state)
                self._emit(d, a)
        finally:
            for eid, (w, _ts) in list(self._writers.items()):
                if self.verbose:
                    print(f"[engine] Closing writer for event {eid}: {w.path}")
                w.close()
            self._writers.clear()
