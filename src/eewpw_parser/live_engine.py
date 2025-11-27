# -*- coding: utf-8 -*-
from __future__ import annotations
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional, List

from dateutil import parser as dtp

from .schemas import Detection, Annotation, Meta
from .live_writer import DailyAlgoWriter

# Finder streaming
from .parsers.finder.finder_parser import FinderParser
from .parsers.finder.dialects import FinderStreamState
# VS streaming directly through dialect
from .parsers.vs.dialects import VSDialect, VSStreamState


class LiveEngine:
    def __init__(
        self,
        source,
        parser,
        data_root: Path,
        algo: str,
        dialect: str,
        instance: str,
        verbose: bool = False,
    ):
        self.source = source
        self.parser = parser
        self.data_root = Path(data_root)
        self.algo = algo
        self.dialect = dialect
        self.instance = instance
        self.verbose = verbose

        self._daily_writer = DailyAlgoWriter(
            data_root=self.data_root,
            algo=self.algo,
            dialect=self.dialect,
            instance=self.instance,
            verbose=self.verbose,
        )

        # parser state
        self._finder_state: Optional[FinderStreamState] = None
        self._vs_state: Optional[VSStreamState] = None
        self._last_event_id: Optional[str] = None
        self._started_dt: Optional[datetime] = None
        self._finished_dt: Optional[datetime] = None
        self._closed = False

        # annotation profile key per algo
        self._ann_profile = {
            "finder": "time_vs_magnitude",
            "vs": "time_vs_magnitude",
        }.get(self.algo, "annotations")

    def _parse_ts(self, ts_iso: str) -> datetime:
        dt = dtp.parse(ts_iso)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        else:
            dt = dt.astimezone(timezone.utc)
        return dt

    def _update_time_bounds(self, ts_iso: str) -> None:
        dt = self._parse_ts(ts_iso)
        if self._started_dt is None or dt < self._started_dt:
            self._started_dt = dt
        if self._finished_dt is None or dt > self._finished_dt:
            self._finished_dt = dt

    def _emit(self, dets: List[Detection], anns: List[Annotation]) -> None:
        for det in dets:
            self._update_time_bounds(det.timestamp)
            self._last_event_id = str(det.event_id)
            self._daily_writer.write_detection(det)
        for ann in anns:
            self._update_time_bounds(ann.timestamp)
            eid = self._last_event_id or ""
            self._daily_writer.write_annotation(self._ann_profile, ann, eid)

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
        # Flush parser and close writer.
        if self._closed:
            return
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
            meta = Meta(
                algo=self.algo,
                dialect=self.dialect,
                files=None,
                started_at=self._started_dt.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%fZ") if self._started_dt else None,
                finished_at=self._finished_dt.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%fZ") if self._finished_dt else None,
                playback_time=None,
                extras={},
                stats_total={},
            )
            self._daily_writer.write_meta(meta)
            self._daily_writer.close()
            self._closed = True
