# -*- coding: utf-8 -*-
import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT / "src"))

from eewpw_parser.parsers.finder.finder_parser import FinderParser
from eewpw_parser.schemas import FinalDoc


SMOKE_CASES = [
    ("scfinder", ROOT / "example-log-files" / "finder_scfinder" / "scfinder_Elm2020" / "scfinder.log", "solution"),
    ("scfinder", ROOT / "example-log-files" / "finder_scfinder" / "scfinder_pb.log", "solution"),
    ("native_finder", ROOT / "example-log-files" / "finder_native" / "log_20100904_M7.1_Darfield", "template_id"),
    ("native_finder_legacy", ROOT / "example-log-files" / "finder_legacy" / "log_20140824_southnapa", "template_id"),
    ("shakealert", ROOT / "example-log-files" / "finder_shakealert" / "finder_20251106_shakealert.log", "event_id"),
]


def _parse_log(dialect: str, log_path: Path) -> FinalDoc:
    if not log_path.exists():
        pytest.skip(f"missing local log file: {log_path}")
    parser = FinderParser({"dialect": dialect, "verbose": False})
    doc = parser.parse([str(log_path)])
    assert doc is not None
    return doc


@pytest.mark.finder_regression_smoke
@pytest.mark.parametrize("dialect,log_path,invariant", SMOKE_CASES)
def test_finder_regression_smoke(dialect: str, log_path: Path, invariant: str) -> None:
    doc = _parse_log(dialect, log_path)
    assert isinstance(doc, FinalDoc)
    assert len(doc.detections) > 0

    if invariant == "solution":
        assert any(d.finder_details and d.finder_details.solution for d in doc.detections)
    elif invariant == "template_id":
        assert any(d.finder_details and d.finder_details.template_id for d in doc.detections)
    elif invariant == "event_id":
        assert any(d.finder_details and d.event_id for d in doc.detections)
        for det in doc.detections:
            core = det.core_info
            assert isinstance(core.mag, str)
            assert isinstance(core.lat, str)
            assert isinstance(core.lon, str)
            assert isinstance(core.depth, str)
            assert core.likelihood is None or isinstance(core.likelihood, str)
            if det.finder_details and det.finder_details.solution_metrics:
                metrics = det.finder_details.solution_metrics
                for key in ("mag", "epicenter_lat", "epicenter_lon", "depth", "likelihood"):
                    if key in metrics:
                        assert isinstance(metrics[key], str)
        for annotations in doc.annotations.values():
            for ann in annotations:
                assert isinstance(ann.line, str)
    else:
        raise AssertionError(f"unknown invariant: {invariant}")
