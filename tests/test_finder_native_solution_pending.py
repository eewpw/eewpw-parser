# -*- coding: utf-8 -*-
import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT / "src"))

from eewpw_parser.parsers.finder.finder_parser import FinderParser


def test_native_finder_pending_solution_darfield_last_detection() -> None:
    log_path = ROOT / "example-log-files" / "finder_native" / "log_20100904_M7.1_Darfield"
    if not log_path.exists():
        pytest.skip(f"missing local log file: {log_path}")

    parser = FinderParser({"dialect": "native_finder", "verbose": False})
    doc = parser.parse([str(log_path)])
    assert doc is not None
    assert len(doc.detections) >= 1

    last_det = doc.detections[-1]
    assert last_det.finder_details is not None
    solution = last_det.finder_details.solution
    assert solution

    required_keys = [
        "Version",
        "Time since object creation",
        "End Lat1",
        "End Lon1",
        "Centroid Lat",
        "Centroid Lon",
        "End Lat2",
        "End Lon2",
    ]
    for key in required_keys:
        assert key in solution
        assert isinstance(solution[key], str)
        assert solution[key] != ""

    for value in solution.values():
        assert isinstance(value, str)
