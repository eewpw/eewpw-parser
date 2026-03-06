# -*- coding: utf-8 -*-
import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT / "src"))

from eewpw_parser.parsers.finder.finder_parser import FinderParser
from eewpw_parser.schemas import FinalDoc


def test_finder_legacy_emission_gating_southnapa() -> None:
    log_path = ROOT / "example-log-files" / "finder_legacy" / "log_20140824_southnapa"
    if not log_path.exists():
        pytest.skip(f"missing local log file: {log_path}")

    text = log_path.read_text(encoding="utf-8", errors="ignore")
    event_id_count = text.count("-> event_id")

    parser = FinderParser({"dialect": "finder_legacy", "verbose": False})
    doc = parser.parse([str(log_path)])
    assert doc is not None
    assert isinstance(doc, FinalDoc)

    dets = doc.detections
    assert len(dets) == event_id_count
    assert len(dets) >= 1

    for det in dets:
        assert det.finder_details is not None
        solution = det.finder_details.solution
        assert solution
        assert "Version" in solution
        assert isinstance(solution["Version"], str)
        assert solution["Version"] != ""
        for value in solution.values():
            assert isinstance(value, str)

    last_det = dets[-1]
    assert last_det.finder_details is not None
    last_solution = last_det.finder_details.solution
    assert last_solution
    assert "Version" in last_solution
    assert isinstance(last_solution["Version"], str)
    assert last_solution["Version"] != ""
