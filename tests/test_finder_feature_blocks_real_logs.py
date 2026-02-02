# -*- coding: utf-8 -*-
import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT / "src"))

from eewpw_parser.parsers.finder.finder_parser import FinderParser
from eewpw_parser.schemas import (
    FaultVertex,
    FinderAzimuthLLK,
    FinderAzimuthValue,
    FinderLengthLLK,
    FinderLengthValue,
)


LOG_CASES = [
    ("native_finder", ROOT / "example-log-files" / "finder_native" / "log_20100904_M7.1_Darfield"),
    ("native_finder", ROOT / "example-log-files" / "finder_native" / "log_20080729_M5.4_ChinoHills"),
    ("native_finder_legacy", ROOT / "example-log-files" / "finder_legacy" / "log_20190706_ridgecrest6"),
    ("native_finder_legacy", ROOT / "example-log-files" / "finder_legacy" / "log_20140824_southnapa"),
]


def _parse_log(dialect: str, log_path: Path):
    if not log_path.exists():
        pytest.skip(f"missing local log file: {log_path}")
    parser = FinderParser({"dialect": dialect, "verbose": False})
    doc = parser.parse([str(log_path)])
    assert doc is not None
    return doc


def _assert_fault_vertex(vertex: FaultVertex) -> None:
    assert isinstance(vertex, FaultVertex)
    assert isinstance(vertex.lat, str)
    assert isinstance(vertex.lon, str)
    if vertex.depth is not None:
        assert isinstance(vertex.depth, str)


def _assert_azimuth_value(value: FinderAzimuthValue) -> None:
    assert isinstance(value, FinderAzimuthValue)
    assert isinstance(value.azimuth, str)
    assert isinstance(value.value, str)


def _assert_length_value(value: FinderLengthValue) -> None:
    assert isinstance(value, FinderLengthValue)
    assert isinstance(value.length, str)
    assert isinstance(value.value, str)


def _assert_azimuth_llk(value: FinderAzimuthLLK) -> None:
    assert isinstance(value, FinderAzimuthLLK)
    assert isinstance(value.azimuth, str)
    assert isinstance(value.llk, str)


def _assert_length_llk(value: FinderLengthLLK) -> None:
    assert isinstance(value, FinderLengthLLK)
    assert isinstance(value.length, str)
    assert isinstance(value.llk, str)


def _assert_list_block(detections, attr: str, item_assert) -> None:
    lists = [
        getattr(d.finder_details, attr)
        for d in detections
        if d.finder_details and getattr(d.finder_details, attr) is not None
    ]
    assert lists, f"no {attr} blocks parsed"
    for items in lists:
        assert isinstance(items, list)
        for item in items:
            item_assert(item)


def _assert_pdf_blocks(detections) -> None:
    pdf_maps = [
        d.finder_details.extra.get("pdf")
        for d in detections
        if d.finder_details and d.finder_details.extra.get("pdf")
    ]
    if not pdf_maps:
        return
    for pdf_map in pdf_maps:
        assert isinstance(pdf_map, dict)
        for key, rows in pdf_map.items():
            assert isinstance(key, str)
            assert isinstance(rows, list)
            assert rows
            for row in rows:
                assert isinstance(row, dict)
                for field in ("lat", "lon", "value"):
                    assert field in row
                    assert isinstance(row[field], str)


@pytest.mark.parametrize("dialect,log_path", LOG_CASES)
def test_finder_feature_blocks_real_logs(dialect: str, log_path: Path) -> None:
    doc = _parse_log(dialect, log_path)
    detections = [d for d in doc.detections if d.finder_details]
    assert detections

    template_ids = [
        d.finder_details.template_id
        for d in detections
        if d.finder_details.template_id is not None
    ]
    assert template_ids
    assert any(template_id != "" for template_id in template_ids)

    centroids = [
        d.finder_details.centroid
        for d in detections
        if d.finder_details.centroid is not None
    ]
    assert centroids
    for centroid in centroids:
        _assert_fault_vertex(centroid)

    _assert_list_block(detections, "rupture_list", _assert_fault_vertex)
    _assert_list_block(detections, "azimuth_list", _assert_azimuth_value)
    _assert_list_block(detections, "length_list", _assert_length_value)
    _assert_list_block(detections, "azimuth_llk_list", _assert_azimuth_llk)
    _assert_list_block(detections, "length_llk_list", _assert_length_llk)

    _assert_pdf_blocks(detections)
