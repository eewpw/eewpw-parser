import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT / "src"))

from eewpw_parser.schemas import FinalDoc, SCHEMA_VERSION  # noqa: E402
from eewpw_parser.parsers.vs.dialects import VSEventState  # noqa: E402


def _base_detection(core_overrides=None, **overrides):
    core = {
        "id": "e1",
        "mag": "5.0",
        "lat": "0.0",
        "lon": "0.0",
        "depth": "10.0",
        "orig_time": "2020-01-01T00:00:00Z",
    }
    core.update(core_overrides or {})
    det = {
        "timestamp": "2020-01-01T00:00:01Z",
        "event_id": "e1",
        "category": "live",
        "instance": "inst",
        "orig_sys": "testsys",
        "version": "1",
        "core_info": core,
        "fault_info": [],
        "gm_info": {"pgv_obs": [], "pga_obs": []},
    }
    det.update(overrides)
    return det


def _base_doc(detection):
    return {
        "meta": {"algo": "a", "dialect": "d", "extras": {}, "stats_total": {}},
        "annotations": {},
        "detections": [detection],
    }


def test_gmcontour_pred_loads_and_preserves_shape():
    contour = {"MMI": "3", "polygon": [[1.0, 2.0], [3.0, 4.0]]}
    det = _base_detection(
        gm_info={"pgv_obs": [], "pga_obs": [], "gmcontour_pred": [contour]},
    )
    doc = FinalDoc.model_validate(_base_doc(det))

    parsed = doc.detections[0].gm_info.gmcontour_pred
    assert len(parsed) == 1
    assert parsed[0].model_dump() == contour


def test_fault_info_empty_dict_coerces_to_list():
    det = _base_detection(fault_info={})
    doc = FinalDoc.model_validate(_base_doc(det))
    assert doc.detections[0].fault_info == []


def test_gmobs_orig_sys_falls_back_to_detection_origin():
    det = _base_detection(
        gm_info={"pgv_obs": [], "pga_obs": [{"SNCL": "STA", "value": "1.0", "lat": "0", "lon": "0", "time": "2020-01-01T00:00:00Z"}]},
    )
    doc = FinalDoc.model_validate(_base_doc(det))
    obs = doc.detections[0].gm_info.pga_obs[0]
    assert obs.orig_sys == "testsys"


def test_meta_schema_version_defaults_when_missing():
    det = _base_detection()
    payload = _base_doc(det)
    # meta.schema_version intentionally omitted
    doc = FinalDoc.model_validate(payload)
    assert doc.meta.schema_version == SCHEMA_VERSION


def test_vs_station_metadata_namespaced_under_vs_key():
    state = VSEventState(
        event_id="ev1",
        vs_mag=4.2,
        lat=1.0,
        lon=2.0,
        depth=5.0,
        creation_time="2025-01-01T00:00:00Z",
        origin_time="2025-01-01T00:00:01Z",
        last_ts_iso="2025-01-01T00:00:02Z",
    )
    state.stations = [
        {
            "sncl": "NET.STA.HHZ.--",
            "component": "HHZ",
            "wavetype": "Z",
            "soil": "SOIL",
            "magnitude": 3.2,
            "lat": 1.1,
            "lon": 2.2,
            "pga_h": 0.5,
            "pga_z": None,
            "epi_dist_km": 12.3,
        }
    ]

    det = state.to_detection({})
    obs = det.gm_info.pga_obs[0]
    assert obs.extra == {
        "vs": {
            "component": "H",
            "station_magnitude": "3.2",
            "wavetype": "Z",
            "soil_class": "SOIL",
            "epi_dist_km": "12.3",
        }
    }
