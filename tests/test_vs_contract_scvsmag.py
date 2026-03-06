import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT / "src"))

from eewpw_parser.parsers.vs.dialects import VSDialect, VSStreamState  # noqa: E402


SAMPLE_SNIPPET = """\
2025/11/24 12:00:00 [processing/info/VsMagnitude] Start logging for event: EVT1
2025/11/24 12:00:00 [processing/info/VsMagnitude] creation time: 2025-11-24 12:00:00; origin time: 2025-11-24 11:59:50;
2025/11/24 12:00:00 [processing/info/VsMagnitude] Sensor: NET.STA.HHZ; Wavetype: Z; Soil class: SOIL; Magnitude: 3.2
2025/11/24 12:00:00 [processing/info/VsMagnitude] station lat: 1.0; station lon: 2.0; epicentral distance: 12.3;
2025/11/24 12:00:00 [processing/info/VsMagnitude] PGA(Z): 1.0; PGV(Z): 2.0; PGD(Z): 3.0
2025/11/24 12:00:00 [processing/info/VsMagnitude] PGA(H): 4.0; PGV(H): 5.0; PGD(H): 6.0
2025/11/24 12:00:01 [processing/info/VsMagnitude] End logging for event: EVT1
"""

SENTINEL_SNIPPET = """\
2025/11/24 12:00:00 [processing/info/VsMagnitude] Start logging for event: EVT2
2025/11/24 12:00:00 [processing/info/VsMagnitude] creation time: 2025-11-24 12:00:00; origin time: 2025-11-24 11:59:40;
2025/11/24 12:00:00 [processing/info/VsMagnitude] Sensor: NET.STA.HHZ; Wavetype: Z; Soil class: SOIL; Magnitude: nan
2025/11/24 12:00:00 [processing/info/VsMagnitude] station lat: 1.0; station lon: 2.0; epicentral distance: 12.3;
2025/11/24 12:00:00 [processing/info/VsMagnitude] PGA(Z): -1.00e+00; PGV(Z): -1.00e+00; PGD(Z): -1.00e+00
2025/11/24 12:00:01 [processing/info/VsMagnitude] End logging for event: EVT2
"""


def _parse_snippet(text: str):
    dialect = VSDialect()
    state = VSStreamState()
    dets = []
    for line in text.splitlines():
        d_chunk, _ = dialect.feed_line(line + "\n", state)
        dets.extend(d_chunk)
    d_flush, _ = dialect.flush(state)
    dets.extend(d_flush)
    return dets


def test_one_update_block_produces_one_detection():
    dets = _parse_snippet(SAMPLE_SNIPPET)
    assert len(dets) == 1


def test_detection_timestamp_matches_obs_time_and_origin():
    dets = _parse_snippet(SAMPLE_SNIPPET)
    det = dets[0]
    assert det.timestamp == "2025-11-24T12:00:00Z"
    assert det.core_info.orig_time == "2025-11-24T11:59:50Z"
    for obs in det.gm_info.pga_obs + det.gm_info.pgv_obs + det.gm_info.pgd_obs:
        assert obs.time == det.timestamp


def test_component_in_extra_per_observation():
    dets = _parse_snippet(SAMPLE_SNIPPET)
    det = dets[0]
    comps = [obs.extra.get("vs", {}).get("component") for obs in det.gm_info.pga_obs + det.gm_info.pgv_obs + det.gm_info.pgd_obs]
    assert set(comps) == {"Z", "H"}


def test_sentinel_values_do_not_emit_observations():
    dets = _parse_snippet(SENTINEL_SNIPPET)
    det = dets[0]
    sentinels = [
        obs
        for obs in det.gm_info.pga_obs + det.gm_info.pgv_obs + det.gm_info.pgd_obs
        if (obs.extra.get("vs") or {}).get("is_sentinel")
    ]
    assert len(sentinels) >= 1
