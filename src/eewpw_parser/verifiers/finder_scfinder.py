# -*- coding: utf-8 -*-
from typing import Any, Dict, List
from pathlib import Path

from eewpw_parser.schemas import FinalDoc
from eewpw_parser.parsers.finder.dialects import FinderBaseDialect
from eewpw_parser.utils import to_iso_utc_z


def extract_finder_oracle_from_log(path: str) -> List[Dict[str, Any]]:
    """
    Derive an oracle from a raw scfinder log without using the parser.
    Captures per-detection essentials: event_id, timestamp, version, rupture points,
    station rows included, and presence of num_stations.
    """
    lines = Path(path).read_text(encoding="utf-8", errors="ignore").splitlines()
    header_re = FinderBaseDialect.P_STATION_HEADER
    row_re = FinderBaseDialect.P_STATION_ROW
    prefix_re = FinderBaseDialect.P_PREFIX_TS
    event_re = FinderBaseDialect.P_EVENT_ID
    rupture_re = FinderBaseDialect.P_GET_RUP
    rupture_cont_re = FinderBaseDialect.P_RUP_LINE
    rupture_pt_re = FinderBaseDialect.P_RUP_PT
    num_stations_re = FinderBaseDialect.P_GET_NUM_STATIONS

    oracle: List[Dict[str, Any]] = []
    pending_rows: List[Any] | None = None
    i = 0

    while i < len(lines):
        line = lines[i]

        if header_re.search(line):
            pending_rows = []
            i += 1
            continue

        matches = row_re.findall(line)
        if pending_rows is not None and matches:
            for m in matches:
                if int(m[5]) == 1:
                    pending_rows.append(m)

        m_event = event_re.search(line)
        if not m_event:
            i += 1
            continue

        event_id = m_event.group(1)
        mprefix = prefix_re.search(line)
        ts_iso = to_iso_utc_z(mprefix.group(1)) if mprefix else None
        has_num_stations = False
        rupture_points = 0

        j = i + 1
        while j < len(lines):
            s = lines[j]
            if header_re.search(s) or event_re.search(s):
                break

            if not ts_iso:
                mp = prefix_re.search(s)
                if mp:
                    ts_iso = to_iso_utc_z(mp.group(1))

            if num_stations_re.search(s):
                has_num_stations = True

            mrup = rupture_re.search(s)
            if mrup:
                rupture_points += len(rupture_pt_re.findall(mrup.group(1)))
                k = j + 1
                while k < len(lines):
                    mc = rupture_cont_re.match(lines[k])
                    if not mc:
                        break
                    rupture_points += 1
                    k += 1
                j = k
                continue

            j += 1

        oracle.append(
            {
                "event_id": event_id,
                "timestamp": ts_iso,
                "station_rows": len(pending_rows or []),
                "rupture_points": rupture_points,
                "has_num_stations": has_num_stations,
            }
        )

        pending_rows = None
        i = j

    return oracle


def summarize_finder_from_doc(doc: FinalDoc) -> List[Dict[str, Any]]:
    """
    Reduce FinalDoc detections to a comparable summary for verifier checks.
    """
    summary: List[Dict[str, Any]] = []
    for det in doc.detections:
        fd = det.finder_details
        summary.append(
            {
                "event_id": det.event_id,
                "timestamp": det.timestamp,
                "station_rows": len(det.gm_info.pga_obs),
                "rupture_points": len(det.fault_info),
                "has_num_stations": bool(fd and fd.solution_metrics.get("num_stations")),
            }
        )
    return summary


def verify_finder_scfinder(doc: FinalDoc, oracle: List[Dict[str, Any]]) -> None:
    observed = summarize_finder_from_doc(doc)

    if len(observed) != len(oracle):
        raise AssertionError(f"detection count mismatch: expected {len(oracle)} got {len(observed)}")

    for idx, (o, obs) in enumerate(zip(oracle, observed)):
        eid = o["event_id"]
        if obs["event_id"] != eid:
            raise AssertionError(f"[{idx}] event_id mismatch: expected {eid} got {obs['event_id']}")
        if o["timestamp"] and obs["timestamp"] != o["timestamp"]:
            raise AssertionError(f"[{eid}] timestamp mismatch: expected {o['timestamp']} got {obs['timestamp']}")
        if obs["rupture_points"] != o["rupture_points"]:
            raise AssertionError(f"[{eid}] rupture_points mismatch: expected {o['rupture_points']} got {obs['rupture_points']}")
        if obs["station_rows"] != o["station_rows"]:
            raise AssertionError(f"[{eid}] station_rows mismatch: expected {o['station_rows']} got {obs['station_rows']}")
        if obs["has_num_stations"] != o["has_num_stations"]:
            raise AssertionError(f"[{eid}] num_stations flag mismatch: expected {o['has_num_stations']} got {obs['has_num_stations']}")
