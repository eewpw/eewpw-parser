# -*- coding: utf-8 -*-
import re
from copy import deepcopy
from typing import Dict, Any, Tuple, List, Set

from eewpw_parser.schemas import FinalDoc
from eewpw_parser.utils import to_iso_utc_z


P_PREFIX = re.compile(r"^(\d{4}/\d{2}/\d{2} \d{2}:\d{2}:\d{2})\s+\[processing/info/VsMagnitude\]\s*(.*)$")
P_START = re.compile(r"Start logging for event:\s*(\S+)")
P_END = re.compile(r"End logging for event:\s*(\S+)")
P_UPDATE = re.compile(r"update number:\s*([-\d]+)")
P_SENSOR = re.compile(
    r"Sensor:\s*([A-Za-z0-9.]+);\s*Wavetype:\s*([^;]+);\s*Soil class:\s*([^;]+);\s*Magnitude:\s*([^\s;]+)"
)
P_LOC = re.compile(r"station lat:\s*([-\d.]+);\s*station lon:\s*([-\d.]+);\s*epicentral distance:\s*([-\d.]+);")
P_Z = re.compile(r"PGA\(Z\):\s*([-\d.eE+]+);\s*PGV\(Z\):\s*([-\d.eE+]+);\s*PGD\(Z\):\s*([-\d.eE+]+)")
P_H = re.compile(r"PGA\(H\):\s*([-\d.eE+]+);\s*PGV\(H\):\s*([-\d.eE+]+);\s*PGD\(H\):\s*([-\d.eE+]+)")
P_VS_TIMES = re.compile(r"creation time:\s*([^;]+);\s*origin time:\s*([^;]+);")
P_UNUSED = re.compile(r"Stations not used for VS-mag:\s*(?P<list>.+)$")


def _to_float_or_none(val: str):
    raw = val.strip()
    if not raw:
        return None
    if raw.lower() == "nan":
        return None
    try:
        f = float(raw)
    except ValueError:
        return None
    if f == -1.0:
        return None
    return f


def extract_vs_oracle_from_log(path: str) -> Dict[Tuple[str, int], Dict[str, Any]]:
    oracle: Dict[Tuple[str, int], Dict[str, Any]] = {}
    current: Dict[str, Any] = {}
    last_sncl: str = ""

    def finalize_current():
        if not current:
            return
        key = (current.get("event_id", "0"), int(current.get("version") or 0))
        oracle[key] = {
            "creation_time": current.get("creation_time"),
            "origin_time": current.get("origin_time"),
            "stations_not_used": list(current.get("stations_not_used", [])),
            "stations": deepcopy(current.get("stations", {})),
        }

    with open(path, "r", encoding="utf-8", errors="ignore") as fh:
        for line in fh:
            m_prefix = P_PREFIX.match(line)
            if not m_prefix:
                continue
            message = m_prefix.group(2)

            if (m := P_START.search(message)):
                finalize_current()
                current = {
                    "event_id": m.group(1),
                    "version": None,
                    "creation_time": None,
                    "origin_time": None,
                    "stations_not_used": [],
                    "stations": {},
                }
                last_sncl = ""
                continue

            if (m := P_END.search(message)):
                finalize_current()
                current = {}
                last_sncl = ""
                continue

            if not current:
                continue

            if (m := P_UPDATE.search(message)):
                try:
                    current["version"] = int(m.group(1))
                except ValueError:
                    current["version"] = None

            if (m := P_VS_TIMES.search(message)):
                current["creation_time"] = to_iso_utc_z(m.group(1).replace(" ", "T"))
                current["origin_time"] = to_iso_utc_z(m.group(2).replace(" ", "T"))

            if (m := P_UNUSED.search(message)):
                raw = m.group("list")
                items = [tok for tok in raw.strip().split() if tok]
                current.setdefault("stations_not_used", []).extend(items)

            if (m := P_SENSOR.search(message)):
                sncl = m.group(1).strip()
                current["stations"].setdefault(sncl, {"measures": {"PGA": {"Z": None, "H": None}, "PGV": {"Z": None, "H": None}, "PGD": {"Z": None, "H": None}}})
                last_sncl = sncl

            if (m := P_LOC.search(message)):
                pass

            if (m := P_Z.search(message)) and current.get("stations") and last_sncl:
                measures = current["stations"][last_sncl]["measures"]
                measures["PGA"]["Z"] = _to_float_or_none(m.group(1))
                measures["PGV"]["Z"] = _to_float_or_none(m.group(2))
                measures["PGD"]["Z"] = _to_float_or_none(m.group(3))

            if (m := P_H.search(message)) and current.get("stations") and last_sncl:
                measures = current["stations"][last_sncl]["measures"]
                measures["PGA"]["H"] = _to_float_or_none(m.group(1))
                measures["PGV"]["H"] = _to_float_or_none(m.group(2))
                measures["PGD"]["H"] = _to_float_or_none(m.group(3))

    finalize_current()
    return oracle


def summarize_vs_from_doc(doc: FinalDoc) -> Dict[Tuple[str, int], Dict[str, Any]]:
    summary: Dict[Tuple[str, int], Dict[str, Any]] = {}

    for det in doc.detections:
        key = (det.event_id, int(det.version))
        obs_set: Set[Tuple[str, str, str, str, float]] = set()
        station_measures: Dict[str, Dict[str, Dict[str, float]]] = {}

        def add_obs(measure: str, obs_list):
            for obs in obs_list:
                comp = (obs.extra.get("vs") or {}).get("component")
                value_float = float(obs.value)
                obs_set.add((measure, comp, obs.SNCL, obs.time, value_float))
                station_measures.setdefault(obs.SNCL, {}).setdefault(measure, {})[comp] = value_float
                if obs.time != det.timestamp:
                    raise AssertionError(f"[{det.event_id} v{det.version}] GMObs time mismatch: expected {det.timestamp} got {obs.time}")

        add_obs("PGA", det.gm_info.pga_obs)
        add_obs("PGV", det.gm_info.pgv_obs)
        add_obs("PGD", det.gm_info.pgd_obs)

        summary[key] = {
            "creation_time": det.timestamp,
            "origin_time": det.core_info.orig_time,
            "stations_not_used": list(det.vs_details.stations_not_used) if det.vs_details else [],
            "obs": obs_set,
            "station_measures": station_measures,
        }

    return summary


def verify_vs_scvsmag(doc: FinalDoc, oracle: Dict[Tuple[str, int], Dict[str, Any]]) -> None:
    observed = summarize_vs_from_doc(doc)

    for key in sorted(oracle.keys()):
        event_id, version = key
        if key not in observed:
            raise AssertionError(f"[{event_id} v{version}] detection missing")

        o = oracle[key]
        obs = observed[key]

        if o["creation_time"] != obs["creation_time"]:
            raise AssertionError(f"[{event_id} v{version}] timestamp mismatch: expected {o['creation_time']} got {obs['creation_time']}")
        if o["origin_time"] != obs["origin_time"]:
            raise AssertionError(f"[{event_id} v{version}] origin_time mismatch: expected {o['origin_time']} got {obs['origin_time']}")

        if set(o.get("stations_not_used", [])) != set(obs.get("stations_not_used", [])):
            raise AssertionError(
                f"[{event_id} v{version}] stations_not_used mismatch: expected {set(o.get('stations_not_used', []))} got {set(obs.get('stations_not_used', []))}"
            )

        expected_obs_keys: Set[Tuple[str, str, str]] = set()

        stations = o.get("stations", {}) or {}
        for sncl, data in stations.items():
            measures = (data or {}).get("measures") or {}
            for measure, comps in measures.items():
                for comp, val in comps.items():
                    expected_obs_keys.add((measure, comp, sncl))
                    if val is None:
                        matches = [ob for ob in obs["obs"] if ob[0] == measure and ob[1] == comp and ob[2] == sncl]
                        if matches:
                            raise AssertionError(
                                f"[{event_id} v{version}] unexpected observation station={sncl} measure={measure} comp={comp} (sentinel in log)"
                            )
                    else:
                        matches = [ob for ob in obs["obs"] if ob[0] == measure and ob[1] == comp and ob[2] == sncl]
                        if not matches:
                            raise AssertionError(
                                f"[{event_id} v{version}] missing observation station={sncl} measure={measure} comp={comp}"
                            )
                        time_set = {m[3] for m in matches}
                        if time_set != {obs["creation_time"]}:
                            raise AssertionError(
                                f"[{event_id} v{version}] observation time mismatch station={sncl} measure={measure} comp={comp}: expected {obs['creation_time']} got {time_set}"
                            )
                        found_val = matches[0][4]
                        if abs(found_val - val) > 1e-9:
                            raise AssertionError(
                                f"[{event_id} v{version}] value mismatch station={sncl} measure={measure} comp={comp}: expected {val} got {found_val}"
                            )

        observed_keys = {(ob[0], ob[1], ob[2]) for ob in obs["obs"]}
        extra = observed_keys - {k for k in expected_obs_keys if stations.get(k[2]) and stations[k[2]]["measures"][k[0]][k[1]] is not None}
        if extra:
            example = next(iter(extra))
            raise AssertionError(
                f"[{event_id} v{version}] unexpected observation station={example[2]} measure={example[0]} comp={example[1]}"
            )
