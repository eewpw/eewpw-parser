import re
from datetime import datetime, timezone
from dateutil import parser as dtp
from dateutil.parser import ParserError

def to_iso_utc_z(s: str) -> str:
    """
    Flexible timestamp normalization to ISO-8601 UTC Z.
    Accepts things like:
      - '2020/10/25 19:34:30'
      - '2024-08-14 06:29:23.003000'
      - '2016-10-30T06:40:20.970000Z'
      - '2016-10-30 06:40:20.97+03:00'
      - '2025-10-21 05:22:03:880'
    """
    # Normalize Finder-style timestamps like '2025-10-21 05:22:03:880'
    # to '2025-10-21 05:22:03.880' so dateutil can parse them.
    m = re.match(r"^(\d{4}[/-]\d{2}[/-]\d{2}[ T]\d{2}:\d{2}:\d{2}):(\d{1,6})$", s)
    if m:
        base, frac = m.groups()
        s = f"{base}.{frac}"

    try:
        dt = dtp.parse(s)
    except ParserError as exc:
        raise ParserError(f"Unsupported timestamp format for to_iso_utc_z: {s}") from exc

    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    else:
        dt = dt.astimezone(timezone.utc)

    # Keep microseconds if present in input
    if "." in s or (hasattr(dt, "microsecond") and dt.microsecond):
        return dt.strftime("%Y-%m-%dT%H:%M:%S.%fZ")
    return dt.strftime("%Y-%m-%dT%H:%M:%SZ")

def epoch_to_iso_z(epoch_str: str) -> str:
    # epoch may come as float string
    dt = datetime.fromtimestamp(float(epoch_str), tz=timezone.utc)
    return dt.strftime("%Y-%m-%dT%H:%M:%S.%fZ")

def trim(s: str) -> str:
    return s.strip()