from __future__ import annotations

import datetime as dt
import math
import re
from urllib.parse import parse_qs, urlparse


def utc_now_iso() -> str:
    return dt.datetime.now(dt.timezone.utc).replace(microsecond=0).isoformat()


def normalize_upload_date(
    upload_date: str | None = None,
    release_date: str | None = None,
    timestamp: int | float | None = None,
) -> str:
    raw = upload_date or release_date
    if raw:
        raw = raw.strip()
        if len(raw) == 8 and raw.isdigit():
            return f"{raw[0:4]}-{raw[4:6]}-{raw[6:8]}"
        if len(raw) >= 10:
            return raw[0:10]
    if timestamp:
        return dt.datetime.fromtimestamp(timestamp, tz=dt.timezone.utc).date().isoformat()
    return "1970-01-01"


def format_duration(seconds: int) -> str:
    if seconds <= 0:
        return "0:00"
    h, rem = divmod(seconds, 3600)
    m, s = divmod(rem, 60)
    if h > 0:
        return f"{h}:{m:02d}:{s:02d}"
    return f"{m}:{s:02d}"


def parse_duration_to_seconds(raw: str) -> int:
    raw = raw.strip()
    if not raw:
        raise ValueError("duration value is empty")
    if raw.isdigit():
        return int(raw)
    parts = raw.split(":")
    if len(parts) == 2:
        minutes, seconds = parts
        return int(minutes) * 60 + int(seconds)
    if len(parts) == 3:
        hours, minutes, seconds = parts
        return int(hours) * 3600 + int(minutes) * 60 + int(seconds)
    raise ValueError(f"unsupported duration value: {raw}")


def truncate(text: str, size: int) -> str:
    if len(text) <= size:
        return text
    return text[: max(size - 1, 0)] + "…"


def extract_video_id(value: str) -> str | None:
    value = value.strip()
    if not value:
        return None
    if re.fullmatch(r"[A-Za-z0-9_-]{11}", value):
        return value
    parsed = urlparse(value)
    if not parsed.netloc:
        return None
    if parsed.netloc.endswith("youtu.be"):
        candidate = parsed.path.strip("/")
        if re.fullmatch(r"[A-Za-z0-9_-]{11}", candidate):
            return candidate
    query = parse_qs(parsed.query)
    candidate = (query.get("v") or [None])[0]
    if candidate and re.fullmatch(r"[A-Za-z0-9_-]{11}", candidate):
        return candidate
    path_parts = [part for part in parsed.path.split("/") if part]
    if "shorts" in path_parts:
        idx = path_parts.index("shorts")
        if idx + 1 < len(path_parts):
            candidate = path_parts[idx + 1]
            if re.fullmatch(r"[A-Za-z0-9_-]{11}", candidate):
                return candidate
    return None


def with_timestamp(url: str, start_seconds: float | int | None) -> str:
    if start_seconds is None:
        return url
    seconds = max(0, int(math.floor(start_seconds)))
    joiner = "&" if "?" in url else "?"
    return f"{url}{joiner}t={seconds}s"


def safe_int(value: object, default: int = 0) -> int:
    try:
        if value is None:
            return default
        return int(value)
    except (ValueError, TypeError):
        return default
