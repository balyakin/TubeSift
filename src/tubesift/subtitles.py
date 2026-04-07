from __future__ import annotations

import html
import json
import re
from dataclasses import dataclass
from urllib.request import Request, urlopen


@dataclass(slots=True)
class SubtitleSegment:
    language: str
    start_seconds: float
    end_seconds: float
    text: str


@dataclass(slots=True)
class SubtitleTrack:
    language: str
    url: str
    ext: str


def extract_subtitle_tracks(info: dict, mode: str, lang: str | None = None) -> list[SubtitleTrack]:
    mode = mode.lower().strip()
    if mode == "none":
        return []

    tracks: list[SubtitleTrack] = []
    lang_filter = lang.lower() if lang else None

    def add_from_bucket(bucket: dict[str, list[dict]]) -> None:
        for language, entries in (bucket or {}).items():
            normalized_lang = language.lower()
            if lang_filter and not normalized_lang.startswith(lang_filter):
                continue
            best = _pick_best_entry(entries)
            if not best:
                continue
            url = best.get("url")
            if not url:
                continue
            tracks.append(
                SubtitleTrack(
                    language=normalized_lang,
                    url=url,
                    ext=str(best.get("ext") or "").lower(),
                )
            )

    if mode == "auto":
        add_from_bucket(info.get("automatic_captions") or {})
    elif mode == "all":
        add_from_bucket(info.get("subtitles") or {})
        add_from_bucket(info.get("automatic_captions") or {})
    else:
        raise ValueError("unsupported subtitles mode, expected: none|auto|all")

    seen: set[tuple[str, str]] = set()
    deduped: list[SubtitleTrack] = []
    for track in tracks:
        key = (track.language, track.url)
        if key in seen:
            continue
        seen.add(key)
        deduped.append(track)
    return deduped


def fetch_subtitle_segments(
    info: dict,
    mode: str,
    lang: str | None = None,
    timeout: int = 20,
) -> list[SubtitleSegment]:
    tracks = extract_subtitle_tracks(info=info, mode=mode, lang=lang)
    segments: list[SubtitleSegment] = []
    for track in tracks:
        try:
            payload = _fetch_url_text(track.url, timeout=timeout)
            parsed = _parse_subtitle_payload(payload, track.ext, track.language)
            segments.extend(parsed)
        except Exception:
            continue

    normalized: list[SubtitleSegment] = []
    seen: set[tuple[str, int, str]] = set()
    for segment in segments:
        text = _clean_subtitle_text(segment.text)
        if not text:
            continue
        key = (segment.language, int(segment.start_seconds * 1000), text)
        if key in seen:
            continue
        seen.add(key)
        normalized.append(
            SubtitleSegment(
                language=segment.language,
                start_seconds=segment.start_seconds,
                end_seconds=segment.end_seconds,
                text=text,
            )
        )
    return normalized


def _pick_best_entry(entries: list[dict]) -> dict | None:
    if not entries:
        return None
    preferred_ext = ["vtt", "srv3", "json3", "ttml", "srt"]
    for ext in preferred_ext:
        for entry in entries:
            if str(entry.get("ext") or "").lower() == ext:
                return entry
    return entries[0]


def _fetch_url_text(url: str, timeout: int) -> str:
    req = Request(url, headers={"User-Agent": "TubeSift/0.1 (+https://github.com/)"})
    with urlopen(req, timeout=timeout) as response:
        payload = response.read()
    return payload.decode("utf-8", errors="replace")


def _parse_subtitle_payload(payload: str, ext: str, language: str) -> list[SubtitleSegment]:
    normalized_ext = (ext or "").lower()
    if normalized_ext in {"vtt", "webvtt", "srv3", "ttml"}:
        return _parse_vtt(payload, language)
    if normalized_ext in {"srt"}:
        return _parse_srt(payload, language)
    if normalized_ext in {"json3", "json"}:
        return _parse_json3(payload, language)

    payload_trimmed = payload.lstrip()
    if payload_trimmed.startswith("{"):
        return _parse_json3(payload, language)
    if "-->" in payload:
        return _parse_vtt(payload, language)
    return []


def _parse_vtt(payload: str, language: str) -> list[SubtitleSegment]:
    lines = payload.splitlines()
    segments: list[SubtitleSegment] = []
    i = 0
    while i < len(lines):
        line = lines[i].strip()
        if "-->" not in line:
            i += 1
            continue
        start_raw, end_raw = [part.strip().split(" ")[0] for part in line.split("-->", 1)]
        start = _parse_time_to_seconds(start_raw)
        end = _parse_time_to_seconds(end_raw)
        i += 1

        cue_lines: list[str] = []
        while i < len(lines):
            text_line = lines[i].strip()
            if not text_line:
                break
            if text_line.startswith("NOTE"):
                break
            cue_lines.append(text_line)
            i += 1
        text = " ".join(cue_lines).strip()
        if text:
            segments.append(
                SubtitleSegment(
                    language=language,
                    start_seconds=start,
                    end_seconds=end,
                    text=text,
                )
            )
        i += 1
    return segments


def _parse_srt(payload: str, language: str) -> list[SubtitleSegment]:
    chunks = re.split(r"\n\s*\n", payload.strip(), flags=re.MULTILINE)
    segments: list[SubtitleSegment] = []
    for chunk in chunks:
        lines = [line.strip() for line in chunk.splitlines() if line.strip()]
        if len(lines) < 2:
            continue
        timing_line = lines[1] if re.match(r"^\d+$", lines[0]) else lines[0]
        if "-->" not in timing_line:
            continue
        start_raw, end_raw = [part.strip().split(" ")[0] for part in timing_line.split("-->", 1)]
        start = _parse_time_to_seconds(start_raw.replace(",", "."))
        end = _parse_time_to_seconds(end_raw.replace(",", "."))
        text_lines = lines[2:] if re.match(r"^\d+$", lines[0]) else lines[1:]
        text = " ".join(text_lines).strip()
        if not text:
            continue
        segments.append(
            SubtitleSegment(
                language=language,
                start_seconds=start,
                end_seconds=end,
                text=text,
            )
        )
    return segments


def _parse_json3(payload: str, language: str) -> list[SubtitleSegment]:
    try:
        data = json.loads(payload)
    except json.JSONDecodeError:
        return []

    events = data.get("events")
    if not isinstance(events, list):
        return []

    segments: list[SubtitleSegment] = []
    for event in events:
        if not isinstance(event, dict):
            continue
        start_ms = event.get("tStartMs")
        duration_ms = event.get("dDurationMs")
        segs = event.get("segs") or []
        if start_ms is None or duration_ms is None:
            continue
        pieces: list[str] = []
        for seg in segs:
            if isinstance(seg, dict):
                text = seg.get("utf8")
                if text:
                    pieces.append(text)
        text = "".join(pieces).strip()
        if not text:
            continue
        start = float(start_ms) / 1000.0
        end = start + float(duration_ms) / 1000.0
        segments.append(
            SubtitleSegment(
                language=language,
                start_seconds=start,
                end_seconds=end,
                text=text,
            )
        )
    return segments


def _parse_time_to_seconds(raw: str) -> float:
    raw = raw.strip()
    if not raw:
        return 0.0
    parts = raw.split(":")
    if len(parts) == 3:
        hours = float(parts[0])
        minutes = float(parts[1])
        seconds = float(parts[2])
        return hours * 3600 + minutes * 60 + seconds
    if len(parts) == 2:
        minutes = float(parts[0])
        seconds = float(parts[1])
        return minutes * 60 + seconds
    return float(parts[0])


def _clean_subtitle_text(text: str) -> str:
    text = html.unescape(text)
    text = re.sub(r"<[^>]+>", "", text)
    text = re.sub(r"\s+", " ", text)
    return text.strip()
