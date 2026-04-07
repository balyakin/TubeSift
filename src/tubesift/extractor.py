from __future__ import annotations

from dataclasses import dataclass
import re

try:
    from yt_dlp import YoutubeDL
except Exception as exc:  # pragma: no cover - depends on runtime env
    YoutubeDL = None
    _YTDLP_IMPORT_ERROR = exc
else:
    _YTDLP_IMPORT_ERROR = None

from tubesift.resolver import ScopeRef
from tubesift.subtitles import SubtitleSegment, fetch_subtitle_segments
from tubesift.utils import normalize_upload_date, safe_int, utc_now_iso


@dataclass(slots=True)
class ScopeMetadata:
    id: str
    scope_type: str
    handle: str | None
    title: str
    url: str
    subscriber_count: int
    video_count: int
    last_synced_at: str


@dataclass(slots=True)
class VideoMetadata:
    id: str
    scope_id: str
    title: str
    description: str
    published_at: str
    duration_seconds: int
    view_count: int
    is_short: int
    thumbnail_url: str
    video_url: str
    subtitle_status: str
    last_synced_at: str


@dataclass(slots=True)
class VideoFetchResult:
    video: VideoMetadata
    subtitle_segments: list[SubtitleSegment]


class Extractor:
    def __init__(
        self,
        *,
        cookies_from_browser: str | None = None,
        cookies_file: str | None = None,
    ) -> None:
        logger = _SilentYtDlpLogger()
        self._base_opts = {
            "quiet": True,
            "no_warnings": True,
            "ignoreconfig": True,
            "skip_download": True,
            "ignoreerrors": False,
            "extractor_retries": 2,
            "retries": 2,
            "socket_timeout": 20,
            "cachedir": False,
            "logger": logger,
            "noprogress": True,
        }
        if cookies_from_browser:
            self._base_opts["cookiesfrombrowser"] = (cookies_from_browser,)
        if cookies_file:
            self._base_opts["cookiefile"] = cookies_file

    def list_scope_videos(self, scope: ScopeRef) -> tuple[ScopeMetadata, list[str]]:
        _require_yt_dlp()
        opts = {
            **self._base_opts,
            "extract_flat": "in_playlist",
            "playlistend": None,
            "ignoreerrors": True,
        }
        listing_url = _scope_listing_url(scope)
        with YoutubeDL(opts) as ydl:
            info = ydl.extract_info(listing_url, download=False)

        entries = info.get("entries") or []
        video_ids: list[str] = []
        seen: set[str] = set()
        for entry in entries:
            if not isinstance(entry, dict):
                continue
            video_id = entry.get("id")
            if not video_id or not isinstance(video_id, str):
                continue
            if not re.fullmatch(r"[A-Za-z0-9_-]{11}", video_id):
                continue
            if video_id in seen:
                continue
            seen.add(video_id)
            video_ids.append(video_id)

        title = (
            info.get("channel")
            or info.get("uploader")
            or info.get("title")
            or scope.raw
        )
        scope_meta = ScopeMetadata(
            id=scope.scope_id,
            scope_type=scope.scope_type,
            handle=scope.handle,
            title=title,
            url=listing_url,
            subscriber_count=safe_int(info.get("channel_follower_count"), 0),
            video_count=safe_int(info.get("playlist_count"), len(video_ids)),
            last_synced_at=utc_now_iso(),
        )
        return scope_meta, video_ids

    def fetch_video(
        self,
        video_id: str,
        scope_id: str,
        subs_mode: str = "none",
        lang: str | None = None,
    ) -> VideoFetchResult:
        _require_yt_dlp()
        url = f"https://www.youtube.com/watch?v={video_id}"
        try:
            with YoutubeDL(self._base_opts) as ydl:
                info = ydl.extract_info(url, download=False)
        except Exception as exc:
            raise RuntimeError(_normalize_extractor_error(str(exc))) from exc

        if not isinstance(info, dict):
            raise RuntimeError(f"could not extract metadata for {video_id}")

        title = str(info.get("title") or video_id)
        description = str(info.get("description") or "")
        published_at = normalize_upload_date(
            upload_date=info.get("upload_date"),
            release_date=info.get("release_date"),
            timestamp=info.get("timestamp"),
        )
        duration_seconds = safe_int(info.get("duration"), 0)
        view_count = safe_int(info.get("view_count"), 0)
        video_url = str(info.get("webpage_url") or url)
        thumbnail_url = str(info.get("thumbnail") or "")
        is_short = 1 if "/shorts/" in video_url or duration_seconds <= 60 else 0

        subtitle_status = _subtitle_status(info)
        subtitle_segments: list[SubtitleSegment] = []
        if subs_mode != "none":
            subtitle_segments = fetch_subtitle_segments(
                info=info,
                mode=subs_mode,
                lang=lang,
            )
            if subtitle_segments:
                subtitle_status = "fetched"

        video = VideoMetadata(
            id=video_id,
            scope_id=scope_id,
            title=title,
            description=description,
            published_at=published_at,
            duration_seconds=duration_seconds,
            view_count=view_count,
            is_short=is_short,
            thumbnail_url=thumbnail_url,
            video_url=video_url,
            subtitle_status=subtitle_status,
            last_synced_at=utc_now_iso(),
        )
        return VideoFetchResult(video=video, subtitle_segments=subtitle_segments)


def _subtitle_status(info: dict) -> str:
    subtitles = info.get("subtitles") or {}
    auto = info.get("automatic_captions") or {}
    if subtitles:
        return "manual"
    if auto:
        return "auto"
    return "none"


def _require_yt_dlp() -> None:
    if YoutubeDL is None:
        raise RuntimeError(f"yt-dlp is not installed: {_YTDLP_IMPORT_ERROR}")


def _scope_listing_url(scope: ScopeRef) -> str:
    if scope.scope_type == "channel":
        if scope.lookup_url.endswith("/videos"):
            return scope.lookup_url
        return scope.lookup_url.rstrip("/") + "/videos"
    return scope.lookup_url


def _normalize_extractor_error(message: str) -> str:
    msg = message.strip().replace("\n", " ")
    if "Sign in to confirm you" in msg:
        return "youtube rate-limit challenge: try again later or use yt-dlp cookies"
    if "Video unavailable" in msg:
        return "video is unavailable"
    if msg.startswith("ERROR:"):
        msg = msg[len("ERROR:") :].strip()
    return msg


class _SilentYtDlpLogger:
    def debug(self, msg: str) -> None:
        return None

    def warning(self, msg: str) -> None:
        return None

    def error(self, msg: str) -> None:
        return None
