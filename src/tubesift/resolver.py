from __future__ import annotations

import re
from dataclasses import dataclass
from urllib.parse import parse_qs, urlparse


class ScopeResolutionError(ValueError):
    pass


@dataclass(slots=True)
class ScopeRef:
    raw: str
    scope_type: str
    scope_id: str
    lookup_url: str
    handle: str | None = None


def resolve_scope(raw_scope: str) -> ScopeRef:
    raw = raw_scope.strip()
    if not raw:
        raise ScopeResolutionError("scope cannot be empty")

    if raw.startswith("@"):
        handle = raw[1:].strip()
        if not handle:
            raise ScopeResolutionError("handle is empty")
        normalized = handle.lower()
        return ScopeRef(
            raw=raw,
            scope_type="channel",
            scope_id=f"handle:{normalized}",
            lookup_url=f"https://www.youtube.com/@{handle}",
            handle=f"@{handle}",
        )

    if re.fullmatch(r"UC[0-9A-Za-z_-]{22}", raw):
        return ScopeRef(
            raw=raw,
            scope_type="channel",
            scope_id=f"channel:{raw}",
            lookup_url=f"https://www.youtube.com/channel/{raw}",
            handle=None,
        )

    parsed = urlparse(raw)
    if parsed.scheme and parsed.netloc:
        return _from_url(raw, parsed)

    raise ScopeResolutionError(
        "unsupported scope format; use @handle, channel URL/id, or playlist URL"
    )


def _from_url(raw: str, parsed) -> ScopeRef:
    host = parsed.netloc.lower()
    if host.startswith("www."):
        host = host[4:]

    if host not in {"youtube.com", "m.youtube.com", "youtu.be"}:
        raise ScopeResolutionError("only youtube.com / youtu.be scopes are supported")

    query = parse_qs(parsed.query)
    playlist_id = (query.get("list") or [None])[0]
    if playlist_id:
        return ScopeRef(
            raw=raw,
            scope_type="playlist",
            scope_id=f"playlist:{playlist_id}",
            lookup_url=f"https://www.youtube.com/playlist?list={playlist_id}",
            handle=None,
        )

    parts = [part for part in parsed.path.split("/") if part]
    if not parts:
        raise ScopeResolutionError("youtube URL has no resolvable scope")

    if parts[0].startswith("@"):
        handle = parts[0][1:]
        normalized = handle.lower()
        return ScopeRef(
            raw=raw,
            scope_type="channel",
            scope_id=f"handle:{normalized}",
            lookup_url=f"https://www.youtube.com/@{handle}",
            handle=f"@{handle}",
        )

    if len(parts) >= 2 and parts[0] == "channel":
        channel_id = parts[1]
        if not re.fullmatch(r"UC[0-9A-Za-z_-]{22}", channel_id):
            raise ScopeResolutionError("invalid channel id in URL")
        return ScopeRef(
            raw=raw,
            scope_type="channel",
            scope_id=f"channel:{channel_id}",
            lookup_url=f"https://www.youtube.com/channel/{channel_id}",
            handle=None,
        )

    raise ScopeResolutionError("unsupported youtube URL scope")
