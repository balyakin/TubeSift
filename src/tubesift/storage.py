from __future__ import annotations

import sqlite3
from pathlib import Path
from typing import TYPE_CHECKING, Iterable, Mapping, Sequence

from tubesift.subtitles import SubtitleSegment
from tubesift.utils import utc_now_iso

if TYPE_CHECKING:
    from tubesift.extractor import ScopeMetadata, VideoMetadata


SCHEMA_SQL = """
PRAGMA foreign_keys = ON;

CREATE TABLE IF NOT EXISTS scopes (
    id TEXT PRIMARY KEY,
    scope_type TEXT NOT NULL,
    handle TEXT,
    title TEXT NOT NULL,
    url TEXT NOT NULL,
    subscriber_count INTEGER,
    video_count INTEGER,
    last_synced_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS videos (
    id TEXT PRIMARY KEY,
    scope_id TEXT NOT NULL,
    title TEXT NOT NULL,
    description TEXT NOT NULL DEFAULT '',
    published_at TEXT NOT NULL,
    duration_seconds INTEGER NOT NULL DEFAULT 0,
    view_count INTEGER NOT NULL DEFAULT 0,
    is_short INTEGER NOT NULL DEFAULT 0,
    thumbnail_url TEXT NOT NULL DEFAULT '',
    video_url TEXT NOT NULL,
    subtitle_status TEXT NOT NULL DEFAULT 'none',
    last_synced_at TEXT NOT NULL,
    FOREIGN KEY (scope_id) REFERENCES scopes(id) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS subtitle_segments (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    video_id TEXT NOT NULL,
    language TEXT NOT NULL,
    start_seconds REAL NOT NULL,
    end_seconds REAL NOT NULL,
    text TEXT NOT NULL,
    FOREIGN KEY (video_id) REFERENCES videos(id) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS fetch_state (
    scope_id TEXT PRIMARY KEY,
    last_fetch_started_at TEXT,
    last_fetch_finished_at TEXT,
    last_success_at TEXT,
    last_error TEXT,
    FOREIGN KEY (scope_id) REFERENCES scopes(id) ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS idx_videos_scope_id ON videos(scope_id);
CREATE INDEX IF NOT EXISTS idx_videos_published_at ON videos(published_at);
CREATE INDEX IF NOT EXISTS idx_videos_view_count ON videos(view_count);
CREATE INDEX IF NOT EXISTS idx_videos_duration_seconds ON videos(duration_seconds);
CREATE INDEX IF NOT EXISTS idx_subtitle_segments_lookup
    ON subtitle_segments(video_id, language, start_seconds);

CREATE VIRTUAL TABLE IF NOT EXISTS video_fts USING fts5(
    video_id UNINDEXED,
    title,
    description,
    scope_title,
    tokenize='porter unicode61'
);

CREATE VIRTUAL TABLE IF NOT EXISTS subtitle_fts USING fts5(
    segment_id UNINDEXED,
    video_id UNINDEXED,
    language UNINDEXED,
    text,
    tokenize='porter unicode61'
);
"""


class Storage:
    def __init__(self, db_path: Path) -> None:
        self.db_path = Path(db_path)
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self.conn = sqlite3.connect(self.db_path)
        self.conn.row_factory = sqlite3.Row
        self.conn.execute("PRAGMA foreign_keys = ON")
        self.conn.execute("PRAGMA journal_mode = WAL")
        self.conn.execute("PRAGMA synchronous = NORMAL")
        self.migrate()

    def close(self) -> None:
        self.conn.close()

    def migrate(self) -> None:
        self.conn.executescript(SCHEMA_SQL)
        self.conn.commit()

    def upsert_scope(self, scope: ScopeMetadata) -> None:
        with self.conn:
            self.conn.execute(
                """
                INSERT INTO scopes (
                    id,
                    scope_type,
                    handle,
                    title,
                    url,
                    subscriber_count,
                    video_count,
                    last_synced_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(id) DO UPDATE SET
                    scope_type=excluded.scope_type,
                    handle=excluded.handle,
                    title=excluded.title,
                    url=excluded.url,
                    subscriber_count=excluded.subscriber_count,
                    video_count=excluded.video_count,
                    last_synced_at=excluded.last_synced_at
                """,
                (
                    scope.id,
                    scope.scope_type,
                    scope.handle,
                    scope.title,
                    scope.url,
                    scope.subscriber_count,
                    scope.video_count,
                    scope.last_synced_at,
                ),
            )

    def upsert_videos(
        self,
        videos: Sequence["VideoMetadata"],
        scope_title: str,
    ) -> tuple[int, int, int]:
        if not videos:
            return 0, 0, 0

        video_ids = [video.id for video in videos]
        existing = self.video_state_by_ids(video_ids)

        created = 0
        updated = 0
        skipped = 0
        with self.conn:
            for video in videos:
                prev = existing.get(video.id)
                if prev is None:
                    created += 1
                else:
                    changed = _is_video_changed(prev, video)
                    if changed:
                        updated += 1
                    else:
                        skipped += 1

                self.conn.execute(
                    """
                    INSERT INTO videos (
                        id,
                        scope_id,
                        title,
                        description,
                        published_at,
                        duration_seconds,
                        view_count,
                        is_short,
                        thumbnail_url,
                        video_url,
                        subtitle_status,
                        last_synced_at
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    ON CONFLICT(id) DO UPDATE SET
                        scope_id=excluded.scope_id,
                        title=excluded.title,
                        description=excluded.description,
                        published_at=excluded.published_at,
                        duration_seconds=excluded.duration_seconds,
                        view_count=excluded.view_count,
                        is_short=excluded.is_short,
                        thumbnail_url=excluded.thumbnail_url,
                        video_url=excluded.video_url,
                        subtitle_status=excluded.subtitle_status,
                        last_synced_at=excluded.last_synced_at
                    """,
                    (
                        video.id,
                        video.scope_id,
                        video.title,
                        video.description,
                        video.published_at,
                        video.duration_seconds,
                        video.view_count,
                        video.is_short,
                        video.thumbnail_url,
                        video.video_url,
                        video.subtitle_status,
                        video.last_synced_at,
                    ),
                )
                self.conn.execute("DELETE FROM video_fts WHERE video_id = ?", (video.id,))
                self.conn.execute(
                    """
                    INSERT INTO video_fts (video_id, title, description, scope_title)
                    VALUES (?, ?, ?, ?)
                    """,
                    (video.id, video.title, video.description, scope_title),
                )

        return created, updated, skipped

    def video_state_by_ids(self, video_ids: Sequence[str]) -> dict[str, sqlite3.Row]:
        if not video_ids:
            return {}
        existing: dict[str, sqlite3.Row] = {}
        for chunk in _iter_chunks(video_ids, size=500):
            placeholders = ",".join("?" for _ in chunk)
            rows = self.conn.execute(
                f"""
                SELECT
                    id,
                    scope_id,
                    title,
                    description,
                    published_at,
                    duration_seconds,
                    view_count,
                    is_short,
                    thumbnail_url,
                    video_url,
                    subtitle_status,
                    last_synced_at
                FROM videos
                WHERE id IN ({placeholders})
                """,
                chunk,
            ).fetchall()
            for row in rows:
                existing[row["id"]] = row
        return existing

    def scope_video_state(self, scope_id: str, video_ids: Sequence[str]) -> dict[str, sqlite3.Row]:
        if not video_ids:
            return {}
        existing: dict[str, sqlite3.Row] = {}
        for chunk in _iter_chunks(video_ids, size=500):
            placeholders = ",".join("?" for _ in chunk)
            rows = self.conn.execute(
                f"""
                SELECT
                    id,
                    subtitle_status,
                    last_synced_at,
                    published_at,
                    view_count,
                    duration_seconds,
                    title,
                    description,
                    video_url,
                    is_short,
                    thumbnail_url
                FROM videos
                WHERE scope_id = ? AND id IN ({placeholders})
                """,
                [scope_id, *chunk],
            ).fetchall()
            for row in rows:
                existing[row["id"]] = row
        return existing

    def upsert_subtitles(
        self,
        subtitles_by_video: Mapping[str, Sequence[SubtitleSegment]],
    ) -> tuple[int, int]:
        videos_with_subtitles = 0
        total_segments = 0

        with self.conn:
            for video_id, segments in subtitles_by_video.items():
                self.conn.execute("DELETE FROM subtitle_fts WHERE video_id = ?", (video_id,))
                self.conn.execute("DELETE FROM subtitle_segments WHERE video_id = ?", (video_id,))

                if not segments:
                    continue

                videos_with_subtitles += 1
                for segment in segments:
                    cursor = self.conn.execute(
                        """
                        INSERT INTO subtitle_segments (
                            video_id,
                            language,
                            start_seconds,
                            end_seconds,
                            text
                        ) VALUES (?, ?, ?, ?, ?)
                        """,
                        (
                            video_id,
                            segment.language,
                            segment.start_seconds,
                            segment.end_seconds,
                            segment.text,
                        ),
                    )
                    segment_id = int(cursor.lastrowid)
                    self.conn.execute(
                        """
                        INSERT INTO subtitle_fts (segment_id, video_id, language, text)
                        VALUES (?, ?, ?, ?)
                        """,
                        (segment_id, video_id, segment.language, segment.text),
                    )
                    total_segments += 1

        return videos_with_subtitles, total_segments

    def mark_fetch_state(
        self,
        scope_id: str,
        *,
        started_at: str | None = None,
        finished_at: str | None = None,
        success: bool,
        error: str | None = None,
    ) -> None:
        now = utc_now_iso()
        with self.conn:
            self.conn.execute(
                """
                INSERT INTO fetch_state (
                    scope_id,
                    last_fetch_started_at,
                    last_fetch_finished_at,
                    last_success_at,
                    last_error
                ) VALUES (?, ?, ?, ?, ?)
                ON CONFLICT(scope_id) DO UPDATE SET
                    last_fetch_started_at=excluded.last_fetch_started_at,
                    last_fetch_finished_at=excluded.last_fetch_finished_at,
                    last_success_at=excluded.last_success_at,
                    last_error=excluded.last_error
                """,
                (
                    scope_id,
                    started_at or now,
                    finished_at or now,
                    now if success else None,
                    None if success else (error or "unknown error"),
                ),
            )

    def list_scopes(self) -> list[sqlite3.Row]:
        rows = self.conn.execute(
            """
            SELECT id, scope_type, handle, title, url, video_count, last_synced_at
            FROM scopes
            ORDER BY title COLLATE NOCASE ASC
            """
        ).fetchall()
        return list(rows)

    def find_scope_ids(self, raw_scope: str) -> list[str]:
        value = raw_scope.strip()
        if not value:
            return []
        rows = self.conn.execute(
            """
            SELECT id
            FROM scopes
            WHERE id = ?
               OR handle = ?
               OR url = ?
               OR title = ?
               OR title LIKE '%' || ? || '%'
            ORDER BY title COLLATE NOCASE ASC
            """,
            (value, value, value, value, value),
        ).fetchall()
        return [row["id"] for row in rows]

    def scope_info(self, scope_id: str) -> sqlite3.Row | None:
        row = self.conn.execute(
            """
            SELECT
                s.id,
                s.scope_type,
                s.handle,
                s.title,
                s.url,
                s.subscriber_count,
                s.video_count,
                s.last_synced_at,
                COUNT(v.id) AS indexed_videos,
                COALESCE(SUM(CASE WHEN v.subtitle_status != 'none' THEN 1 ELSE 0 END), 0) AS videos_with_subtitles,
                COALESCE(MIN(v.published_at), '') AS earliest_video,
                COALESCE(MAX(v.published_at), '') AS latest_video
            FROM scopes s
            LEFT JOIN videos v ON v.scope_id = s.id
            WHERE s.id = ?
            GROUP BY s.id
            """,
            (scope_id,),
        ).fetchone()
        return row

    def top_videos(self, scope_id: str, limit: int = 10) -> list[sqlite3.Row]:
        rows = self.conn.execute(
            """
            SELECT id, title, published_at, view_count, duration_seconds, video_url
            FROM videos
            WHERE scope_id = ?
            ORDER BY view_count DESC, published_at DESC
            LIMIT ?
            """,
            (scope_id, limit),
        ).fetchall()
        return list(rows)

    def clear_scope(self, scope_id: str) -> tuple[int, int]:
        video_ids = [
            row["id"]
            for row in self.conn.execute(
                "SELECT id FROM videos WHERE scope_id = ?",
                (scope_id,),
            ).fetchall()
        ]
        with self.conn:
            for video_id in video_ids:
                self.conn.execute("DELETE FROM video_fts WHERE video_id = ?", (video_id,))
                self.conn.execute("DELETE FROM subtitle_fts WHERE video_id = ?", (video_id,))
            deleted_videos = self.conn.execute(
                "DELETE FROM videos WHERE scope_id = ?",
                (scope_id,),
            ).rowcount
            deleted_scopes = self.conn.execute(
                "DELETE FROM scopes WHERE id = ?",
                (scope_id,),
            ).rowcount
            self.conn.execute(
                "DELETE FROM fetch_state WHERE scope_id = ?",
                (scope_id,),
            )
        return deleted_scopes, deleted_videos

    def all_scope_ids(self) -> list[str]:
        rows = self.conn.execute("SELECT id FROM scopes ORDER BY title COLLATE NOCASE ASC").fetchall()
        return [row["id"] for row in rows]

    def get_video(self, video_id: str) -> sqlite3.Row | None:
        row = self.conn.execute(
            """
            SELECT
                v.id,
                v.scope_id,
                v.title,
                v.description,
                v.published_at,
                v.duration_seconds,
                v.view_count,
                v.video_url,
                s.title AS scope_title
            FROM videos v
            JOIN scopes s ON s.id = v.scope_id
            WHERE v.id = ?
            """,
            (video_id,),
        ).fetchone()
        return row

    def execute(self, sql: str, params: Sequence[object] | None = None) -> Iterable[sqlite3.Row]:
        cursor = self.conn.execute(sql, params or [])
        return cursor.fetchall()


def _is_video_changed(previous: sqlite3.Row, video: "VideoMetadata") -> bool:
    return (
        previous["title"] != video.title
        or previous["description"] != video.description
        or previous["published_at"] != video.published_at
        or previous["duration_seconds"] != video.duration_seconds
        or previous["view_count"] != video.view_count
        or previous["is_short"] != video.is_short
        or previous["thumbnail_url"] != video.thumbnail_url
        or previous["video_url"] != video.video_url
        or previous["subtitle_status"] != video.subtitle_status
    )


def _iter_chunks(items: Sequence[str], size: int) -> Iterable[list[str]]:
    total = len(items)
    for start in range(0, total, size):
        yield list(items[start : start + size])
