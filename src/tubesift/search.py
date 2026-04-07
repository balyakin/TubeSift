from __future__ import annotations

import re
import sqlite3
from dataclasses import dataclass, field as dataclass_field

from tubesift.ranking import compute_score
from tubesift.storage import Storage
from tubesift.utils import truncate, with_timestamp


@dataclass(slots=True)
class SearchFilters:
    after: str | None = None
    before: str | None = None
    min_views: int | None = None
    max_views: int | None = None
    min_duration: int | None = None
    max_duration: int | None = None
    no_shorts: bool = False
    only_shorts: bool = False
    lang: str | None = None


@dataclass(slots=True)
class SearchRequest:
    scope: str | None
    query: str | None
    field: str = "any"
    regex: bool = False
    sort: str = "score"
    limit: int = 30
    filters: SearchFilters = dataclass_field(default_factory=SearchFilters)


@dataclass(slots=True)
class SearchResult:
    video_id: str
    title: str
    channel: str
    published_at: str
    view_count: int
    duration_seconds: int
    match_type: str
    timestamp_seconds: float | None
    snippet: str
    url: str
    score: float
    regex_haystack: str


class SearchEngine:
    def __init__(self, storage: Storage) -> None:
        self.storage = storage

    def search(self, request: SearchRequest) -> list[SearchResult]:
        scope_ids = self._resolve_scope_ids(request.scope)
        limit = max(1, min(request.limit, 500))

        try:
            if request.regex:
                results = self._search_regex(scope_ids=scope_ids, request=request)
            elif request.query:
                results = self._search_fts(scope_ids=scope_ids, request=request)
            else:
                results = self._search_structured_only(scope_ids=scope_ids, request=request)
        except sqlite3.OperationalError as exc:
            raise ValueError(f"search query error: {exc}") from exc

        sorted_results = self._sort_results(results, request.sort)
        return sorted_results[:limit]

    def _search_fts(self, scope_ids: list[str] | None, request: SearchRequest) -> list[SearchResult]:
        fts_query = prepare_fts_query(request.query or "")
        if not fts_query:
            return self._search_structured_only(scope_ids=scope_ids, request=request)

        candidates: dict[str, SearchResult] = {}
        candidate_limit = max(request.limit * 12, 120)
        filters_sql, filter_params = _video_filter_sql(request.filters, scope_ids, alias="v")

        if request.field in {"any", "title", "description"}:
            scoped_query = _scoped_metadata_query(fts_query, request.field)
            metadata_rows = self.storage.conn.execute(
                f"""
                SELECT
                    v.id,
                    v.title,
                    v.description,
                    v.published_at,
                    v.duration_seconds,
                    v.view_count,
                    v.video_url,
                    s.title AS scope_title,
                    bm25(video_fts, 8.0, 2.0, 1.0) AS fts_rank,
                    snippet(video_fts, 1, '[', ']', ' ... ', 12) AS title_snippet,
                    snippet(video_fts, 2, '[', ']', ' ... ', 18) AS description_snippet
                FROM video_fts
                JOIN videos v ON v.id = video_fts.video_id
                JOIN scopes s ON s.id = v.scope_id
                WHERE video_fts MATCH ? AND {filters_sql}
                LIMIT ?
                """,
                [scoped_query, *filter_params, candidate_limit],
            ).fetchall()

            query_text = (request.query or "").lower()
            for row in metadata_rows:
                title = row["title"]
                description = row["description"]
                title_lc = title.lower()
                description_lc = description.lower()

                if request.field == "title":
                    match_type = "title"
                elif request.field == "description":
                    match_type = "description"
                elif query_text and query_text in title_lc:
                    match_type = "title"
                elif "[" in (row["title_snippet"] or ""):
                    match_type = "title"
                else:
                    match_type = "description"

                snippet = row["title_snippet"] if match_type == "title" else row["description_snippet"]
                if not snippet:
                    snippet = truncate(description or title, 160)

                phrase_hit = _phrase_hit(request.query or "", f"{title}\n{description}")
                score = compute_score(
                    match_type=match_type,
                    fts_rank=float(row["fts_rank"] or 0.0),
                    published_at=row["published_at"],
                    view_count=row["view_count"],
                    phrase_hit=phrase_hit,
                )

                result = SearchResult(
                    video_id=row["id"],
                    title=title,
                    channel=row["scope_title"],
                    published_at=row["published_at"],
                    view_count=row["view_count"],
                    duration_seconds=row["duration_seconds"],
                    match_type=match_type,
                    timestamp_seconds=None,
                    snippet=snippet,
                    url=row["video_url"],
                    score=score,
                    regex_haystack=f"{title}\n{description}",
                )
                _merge_candidate(candidates, result)

        if request.field in {"any", "subtitle"}:
            subtitle_where_sql = filters_sql
            subtitle_params: list[object] = list(filter_params)
            lang_clause = ""
            if request.filters.lang:
                lang_clause = " AND sf.language LIKE ?"
                subtitle_params.append(f"{request.filters.lang.lower()}%")

            subtitle_rows = self.storage.conn.execute(
                f"""
                SELECT
                    sf.video_id,
                    sf.language,
                    seg.start_seconds,
                    seg.end_seconds,
                    sf.text AS subtitle_text,
                    v.title,
                    v.description,
                    v.published_at,
                    v.duration_seconds,
                    v.view_count,
                    v.video_url,
                    s.title AS scope_title,
                    bm25(subtitle_fts, 1.0) AS fts_rank,
                    snippet(subtitle_fts, 3, '[', ']', ' ... ', 20) AS subtitle_snippet
                FROM subtitle_fts sf
                JOIN subtitle_segments seg ON seg.id = sf.segment_id
                JOIN videos v ON v.id = sf.video_id
                JOIN scopes s ON s.id = v.scope_id
                WHERE subtitle_fts MATCH ? AND {subtitle_where_sql}{lang_clause}
                LIMIT ?
                """,
                [fts_query, *subtitle_params, candidate_limit],
            ).fetchall()

            for row in subtitle_rows:
                text = row["subtitle_text"] or ""
                phrase_hit = _phrase_hit(request.query or "", text)
                score = compute_score(
                    match_type="subtitle",
                    fts_rank=float(row["fts_rank"] or 0.0),
                    published_at=row["published_at"],
                    view_count=row["view_count"],
                    phrase_hit=phrase_hit,
                )
                snippet = row["subtitle_snippet"] or truncate(text, 180)
                start_seconds = float(row["start_seconds"])
                result = SearchResult(
                    video_id=row["video_id"],
                    title=row["title"],
                    channel=row["scope_title"],
                    published_at=row["published_at"],
                    view_count=row["view_count"],
                    duration_seconds=row["duration_seconds"],
                    match_type="subtitle",
                    timestamp_seconds=start_seconds,
                    snippet=snippet,
                    url=with_timestamp(row["video_url"], start_seconds),
                    score=score,
                    regex_haystack=text,
                )
                _merge_candidate(candidates, result)

        return list(candidates.values())

    def _search_regex(self, scope_ids: list[str] | None, request: SearchRequest) -> list[SearchResult]:
        query = request.query or ""
        if not query:
            return []
        try:
            pattern = re.compile(query, re.IGNORECASE)
        except re.error as exc:
            raise ValueError(f"invalid regex: {exc}") from exc

        candidates: dict[str, SearchResult] = {}
        candidate_limit = max(request.limit * 30, 300)
        filters_sql, filter_params = _video_filter_sql(request.filters, scope_ids, alias="v")
        hint_query = _regex_hint_query(query)

        if request.field in {"any", "title", "description"}:
            if hint_query:
                rows = self.storage.conn.execute(
                    f"""
                    SELECT
                        v.id,
                        v.title,
                        v.description,
                        v.published_at,
                        v.duration_seconds,
                        v.view_count,
                        v.video_url,
                        s.title AS scope_title
                    FROM video_fts
                    JOIN videos v ON v.id = video_fts.video_id
                    JOIN scopes s ON s.id = v.scope_id
                    WHERE video_fts MATCH ? AND {filters_sql}
                    LIMIT ?
                    """,
                    [hint_query, *filter_params, candidate_limit],
                ).fetchall()
            else:
                rows = self.storage.conn.execute(
                    f"""
                    SELECT
                        v.id,
                        v.title,
                        v.description,
                        v.published_at,
                        v.duration_seconds,
                        v.view_count,
                        v.video_url,
                        s.title AS scope_title
                    FROM videos v
                    JOIN scopes s ON s.id = v.scope_id
                    WHERE {filters_sql}
                    ORDER BY v.published_at DESC
                    LIMIT ?
                    """,
                    [*filter_params, candidate_limit],
                ).fetchall()

            for row in rows:
                title = row["title"] or ""
                description = row["description"] or ""

                title_hit = bool(pattern.search(title))
                description_hit = bool(pattern.search(description))
                if request.field == "title" and not title_hit:
                    continue
                if request.field == "description" and not description_hit:
                    continue
                if request.field == "any" and not (title_hit or description_hit):
                    continue

                match_type = "title" if title_hit else "description"
                haystack = title if title_hit else description
                match = pattern.search(haystack)
                snippet = truncate(haystack[max((match.start() if match else 0) - 50, 0) :], 160)
                score = compute_score(
                    match_type=match_type,
                    fts_rank=0.0,
                    published_at=row["published_at"],
                    view_count=row["view_count"],
                )
                result = SearchResult(
                    video_id=row["id"],
                    title=row["title"],
                    channel=row["scope_title"],
                    published_at=row["published_at"],
                    view_count=row["view_count"],
                    duration_seconds=row["duration_seconds"],
                    match_type=match_type,
                    timestamp_seconds=None,
                    snippet=snippet,
                    url=row["video_url"],
                    score=score,
                    regex_haystack=f"{title}\n{description}",
                )
                _merge_candidate(candidates, result)

        if request.field in {"any", "subtitle"}:
            subtitle_filters = filters_sql
            params: list[object] = list(filter_params)
            lang_clause = ""
            if request.filters.lang:
                lang_clause = " AND seg.language LIKE ?"
                params.append(f"{request.filters.lang.lower()}%")

            if hint_query:
                rows = self.storage.conn.execute(
                    f"""
                    SELECT
                        seg.video_id,
                        seg.language,
                        seg.start_seconds,
                        seg.text,
                        v.title,
                        v.description,
                        v.published_at,
                        v.duration_seconds,
                        v.view_count,
                        v.video_url,
                        s.title AS scope_title
                    FROM subtitle_fts sf
                    JOIN subtitle_segments seg ON seg.id = sf.segment_id
                    JOIN videos v ON v.id = seg.video_id
                    JOIN scopes s ON s.id = v.scope_id
                    WHERE subtitle_fts MATCH ? AND {subtitle_filters}{lang_clause}
                    LIMIT ?
                    """,
                    [hint_query, *params, candidate_limit * 5],
                ).fetchall()
            else:
                rows = self.storage.conn.execute(
                    f"""
                    SELECT
                        seg.video_id,
                        seg.language,
                        seg.start_seconds,
                        seg.text,
                        v.title,
                        v.description,
                        v.published_at,
                        v.duration_seconds,
                        v.view_count,
                        v.video_url,
                        s.title AS scope_title
                    FROM subtitle_segments seg
                    JOIN videos v ON v.id = seg.video_id
                    JOIN scopes s ON s.id = v.scope_id
                    WHERE {subtitle_filters}{lang_clause}
                    ORDER BY v.published_at DESC
                    LIMIT ?
                    """,
                    [*params, candidate_limit * 5],
                ).fetchall()

            for row in rows:
                text = row["text"] or ""
                hit = pattern.search(text)
                if not hit:
                    continue
                start = max(hit.start() - 50, 0)
                snippet = truncate(text[start:], 180)
                score = compute_score(
                    match_type="subtitle",
                    fts_rank=0.0,
                    published_at=row["published_at"],
                    view_count=row["view_count"],
                )
                result = SearchResult(
                    video_id=row["video_id"],
                    title=row["title"],
                    channel=row["scope_title"],
                    published_at=row["published_at"],
                    view_count=row["view_count"],
                    duration_seconds=row["duration_seconds"],
                    match_type="subtitle",
                    timestamp_seconds=float(row["start_seconds"]),
                    snippet=snippet,
                    url=with_timestamp(row["video_url"], row["start_seconds"]),
                    score=score,
                    regex_haystack=text,
                )
                _merge_candidate(candidates, result)

        return list(candidates.values())

    def _search_structured_only(
        self,
        scope_ids: list[str] | None,
        request: SearchRequest,
    ) -> list[SearchResult]:
        filters_sql, filter_params = _video_filter_sql(request.filters, scope_ids, alias="v")
        sort_column = {
            "date": "v.published_at DESC",
            "views": "v.view_count DESC",
            "duration": "v.duration_seconds DESC",
            "score": "v.published_at DESC",
        }.get(request.sort, "v.published_at DESC")

        rows = self.storage.conn.execute(
            f"""
            SELECT
                v.id,
                v.title,
                v.description,
                v.published_at,
                v.duration_seconds,
                v.view_count,
                v.video_url,
                s.title AS scope_title
            FROM videos v
            JOIN scopes s ON s.id = v.scope_id
            WHERE {filters_sql}
            ORDER BY {sort_column}
            LIMIT ?
            """,
            [*filter_params, max(request.limit, 30)],
        ).fetchall()

        results: list[SearchResult] = []
        for row in rows:
            score = compute_score(
                match_type="metadata",
                fts_rank=0.5,
                published_at=row["published_at"],
                view_count=row["view_count"],
            )
            results.append(
                SearchResult(
                    video_id=row["id"],
                    title=row["title"],
                    channel=row["scope_title"],
                    published_at=row["published_at"],
                    view_count=row["view_count"],
                    duration_seconds=row["duration_seconds"],
                    match_type="metadata",
                    timestamp_seconds=None,
                    snippet=truncate(row["description"] or row["title"], 160),
                    url=row["video_url"],
                    score=score,
                    regex_haystack=f"{row['title']}\n{row['description']}",
                )
            )
        return results

    def _resolve_scope_ids(self, scope: str | None) -> list[str] | None:
        if not scope:
            return None
        ids = self.storage.find_scope_ids(scope)
        if not ids:
            raise ValueError(f"scope not indexed locally: {scope}")
        return ids

    @staticmethod
    def _sort_results(results: list[SearchResult], mode: str) -> list[SearchResult]:
        if mode == "date":
            return sorted(results, key=lambda item: item.published_at, reverse=True)
        if mode == "views":
            return sorted(results, key=lambda item: item.view_count, reverse=True)
        if mode == "duration":
            return sorted(results, key=lambda item: item.duration_seconds, reverse=True)
        return sorted(results, key=lambda item: item.score, reverse=True)


def prepare_fts_query(query: str) -> str:
    cleaned = query.strip()
    if not cleaned:
        return ""
    if any(token in cleaned for token in ['"', "*", " OR ", " AND ", " NOT "]):
        return cleaned
    terms = [term for term in re.split(r"\s+", cleaned) if term]
    if len(terms) == 1:
        return _sanitize_fts_term(terms[0])
    safe_terms = [token for token in (_sanitize_fts_term(term) for term in terms) if token]
    return " AND ".join(safe_terms)


def _sanitize_fts_term(term: str) -> str:
    if re.fullmatch(r"[A-Za-z0-9_]+", term):
        return term
    escaped = term.replace('"', " ").strip()
    if not escaped:
        return ""
    return f'"{escaped}"'


def _regex_hint_query(pattern: str) -> str | None:
    tokens = [token.lower() for token in re.findall(r"[A-Za-z0-9_]{3,}", pattern)]
    if not tokens:
        return None

    unique: list[str] = []
    seen: set[str] = set()
    for token in tokens:
        if token in seen:
            continue
        seen.add(token)
        unique.append(token)
        if len(unique) >= 8:
            break

    safe_terms = [term for term in (_sanitize_fts_term(token) for token in unique) if term]
    if not safe_terms:
        return None
    if len(safe_terms) == 1:
        return safe_terms[0]
    return " OR ".join(safe_terms)


def _scoped_metadata_query(fts_query: str, field: str) -> str:
    if field == "title":
        return f"title : ({fts_query})"
    if field == "description":
        return f"description : ({fts_query})"
    return fts_query


def _merge_candidate(bucket: dict[str, SearchResult], result: SearchResult) -> None:
    existing = bucket.get(result.video_id)
    if existing is None or result.score > existing.score:
        bucket[result.video_id] = result


def _phrase_hit(query: str, haystack: str) -> bool:
    normalized = query.strip().strip('"').lower()
    if len(normalized) < 3:
        return False
    return normalized in haystack.lower()


def _video_filter_sql(
    filters: SearchFilters,
    scope_ids: list[str] | None,
    *,
    alias: str,
) -> tuple[str, list[object]]:
    clauses: list[str] = []
    params: list[object] = []

    if scope_ids:
        placeholders = ",".join("?" for _ in scope_ids)
        clauses.append(f"{alias}.scope_id IN ({placeholders})")
        params.extend(scope_ids)

    if filters.after:
        clauses.append(f"{alias}.published_at >= ?")
        params.append(filters.after)
    if filters.before:
        clauses.append(f"{alias}.published_at <= ?")
        params.append(filters.before)
    if filters.min_views is not None:
        clauses.append(f"{alias}.view_count >= ?")
        params.append(filters.min_views)
    if filters.max_views is not None:
        clauses.append(f"{alias}.view_count <= ?")
        params.append(filters.max_views)
    if filters.min_duration is not None:
        clauses.append(f"{alias}.duration_seconds >= ?")
        params.append(filters.min_duration)
    if filters.max_duration is not None:
        clauses.append(f"{alias}.duration_seconds <= ?")
        params.append(filters.max_duration)
    if filters.no_shorts:
        clauses.append(f"{alias}.is_short = 0")
    if filters.only_shorts:
        clauses.append(f"{alias}.is_short = 1")

    if not clauses:
        return "1=1", params
    return " AND ".join(clauses), params


def sqlite_has_fts5(connection: sqlite3.Connection) -> bool:
    try:
        connection.execute("CREATE VIRTUAL TABLE temp.__fts5_probe USING fts5(value)")
        connection.execute("DROP TABLE temp.__fts5_probe")
        return True
    except sqlite3.DatabaseError:
        return False
