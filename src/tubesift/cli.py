from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from pathlib import Path

import typer
from rich.console import Console
from rich.progress import BarColumn, Progress, TaskProgressColumn, TextColumn, TimeElapsedColumn
from rich.table import Table

from tubesift.browser import open_url
from tubesift.config import get_settings
from tubesift.doctor import run_doctor
from tubesift.extractor import Extractor
from tubesift.formatter import (
    print_search_table,
    search_results_to_csv,
    search_results_to_json,
    search_results_to_markdown,
)
from tubesift.resolver import ScopeResolutionError, resolve_scope
from tubesift.search import SearchEngine, SearchFilters, SearchRequest
from tubesift.storage import Storage
from tubesift.utils import format_duration, parse_duration_to_seconds, utc_now_iso, with_timestamp

app = typer.Typer(add_completion=False, no_args_is_help=True)
console = Console()


@dataclass(slots=True)
class FetchSummary:
    scope_id: str
    scope_title: str
    discovered_videos: int
    fetched_videos: int
    created: int
    updated: int
    skipped: int
    errors: int
    error_samples: list[str]
    videos_with_subtitles: int
    subtitle_segments: int


def _open_storage() -> Storage:
    settings = get_settings()
    settings.ensure_data_dirs()
    return Storage(settings.db_path)


def _fetch_scope(
    storage: Storage,
    scope_input: str,
    *,
    subs: str,
    workers: int,
    lang: str | None,
    refresh_recent: int,
    max_videos: int | None,
    cookies_from_browser: str | None,
    cookies: Path | None,
) -> FetchSummary:
    extractor = Extractor(
        cookies_from_browser=cookies_from_browser,
        cookies_file=str(cookies) if cookies else None,
    )
    started_at = utc_now_iso()

    try:
        scope_ref = resolve_scope(scope_input)
    except ScopeResolutionError as exc:
        raise typer.BadParameter(str(exc)) from exc

    scope_meta, video_ids = extractor.list_scope_videos(scope_ref)
    if max_videos is not None:
        video_ids = video_ids[: max(max_videos, 0)]

    existing = storage.scope_video_state(scope_meta.id, video_ids)
    to_fetch: list[str] = []
    skipped_preexisting = 0
    for index, video_id in enumerate(video_ids):
        current = existing.get(video_id)
        if current is None:
            to_fetch.append(video_id)
            continue

        need_subtitles = subs != "none" and current["subtitle_status"] != "fetched"
        refresh_hotset = index < max(refresh_recent, 0)
        if need_subtitles or refresh_hotset:
            to_fetch.append(video_id)
        else:
            skipped_preexisting += 1

    scope_meta.last_synced_at = utc_now_iso()
    scope_meta.video_count = len(video_ids)
    storage.upsert_scope(scope_meta)

    fetched_videos = []
    subtitles: dict[str, list] = {}
    errors: list[str] = []
    max_workers = max(1, workers)

    progress_columns = [
        TextColumn("[cyan]{task.description}"),
        BarColumn(),
        TaskProgressColumn(),
        TimeElapsedColumn(),
    ]

    with Progress(*progress_columns, console=console, transient=True) as progress:
        task_id = progress.add_task(f"fetch {scope_input}", total=len(to_fetch))
        if to_fetch:
            with ThreadPoolExecutor(max_workers=max_workers) as pool:
                futures = {
                    pool.submit(
                        extractor.fetch_video,
                        video_id,
                        scope_meta.id,
                        subs,
                        lang,
                    ): video_id
                    for video_id in to_fetch
                }

                for future in as_completed(futures):
                    video_id = futures[future]
                    try:
                        result = future.result()
                        fetched_videos.append(result.video)
                        if subs != "none":
                            subtitles[result.video.id] = result.subtitle_segments
                    except Exception as exc:
                        errors.append(f"{video_id}: {exc}")
                    progress.advance(task_id)

    created, updated, skipped_fetched = storage.upsert_videos(fetched_videos, scope_meta.title)
    videos_with_subtitles, subtitle_segments = (0, 0)
    if subs != "none":
        videos_with_subtitles, subtitle_segments = storage.upsert_subtitles(subtitles)

    finished_at = utc_now_iso()
    storage.mark_fetch_state(
        scope_meta.id,
        started_at=started_at,
        finished_at=finished_at,
        success=len(errors) == 0,
        error="\n".join(errors[:20]) if errors else None,
    )

    return FetchSummary(
        scope_id=scope_meta.id,
        scope_title=scope_meta.title,
        discovered_videos=len(video_ids),
        fetched_videos=len(to_fetch),
        created=created,
        updated=updated,
        skipped=skipped_preexisting + skipped_fetched,
        errors=len(errors),
        error_samples=errors[:5],
        videos_with_subtitles=videos_with_subtitles,
        subtitle_segments=subtitle_segments,
    )


@app.command()
def fetch(
    scope: str = typer.Argument(..., help="@handle, channel URL/id, or playlist URL"),
    subs: str = typer.Option("none", "--subs", help="none|auto|all"),
    workers: int = typer.Option(8, "--workers", min=1, max=64),
    lang: str | None = typer.Option(None, "--lang", help="Subtitle language filter, e.g. en"),
    refresh_recent: int = typer.Option(30, "--refresh-recent", min=0, help="Refresh last N videos every run"),
    max_videos: int | None = typer.Option(None, "--max-videos", min=1, help="Limit fetched video count"),
    cookies_from_browser: str | None = typer.Option(None, "--cookies-from-browser", help="Browser name for yt-dlp cookies, e.g. chrome"),
    cookies: Path | None = typer.Option(None, "--cookies", help="Path to Netscape cookies.txt file"),
) -> None:
    """Index channel or playlist into local SQLite DB."""
    subs = subs.lower().strip()
    if subs not in {"none", "auto", "all"}:
        raise typer.BadParameter("--subs must be one of: none, auto, all")

    storage = _open_storage()
    try:
        summary = _fetch_scope(
            storage,
            scope,
            subs=subs,
            workers=workers,
            lang=lang,
            refresh_recent=refresh_recent,
            max_videos=max_videos,
            cookies_from_browser=cookies_from_browser,
            cookies=cookies,
        )
    except Exception as exc:
        console.print(f"[red]Fetch failed:[/red] {exc}")
        console.print("[yellow]Tip:[/yellow] install dependencies with `pip install -e .`")
        raise typer.Exit(1)
    finally:
        storage.close()

    _print_fetch_summary(summary)


@app.command()
def sync(
    scopes: list[str] = typer.Argument(None, help="Optional list of scopes to sync"),
    subs: str = typer.Option("none", "--subs", help="none|auto|all"),
    workers: int = typer.Option(8, "--workers", min=1, max=64),
    lang: str | None = typer.Option(None, "--lang", help="Subtitle language filter"),
    refresh_recent: int = typer.Option(30, "--refresh-recent", min=0, help="Refresh last N videos every run"),
    max_videos: int | None = typer.Option(None, "--max-videos", min=1, help="Limit fetched video count"),
    cookies_from_browser: str | None = typer.Option(None, "--cookies-from-browser", help="Browser name for yt-dlp cookies, e.g. chrome"),
    cookies: Path | None = typer.Option(None, "--cookies", help="Path to Netscape cookies.txt file"),
) -> None:
    """Refresh already indexed scopes (or selected scopes)."""
    subs = subs.lower().strip()
    if subs not in {"none", "auto", "all"}:
        raise typer.BadParameter("--subs must be one of: none, auto, all")

    storage = _open_storage()
    try:
        if scopes:
            target_scopes: list[str] = []
            for raw_scope in scopes:
                ids = storage.find_scope_ids(raw_scope)
                if ids:
                    info_row = storage.scope_info(ids[0])
                    if info_row is not None:
                        target_scopes.append(info_row["url"])
                        continue
                target_scopes.append(raw_scope)
        else:
            target_scopes = [row["url"] for row in storage.list_scopes()]

        if not target_scopes:
            console.print("[yellow]No indexed scopes found. Use `tubesift fetch <scope>` first.[/yellow]")
            return

        summaries: list[FetchSummary] = []
        for scope in target_scopes:
            console.print(f"[bold cyan]Sync[/bold cyan] {scope}")
            try:
                summary = _fetch_scope(
                    storage,
                    scope,
                    subs=subs,
                    workers=workers,
                    lang=lang,
                    refresh_recent=refresh_recent,
                    max_videos=max_videos,
                    cookies_from_browser=cookies_from_browser,
                    cookies=cookies,
                )
                summaries.append(summary)
            except Exception as exc:
                console.print(f"[red]Failed to sync {scope}: {exc}[/red]")

        if summaries:
            for summary in summaries:
                _print_fetch_summary(summary)
    finally:
        storage.close()


@app.command()
def search(
    scope_or_query: str = typer.Argument(..., help="Scope or query"),
    maybe_query: str | None = typer.Argument(None, help="Query when scope is provided"),
    all_scopes: bool = typer.Option(False, "--all", help="Search across all local scopes"),
    field: str = typer.Option("any", "--field", help="any|title|description|subtitle"),
    regex: bool = typer.Option(False, "--regex", help="Treat query as regular expression"),
    after: str | None = typer.Option(None, "--after", help="Published date lower bound YYYY-MM-DD"),
    before: str | None = typer.Option(None, "--before", help="Published date upper bound YYYY-MM-DD"),
    min_views: int | None = typer.Option(None, "--min-views"),
    max_views: int | None = typer.Option(None, "--max-views"),
    min_duration: str | None = typer.Option(None, "--min-duration", help="seconds or HH:MM:SS"),
    max_duration: str | None = typer.Option(None, "--max-duration", help="seconds or HH:MM:SS"),
    no_shorts: bool = typer.Option(False, "--no-shorts"),
    only_shorts: bool = typer.Option(False, "--only-shorts"),
    lang: str | None = typer.Option(None, "--lang"),
    sort: str = typer.Option("score", "--sort", help="score|date|views|duration"),
    limit: int = typer.Option(30, "--limit", min=1, max=500),
    open_n: int | None = typer.Option(None, "--open", min=1, help="Open Nth result"),
    as_json: bool = typer.Option(False, "--json"),
    as_csv: bool = typer.Option(False, "--csv"),
    as_markdown: bool = typer.Option(False, "--markdown"),
    output: Path | None = typer.Option(None, "--output", "-o"),
) -> None:
    """Search across local index by scope and query."""
    field = field.lower().strip()
    if field not in {"any", "title", "description", "subtitle"}:
        raise typer.BadParameter("--field must be one of: any, title, description, subtitle")

    sort = sort.lower().strip()
    if sort not in {"score", "date", "views", "duration"}:
        raise typer.BadParameter("--sort must be one of: score, date, views, duration")

    mode_count = int(as_json) + int(as_csv) + int(as_markdown)
    if mode_count > 1:
        raise typer.BadParameter("Use only one export mode: --json OR --csv OR --markdown")

    if maybe_query is None:
        scope = None
        query = scope_or_query
    else:
        scope = None if all_scopes else scope_or_query
        query = maybe_query

    if all_scopes:
        scope = None

    min_duration_seconds = _parse_optional_duration(min_duration)
    max_duration_seconds = _parse_optional_duration(max_duration)

    storage = _open_storage()
    try:
        engine = SearchEngine(storage)
        request = SearchRequest(
            scope=scope,
            query=query,
            field=field,
            regex=regex,
            sort=sort,
            limit=limit,
            filters=SearchFilters(
                after=after,
                before=before,
                min_views=min_views,
                max_views=max_views,
                min_duration=min_duration_seconds,
                max_duration=max_duration_seconds,
                no_shorts=no_shorts,
                only_shorts=only_shorts,
                lang=lang.lower() if lang else None,
            ),
        )
        try:
            results = engine.search(request)
        except ValueError as exc:
            console.print(f"[red]Search failed:[/red] {exc}")
            raise typer.Exit(1)
    finally:
        storage.close()

    if not results:
        console.print("[yellow]No results[/yellow]")
        return

    payload = None
    if as_json:
        payload = search_results_to_json(results)
    elif as_csv:
        payload = search_results_to_csv(results)
    elif as_markdown:
        payload = search_results_to_markdown(results)

    if payload is not None:
        if output:
            output.parent.mkdir(parents=True, exist_ok=True)
            output.write_text(payload, encoding="utf-8")
            console.print(f"[green]Saved[/green] {output}")
        else:
            console.print(payload)
    else:
        print_search_table(console, results)

    if open_n:
        index = open_n - 1
        if not (0 <= index < len(results)):
            raise typer.BadParameter(f"--open {open_n} is out of range")
        url = results[index].url
        ok = open_url(url)
        if ok:
            console.print(f"[green]Opened:[/green] {url}")
        else:
            console.print(f"[red]Could not open:[/red] {url}")


@app.command("list")
def list_scopes() -> None:
    """List local scopes in DB."""
    storage = _open_storage()
    try:
        rows = storage.list_scopes()
    finally:
        storage.close()

    if not rows:
        console.print("[yellow]No scopes indexed yet.[/yellow]")
        return

    table = Table(show_header=True, header_style="bold cyan")
    table.add_column("Scope ID")
    table.add_column("Type")
    table.add_column("Title")
    table.add_column("Videos", justify="right")
    table.add_column("Last Sync")

    for row in rows:
        table.add_row(
            row["id"],
            row["scope_type"],
            row["title"],
            str(row["video_count"] or 0),
            row["last_synced_at"],
        )
    console.print(table)


@app.command()
def info(scope: str = typer.Argument(..., help="Scope ID, handle, URL, or title fragment")) -> None:
    """Show channel or playlist index stats."""
    storage = _open_storage()
    try:
        scope_id = _resolve_scope_id(storage, scope)
        row = storage.scope_info(scope_id)
    finally:
        storage.close()

    if row is None:
        console.print(f"[red]Scope not found:[/red] {scope}")
        raise typer.Exit(1)

    table = Table(show_header=False)
    table.add_row("id", row["id"])
    table.add_row("type", row["scope_type"])
    table.add_row("handle", str(row["handle"] or ""))
    table.add_row("title", row["title"])
    table.add_row("url", row["url"])
    table.add_row("indexed_videos", str(row["indexed_videos"]))
    table.add_row("videos_with_subtitles", str(row["videos_with_subtitles"]))
    table.add_row("earliest_video", str(row["earliest_video"] or "-"))
    table.add_row("latest_video", str(row["latest_video"] or "-"))
    table.add_row("last_synced_at", row["last_synced_at"])
    console.print(table)


@app.command()
def top(
    scope: str = typer.Argument(..., help="Scope ID, handle, URL, or title fragment"),
    limit: int = typer.Option(10, "--limit", min=1, max=100),
) -> None:
    """Show top videos by views."""
    storage = _open_storage()
    try:
        scope_id = _resolve_scope_id(storage, scope)
        rows = storage.top_videos(scope_id, limit=limit)
    finally:
        storage.close()

    if not rows:
        console.print("[yellow]No videos for this scope[/yellow]")
        return

    table = Table(show_header=True, header_style="bold cyan")
    table.add_column("#", justify="right")
    table.add_column("Title")
    table.add_column("Views", justify="right")
    table.add_column("Date")
    table.add_column("Duration", justify="right")

    for idx, row in enumerate(rows, start=1):
        table.add_row(
            str(idx),
            row["title"],
            f"{row['view_count']:,}",
            row["published_at"],
            format_duration(row["duration_seconds"]),
        )
    console.print(table)


@app.command()
def open(
    video_id: str = typer.Argument(..., help="YouTube video id"),
    at: int | None = typer.Option(None, "--at", min=0, help="Timestamp in seconds"),
) -> None:
    """Open video from local DB in browser."""
    storage = _open_storage()
    try:
        row = storage.get_video(video_id)
    finally:
        storage.close()

    url = row["video_url"] if row else f"https://www.youtube.com/watch?v={video_id}"
    url = with_timestamp(url, at)
    ok = open_url(url)
    if ok:
        console.print(f"[green]Opened:[/green] {url}")
    else:
        console.print(f"[red]Could not open:[/red] {url}")


@app.command()
def clear(scope: str = typer.Argument(..., help="Scope ID, handle, URL, or title fragment")) -> None:
    """Delete local index for one scope."""
    storage = _open_storage()
    try:
        scope_id = _resolve_scope_id(storage, scope)
        deleted_scopes, deleted_videos = storage.clear_scope(scope_id)
    finally:
        storage.close()

    if deleted_scopes == 0:
        console.print(f"[yellow]Scope not found:[/yellow] {scope}")
    else:
        console.print(
            f"[green]Deleted[/green] scope={scope_id} videos={deleted_videos}"
        )


@app.command()
def doctor() -> None:
    """Environment and dependency diagnostics."""
    settings = get_settings()
    checks = run_doctor(settings.data_dir, settings.db_path)

    table = Table(show_header=True, header_style="bold cyan")
    table.add_column("Check")
    table.add_column("Status")
    table.add_column("Details")
    for check in checks:
        color = {"ok": "green", "warn": "yellow", "error": "red"}.get(check.status, "white")
        table.add_row(check.name, f"[{color}]{check.status}[/{color}]", check.details)
    console.print(table)


@app.command()
def ui(scope: str | None = typer.Argument(None, help="Optional local scope identifier")) -> None:
    """Launch Textual TUI."""
    storage = _open_storage()
    try:
        scope_id = _resolve_scope_id(storage, scope) if scope else None
        try:
            from tubesift.tui import run_tui
        except Exception as exc:
            console.print(f"[red]TUI is unavailable:[/red] {exc}")
            raise typer.Exit(1)

        run_tui(storage=storage, scope=scope_id)
    finally:
        storage.close()


def _resolve_scope_id(storage: Storage, raw_scope: str) -> str:
    candidates = storage.find_scope_ids(raw_scope)
    if candidates:
        return candidates[0]

    try:
        resolved = resolve_scope(raw_scope)
    except ScopeResolutionError:
        raise typer.BadParameter(f"scope not indexed locally: {raw_scope}")

    candidates = storage.find_scope_ids(resolved.scope_id)
    if candidates:
        return candidates[0]

    raise typer.BadParameter(f"scope not indexed locally: {raw_scope}")


def _parse_optional_duration(value: str | None) -> int | None:
    if value is None:
        return None
    try:
        return parse_duration_to_seconds(value)
    except ValueError as exc:
        raise typer.BadParameter(str(exc)) from exc


def _print_fetch_summary(summary: FetchSummary) -> None:
    table = Table(show_header=False)
    table.add_row("scope", f"{summary.scope_title} ({summary.scope_id})")
    table.add_row("discovered", str(summary.discovered_videos))
    table.add_row("fetched", str(summary.fetched_videos))
    table.add_row("new", str(summary.created))
    table.add_row("updated", str(summary.updated))
    table.add_row("skipped", str(summary.skipped))
    table.add_row("errors", str(summary.errors))
    table.add_row("videos_with_subtitles", str(summary.videos_with_subtitles))
    table.add_row("subtitle_segments", str(summary.subtitle_segments))
    console.print(table)
    if summary.error_samples:
        console.print("[yellow]Sample errors:[/yellow]")
        for item in summary.error_samples:
            console.print(f"- {item}")
