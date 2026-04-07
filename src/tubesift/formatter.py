from __future__ import annotations

import csv
import io
import json

from rich.console import Console
from rich.table import Table

from tubesift.search import SearchResult
from tubesift.utils import format_duration, truncate


def print_search_table(console: Console, results: list[SearchResult]) -> None:
    table = Table(show_header=True, header_style="bold cyan")
    table.add_column("#", justify="right", no_wrap=True)
    table.add_column("Title", min_width=22)
    table.add_column("Channel", min_width=16)
    table.add_column("Date", no_wrap=True)
    table.add_column("Views", justify="right")
    table.add_column("Duration", justify="right")
    table.add_column("Match", no_wrap=True)
    table.add_column("Time", no_wrap=True)
    table.add_column("Snippet", min_width=28)

    for idx, result in enumerate(results, start=1):
        timestamp = ""
        if result.timestamp_seconds is not None:
            timestamp = format_duration(int(result.timestamp_seconds))

        table.add_row(
            str(idx),
            truncate(result.title, 60),
            truncate(result.channel, 28),
            result.published_at,
            f"{result.view_count:,}",
            format_duration(result.duration_seconds),
            result.match_type,
            timestamp,
            truncate(result.snippet.replace("\n", " "), 120),
        )

    console.print(table)


def search_results_to_json(results: list[SearchResult]) -> str:
    payload = [_to_record(result) for result in results]
    return json.dumps(payload, ensure_ascii=False, indent=2)


def search_results_to_csv(results: list[SearchResult]) -> str:
    output = io.StringIO()
    records = [_to_record(result) for result in results]
    fieldnames = list(records[0].keys()) if records else [
        "index",
        "video_id",
        "title",
        "channel",
        "published_at",
        "view_count",
        "duration_seconds",
        "match_type",
        "timestamp_seconds",
        "snippet",
        "url",
        "score",
    ]
    writer = csv.DictWriter(output, fieldnames=fieldnames)
    writer.writeheader()
    for record in records:
        writer.writerow(record)
    return output.getvalue()


def search_results_to_markdown(results: list[SearchResult]) -> str:
    headers = [
        "#",
        "video_id",
        "title",
        "channel",
        "published_at",
        "view_count",
        "duration_seconds",
        "match_type",
        "timestamp_seconds",
        "snippet",
        "url",
    ]
    lines = ["| " + " | ".join(headers) + " |", "|" + "|".join(["---"] * len(headers)) + "|"]

    for idx, result in enumerate(results, start=1):
        row = [
            str(idx),
            result.video_id,
            result.title.replace("|", "\\|"),
            result.channel.replace("|", "\\|"),
            result.published_at,
            str(result.view_count),
            str(result.duration_seconds),
            result.match_type,
            "" if result.timestamp_seconds is None else f"{result.timestamp_seconds:.3f}",
            result.snippet.replace("|", "\\|").replace("\n", " "),
            result.url,
        ]
        lines.append("| " + " | ".join(row) + " |")
    return "\n".join(lines)


def _to_record(result: SearchResult) -> dict[str, object]:
    return {
        "video_id": result.video_id,
        "title": result.title,
        "channel": result.channel,
        "published_at": result.published_at,
        "view_count": result.view_count,
        "duration_seconds": result.duration_seconds,
        "match_type": result.match_type,
        "timestamp_seconds": result.timestamp_seconds,
        "snippet": result.snippet,
        "url": result.url,
        "score": round(result.score, 4),
    }
