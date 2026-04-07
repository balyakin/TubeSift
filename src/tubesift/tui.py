from __future__ import annotations

import os
import subprocess
import sys

from textual.app import App, ComposeResult
from textual.binding import Binding
from textual.containers import Horizontal, Vertical
from textual.widgets import DataTable, Footer, Header, Input, ListItem, ListView, Static

from tubesift.browser import open_url
from tubesift.search import SearchEngine, SearchFilters, SearchRequest, SearchResult
from tubesift.storage import Storage
from tubesift.utils import format_duration, truncate


class TubeSiftApp(App):
    BINDINGS = [
        Binding("enter", "open_selected", "Open"),
        Binding("/", "focus_search", "Search"),
        Binding("tab", "cycle_focus", "Tab"),
        Binding("s", "cycle_sort", "Sort"),
        Binding("f", "toggle_no_shorts", "Filter"),
        Binding("y", "copy_url", "Copy URL"),
        Binding("t", "toggle_preview", "Preview"),
        Binding("q", "quit", "Quit"),
    ]

    CSS = """
    #main {
        layout: horizontal;
        height: 1fr;
    }

    #scope-pane {
        width: 28;
        border: round #3f5f7f;
    }

    #results-pane {
        width: 2fr;
        border: round #3f7f5f;
    }

    #preview-pane {
        width: 1fr;
        border: round #7f6a3f;
        padding: 1;
    }

    #query-input {
        margin: 1;
    }

    #scope-title {
        content-align: center middle;
        height: 3;
        text-style: bold;
    }

    #preview {
        height: 1fr;
    }
    """

    def __init__(self, storage: Storage, initial_scope: str | None = None) -> None:
        super().__init__()
        self.storage = storage
        self.search_engine = SearchEngine(storage)
        self.scope_rows = storage.list_scopes()
        self.scope_ids = [row["id"] for row in self.scope_rows]
        self.initial_scope = initial_scope
        self.current_scope: str | None = None
        self.current_sort = "score"
        self.preview_mode = "metadata"
        self.no_shorts = False
        self.results: list[SearchResult] = []

    def compose(self) -> ComposeResult:
        yield Header(show_clock=False)
        with Horizontal(id="main"):
            with Vertical(id="scope-pane"):
                yield Static("Scopes", id="scope-title")
                items = [ListItem(Static(row["title"])) for row in self.scope_rows]
                yield ListView(*items, id="scope-list")

            with Vertical(id="results-pane"):
                yield Input(placeholder="Search…", id="query-input")
                yield DataTable(id="results-table")

            with Vertical(id="preview-pane"):
                yield Static("Preview", id="preview")
        yield Footer()

    def on_mount(self) -> None:
        table = self.query_one("#results-table", DataTable)
        table.cursor_type = "row"
        table.add_columns("#", "Title", "Channel", "Date", "Views", "Dur", "Match", "Time")

        if self.scope_ids:
            initial_index = 0
            if self.initial_scope:
                try:
                    initial_index = self.scope_ids.index(self.initial_scope)
                except ValueError:
                    initial_index = 0
            self.current_scope = self.scope_ids[initial_index]
            scope_list = self.query_one("#scope-list", ListView)
            scope_list.index = initial_index

        self._refresh_results()

    def on_input_changed(self, event: Input.Changed) -> None:
        if event.input.id != "query-input":
            return
        self._refresh_results()

    def on_list_view_highlighted(self, event: ListView.Highlighted) -> None:
        if event.list_view.id != "scope-list":
            return
        index = getattr(event, "index", None)
        if index is None:
            return
        if 0 <= index < len(self.scope_ids):
            self.current_scope = self.scope_ids[index]
            self._refresh_results()

    def on_data_table_row_highlighted(self, event: DataTable.RowHighlighted) -> None:
        if event.data_table.id != "results-table":
            return
        self._update_preview(event.cursor_row)

    def action_open_selected(self) -> None:
        result = self._selected_result()
        if not result:
            self.notify("No result selected", severity="warning")
            return
        if open_url(result.url):
            self.notify("Opened in browser")
        else:
            self.notify("Could not open URL", severity="error")

    def action_focus_search(self) -> None:
        self.query_one("#query-input", Input).focus()

    def action_cycle_focus(self) -> None:
        query = self.query_one("#query-input", Input)
        table = self.query_one("#results-table", DataTable)
        scope = self.query_one("#scope-list", ListView)
        chain = [query, table, scope]
        focused = self.focused
        try:
            idx = chain.index(focused)
        except ValueError:
            idx = 0
        chain[(idx + 1) % len(chain)].focus()

    def action_cycle_sort(self) -> None:
        order = ["score", "date", "views", "duration"]
        next_idx = (order.index(self.current_sort) + 1) % len(order)
        self.current_sort = order[next_idx]
        self.notify(f"Sort: {self.current_sort}")
        self._refresh_results()

    def action_toggle_no_shorts(self) -> None:
        self.no_shorts = not self.no_shorts
        state = "enabled" if self.no_shorts else "disabled"
        self.notify(f"No-shorts filter {state}")
        self._refresh_results()

    def action_copy_url(self) -> None:
        result = self._selected_result()
        if not result:
            self.notify("No result selected", severity="warning")
            return
        ok = _copy_to_clipboard(result.url)
        if ok:
            self.notify("URL copied")
        else:
            self.notify("Clipboard is not available", severity="warning")

    def action_toggle_preview(self) -> None:
        self.preview_mode = "subtitle" if self.preview_mode == "metadata" else "metadata"
        self.notify(f"Preview mode: {self.preview_mode}")
        row = self.query_one("#results-table", DataTable).cursor_row
        self._update_preview(row)

    def _refresh_results(self) -> None:
        query_value = self.query_one("#query-input", Input).value.strip()
        request = SearchRequest(
            scope=self.current_scope,
            query=query_value or None,
            field="any",
            regex=False,
            sort=self.current_sort,
            limit=200,
            filters=SearchFilters(no_shorts=self.no_shorts),
        )
        try:
            self.results = self.search_engine.search(request)
        except Exception as exc:
            self.results = []
            self.notify(str(exc), severity="error")

        table = self.query_one("#results-table", DataTable)
        table.clear()
        for idx, result in enumerate(self.results, start=1):
            timestamp = ""
            if result.timestamp_seconds is not None:
                timestamp = format_duration(int(result.timestamp_seconds))
            table.add_row(
                str(idx),
                truncate(result.title, 48),
                truncate(result.channel, 20),
                result.published_at,
                f"{result.view_count:,}",
                format_duration(result.duration_seconds),
                result.match_type,
                timestamp,
            )

        if self.results:
            table.move_cursor(row=0)
            self._update_preview(0)
        else:
            self.query_one("#preview", Static).update("No results")

    def _selected_result(self) -> SearchResult | None:
        table = self.query_one("#results-table", DataTable)
        row = table.cursor_row
        if row < 0 or row >= len(self.results):
            return None
        return self.results[row]

    def _update_preview(self, row: int) -> None:
        if row < 0 or row >= len(self.results):
            return
        result = self.results[row]
        if self.preview_mode == "subtitle":
            snippet = result.snippet or "No subtitle snippet"
            payload = (
                f"[b]{result.title}[/b]\n\n"
                f"URL: {result.url}\n"
                f"Match: {result.match_type}\n"
                f"Time: {'' if result.timestamp_seconds is None else format_duration(int(result.timestamp_seconds))}\n\n"
                f"{snippet}"
            )
        else:
            payload = (
                f"[b]{result.title}[/b]\n\n"
                f"Channel: {result.channel}\n"
                f"Date: {result.published_at}\n"
                f"Views: {result.view_count:,}\n"
                f"Duration: {format_duration(result.duration_seconds)}\n"
                f"Match: {result.match_type}\n\n"
                f"{result.snippet}"
            )
        self.query_one("#preview", Static).update(payload)


def run_tui(storage: Storage, scope: str | None = None) -> None:
    app = TubeSiftApp(storage=storage, initial_scope=scope)
    app.run()


def _copy_to_clipboard(text: str) -> bool:
    try:
        if sys.platform == "darwin":
            subprocess.run(["pbcopy"], input=text.encode("utf-8"), check=False)
            return True
        if os.name == "nt":
            subprocess.run(["clip"], input=text.encode("utf-16le"), check=False)
            return True
        for cmd in (["xclip", "-selection", "clipboard"], ["xsel", "--clipboard", "--input"]):
            proc = subprocess.run(cmd, input=text.encode("utf-8"), check=False)
            if proc.returncode == 0:
                return True
    except Exception:
        return False
    return False
