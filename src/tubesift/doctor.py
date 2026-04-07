from __future__ import annotations

import os
import sqlite3
import sys
from dataclasses import dataclass
from pathlib import Path


@dataclass(slots=True)
class DoctorCheck:
    name: str
    status: str
    details: str


def run_doctor(data_dir: Path, db_path: Path) -> list[DoctorCheck]:
    checks: list[DoctorCheck] = []
    checks.append(
        DoctorCheck(
            name="Python",
            status="ok" if sys.version_info >= (3, 11) else "warn",
            details=sys.version.split()[0],
        )
    )

    try:
        import yt_dlp  # noqa: F401

        checks.append(DoctorCheck(name="yt-dlp", status="ok", details="imported"))
    except Exception as exc:
        checks.append(DoctorCheck(name="yt-dlp", status="error", details=str(exc)))

    try:
        import textual  # noqa: F401

        checks.append(DoctorCheck(name="textual", status="ok", details="imported"))
    except Exception as exc:
        checks.append(DoctorCheck(name="textual", status="warn", details=str(exc)))

    try:
        conn = sqlite3.connect(":memory:")
        conn.execute("CREATE VIRTUAL TABLE temp.t USING fts5(body)")
        conn.execute("DROP TABLE temp.t")
        conn.close()
        checks.append(DoctorCheck(name="SQLite FTS5", status="ok", details="enabled"))
    except Exception as exc:
        checks.append(DoctorCheck(name="SQLite FTS5", status="error", details=str(exc)))

    try:
        data_dir.mkdir(parents=True, exist_ok=True)
        test_file = data_dir / ".write-test"
        test_file.write_text("ok", encoding="utf-8")
        test_file.unlink(missing_ok=True)
        checks.append(DoctorCheck(name="Data dir", status="ok", details=str(data_dir)))
    except Exception as exc:
        checks.append(DoctorCheck(name="Data dir", status="error", details=str(exc)))

    db_parent = db_path.parent
    if db_path.exists() or os.access(db_parent, os.W_OK):
        checks.append(DoctorCheck(name="DB path", status="ok", details=str(db_path)))
    else:
        checks.append(
            DoctorCheck(name="DB path", status="error", details=f"no write access: {db_path}")
        )

    return checks
