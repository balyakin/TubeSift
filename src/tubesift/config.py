from __future__ import annotations

import os
from dataclasses import dataclass
from functools import lru_cache
from pathlib import Path


@dataclass(slots=True)
class Settings:
    data_dir: Path
    db_path: Path
    logs_dir: Path
    exports_dir: Path
    cache_dir: Path

    def ensure_data_dirs(self) -> None:
        self.data_dir.mkdir(parents=True, exist_ok=True)
        self.logs_dir.mkdir(parents=True, exist_ok=True)
        self.exports_dir.mkdir(parents=True, exist_ok=True)
        self.cache_dir.mkdir(parents=True, exist_ok=True)


@lru_cache(maxsize=1)
def get_settings() -> Settings:
    root = Path(os.environ.get("TUBESIFT_HOME", "~/.tubesift")).expanduser()
    return Settings(
        data_dir=root,
        db_path=root / "tubesift.db",
        logs_dir=root / "logs",
        exports_dir=root / "exports",
        cache_dir=root / "cache",
    )
