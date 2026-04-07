import pytest

from tubesift.search import SearchEngine, SearchRequest
from tubesift.storage import Storage


def test_invalid_regex_returns_value_error(tmp_path) -> None:
    storage = Storage(tmp_path / "tubesift.db")
    try:
        engine = SearchEngine(storage)
        with pytest.raises(ValueError, match="invalid regex"):
            engine.search(
                SearchRequest(
                    scope=None,
                    query="(",
                    regex=True,
                )
            )
    finally:
        storage.close()
