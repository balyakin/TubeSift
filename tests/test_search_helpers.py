from tubesift.search import _regex_hint_query, prepare_fts_query


def test_prepare_fts_query_simple_terms() -> None:
    assert prepare_fts_query("black hole") == "black AND hole"


def test_prepare_fts_query_keeps_phrase() -> None:
    assert prepare_fts_query('"linear algebra"') == '"linear algebra"'


def test_prepare_fts_query_sanitizes_symbols() -> None:
    assert prepare_fts_query("state-of-the-art") == '"state-of-the-art"'


def test_regex_hint_query_extracts_terms() -> None:
    assert _regex_hint_query(r"regex|automata") == "regex OR automata"
