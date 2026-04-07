from tubesift.resolver import resolve_scope


def test_resolve_handle() -> None:
    scope = resolve_scope("@Veritasium")
    assert scope.scope_type == "channel"
    assert scope.scope_id == "handle:veritasium"
    assert scope.lookup_url == "https://www.youtube.com/@Veritasium"


def test_resolve_channel_id() -> None:
    scope = resolve_scope("UCYO_jab_esuFRV4b17AJtAw")
    assert scope.scope_id == "channel:UCYO_jab_esuFRV4b17AJtAw"


def test_resolve_playlist_url() -> None:
    scope = resolve_scope("https://www.youtube.com/playlist?list=PL12345")
    assert scope.scope_type == "playlist"
    assert scope.scope_id == "playlist:PL12345"
