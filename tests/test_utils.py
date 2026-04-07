from tubesift.utils import extract_video_id, format_duration, parse_duration_to_seconds


def test_format_duration() -> None:
    assert format_duration(59) == "0:59"
    assert format_duration(61) == "1:01"
    assert format_duration(3661) == "1:01:01"


def test_parse_duration_to_seconds() -> None:
    assert parse_duration_to_seconds("42") == 42
    assert parse_duration_to_seconds("05:10") == 310
    assert parse_duration_to_seconds("01:05:10") == 3910


def test_extract_video_id() -> None:
    assert extract_video_id("dQw4w9WgXcQ") == "dQw4w9WgXcQ"
    assert extract_video_id("https://youtu.be/dQw4w9WgXcQ") == "dQw4w9WgXcQ"
    assert extract_video_id("https://www.youtube.com/watch?v=dQw4w9WgXcQ") == "dQw4w9WgXcQ"
