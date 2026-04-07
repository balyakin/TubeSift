from tubesift.extractor import Extractor


def test_extractor_ignores_external_ytdlp_config() -> None:
    # ARRANGE
    extractor = Extractor()

    # ACT
    ignore_config = extractor._base_opts["ignoreconfig"]

    # ASSERT
    assert ignore_config is True


def test_extractor_ignores_missing_formats_for_metadata_only_mode() -> None:
    # ARRANGE
    extractor = Extractor()

    # ACT
    ignore_no_formats_error = extractor._base_opts["ignore_no_formats_error"]

    # ASSERT
    assert ignore_no_formats_error is True
