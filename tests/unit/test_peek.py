from streamguard.producer.peek import fmt_ts, last_n, render, start_offsets


def test_start_offsets_read_the_last_n_or_everything_available():
    marks = {0: (0, 100), 1: (95, 100), 2: (5, 5)}
    assert start_offsets(marks, 10) == {0: 90, 1: 95}


def test_last_n_returns_newest_messages_oldest_first_across_partitions():
    records = [(3000, 0, 7, None, b"c"), (1000, 1, 2, None, b"a"), (2000, 0, 6, None, b"b")]
    assert [r[4] for r in last_n(records, 2)] == [b"b", b"c"]
    assert last_n(records, 0) == []


def test_render_prints_text_or_base64_and_timestamps_as_iso():
    assert render(b'{"a": 1}') == '{"a": 1}'
    assert render(b"\xff\x00") == "base64:/wA="
    assert render(None) == "null"
    assert fmt_ts(0) == "1970-01-01T00:00:00.000Z"
