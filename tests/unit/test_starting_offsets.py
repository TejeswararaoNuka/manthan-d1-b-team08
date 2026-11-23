"""STARTING_OFFSETS accepts latest/earliest or Spark's per-partition JSON (replay runbook)."""

import argparse

import pytest

from streamguard.processor.job import parse_starting_offsets


@pytest.mark.parametrize("value", ["latest", "earliest"])
def test_keywords_pass_through(value):
    assert parse_starting_offsets(value) == value


def test_per_partition_json_is_normalised():
    raw = '{"payments.raw": {"0": 1200, "1": -2, "2": -1}}'
    assert parse_starting_offsets(raw) == '{"payments.raw":{"0":1200,"1":-2,"2":-1}}'


@pytest.mark.parametrize(
    "value",
    [
        "newest",
        '{"payments.alerts": {"0": 1}}',
        '{"payments.raw": {}}',
        '{"payments.raw": {"x": 1}}',
        '{"payments.raw": {"0": -3}}',
        '{"payments.raw": {"0": "5"}}',
        '{"payments.raw": {"0": true}}',
    ],
)
def test_invalid_starting_offsets_are_rejected(value):
    with pytest.raises(argparse.ArgumentTypeError):
        parse_starting_offsets(value)
