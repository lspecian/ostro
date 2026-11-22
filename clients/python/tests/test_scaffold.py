"""Placeholder test for the ostro-client scaffold (plan 10-04)."""

import pathlib
import sys

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parents[1]))

import ostro_client  # noqa: E402


def test_version():
    assert ostro_client.__version__ == "0.1.0"
