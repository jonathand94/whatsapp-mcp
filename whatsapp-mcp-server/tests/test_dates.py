"""list_messages takes since/until as people and programs write them.

A 13-digit timestamp is milliseconds (JavaScript's Date.now(), WhatsApp's
*_ms fields). Until 2026-10-21 it was read as seconds: a date tens of
thousands of years out, so the filter silently matched nothing.
"""

from __future__ import annotations

from datetime import datetime

import pytest

from main import _to_unix


def test_seconds_and_milliseconds_are_the_same_moment():
    assert _to_unix("1792557000", end_of_day=False) == 1792557000
    assert _to_unix("1792557000123", end_of_day=False) == 1792557000
    assert _to_unix("1792557000.9", end_of_day=False) == 1792557000


def test_a_bare_date_as_upper_bound_is_the_whole_day():
    start = _to_unix("2026-10-21", end_of_day=False)
    end = _to_unix("2026-10-21", end_of_day=True)
    assert start == int(datetime(2026, 10, 21).timestamp())
    assert end - start == 86399


def test_date_and_time():
    assert _to_unix("2026-10-21 14:30", end_of_day=True) == int(datetime(2026, 10, 21, 14, 30).timestamp())


@pytest.mark.parametrize("bad", ["ayer", "21/10/2026", "1.2.3", "-5"])
def test_unrecognised_dates_say_what_works(bad):
    with pytest.raises(ValueError, match="segundos o milisegundos"):
        _to_unix(bad, end_of_day=False)
