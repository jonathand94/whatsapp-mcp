"""list_messages takes since/until as people and programs write them.

A 13-digit timestamp is milliseconds (JavaScript's Date.now(), WhatsApp's
*_ms fields). Until 2026-10-21 it was read as seconds: a date tens of
thousands of years out, so the filter silently matched nothing.
"""

from __future__ import annotations

from datetime import datetime

import pytest

from main import _to_unix


def test_every_unit_of_timestamp_is_the_same_moment():
    assert _to_unix("1792557000", end_of_day=False) == 1792557000
    assert _to_unix("1792557000.9", end_of_day=False) == 1792557000
    assert _to_unix("1792557000123", end_of_day=False) == 1792557000  # ms
    assert _to_unix("1792557000123456", end_of_day=False) == 1792557000  # µs
    assert _to_unix("1792557000123456789", end_of_day=False) == 1792557000  # ns


def test_a_bare_date_as_upper_bound_is_the_whole_day():
    start = _to_unix("2026-10-21", end_of_day=False)
    end = _to_unix("2026-10-21", end_of_day=True)
    assert start == int(datetime(2026, 10, 21).timestamp())
    assert end - start == 86399


def test_date_and_time():
    assert _to_unix("2026-10-21 14:30", end_of_day=True) == int(datetime(2026, 10, 21, 14, 30).timestamp())


@pytest.mark.parametrize("bad", ["ayer", "21/10/2026", "1.2.3", "-5", "²", "1" * 400])
def test_unrecognised_dates_say_what_works(bad):
    with pytest.raises(ValueError, match="segundos o milisegundos"):
        _to_unix(bad, end_of_day=False)


def test_list_chats_refuses_impossible_pages():
    """offset=-5 used to read as 0 and limit=0 as an empty page, silently."""
    import asyncio

    import main

    with pytest.raises(ValueError, match="offset"):
        asyncio.run(main.list_chats(offset=-5))
    with pytest.raises(ValueError, match="limit"):
        asyncio.run(main.list_chats(limit=0))
    with pytest.raises(ValueError, match="limit"):
        asyncio.run(main.list_messages(chat_jid="5215550000001@s.whatsapp.net", limit=0))
