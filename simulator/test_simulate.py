import base64
import json
import re
import uuid

import simulate


UUID_RE = re.compile(r"^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$")


def decode_cx(cx):
    """Decode a tp2 cx field back to the self-describing contexts wrapper."""
    # tp2 cx is base64url without padding; re-pad before decoding.
    pad = "=" * (-len(cx) % 4)
    raw = base64.urlsafe_b64decode(cx + pad)
    return json.loads(raw)


def test_encode_cx_roundtrip():
    contexts = [
        {
            "schema": "iglu:com.snowplowanalytics.snowplow/web_page/jsonschema/1-0-0",
            "data": {"id": "abc"},
        }
    ]
    cx = simulate.encode_cx(contexts)
    decoded = decode_cx(cx)
    assert decoded["schema"] == "iglu:com.snowplowanalytics.snowplow/contexts/jsonschema/1-0-0"
    assert decoded["data"] == contexts


def test_page_view_attaches_web_page_context():
    ev = simulate.page_view("duid-1", "sid-1", 1, "home")
    assert "cx" in ev
    decoded = decode_cx(ev["cx"])
    [web_page] = decoded["data"]
    assert web_page["schema"].startswith("iglu:com.snowplowanalytics.snowplow/web_page/jsonschema/1-")
    assert UUID_RE.match(web_page["data"]["id"])


def test_page_view_id_is_returned_for_reuse():
    ev, page_view_id = simulate.page_view_with_id("duid-1", "sid-1", 1, "home")
    decoded = decode_cx(ev["cx"])
    assert decoded["data"][0]["data"]["id"] == page_view_id


def test_struct_event_reuses_given_page_view_id():
    pv_id = str(uuid.uuid4())
    ev = simulate.struct_event(
        "duid-1", "sid-1", 1, "cart", "add_to_cart", page_view_id=pv_id
    )
    decoded = decode_cx(ev["cx"])
    assert decoded["data"][0]["data"]["id"] == pv_id


import asyncio
import pytest

from simulate import RateRegulator


@pytest.mark.asyncio
async def test_rate_regulator_enforces_concurrency():
    """With sessions_per_min effectively infinite, concurrency caps active slots."""
    reg = RateRegulator(sessions_per_min=6000, concurrency=2)
    await reg.start()
    slot_a = await reg.acquire()
    slot_b = await reg.acquire()

    # Third acquire must not complete while two slots are held.
    third = asyncio.create_task(reg.acquire())
    done, _ = await asyncio.wait({third}, timeout=0.1)
    assert not done
    reg.release(slot_a)
    slot_c = await asyncio.wait_for(third, timeout=0.2)
    reg.release(slot_b)
    reg.release(slot_c)
    await reg.stop()


@pytest.mark.asyncio
async def test_rate_regulator_respects_sessions_per_min():
    """Interval between permits matches 60 / sessions_per_min (within jitter)."""
    reg = RateRegulator(sessions_per_min=600, concurrency=10)  # 10/sec -> 0.1s interval
    await reg.start()
    loop = asyncio.get_running_loop()
    t0 = loop.time()
    slots = []
    for _ in range(5):
        slots.append(await reg.acquire())
    elapsed = loop.time() - t0
    # 5 permits at 10/sec ≈ 0.4 s minimum (first permit is immediate).
    assert 0.3 <= elapsed <= 0.8, elapsed
    for s in slots:
        reg.release(s)
    await reg.stop()


import time as _time

def test_base_event_default_dtm_matches_stm_now():
    t0 = int(_time.time() * 1000)
    ev = simulate.base_event("u", "s", 1)
    t1 = int(_time.time() * 1000)
    assert t0 <= int(ev["dtm"]) <= t1
    assert t0 <= int(ev["stm"]) <= t1
    # default: dtm and stm are both "now", within a millisecond of each other
    assert abs(int(ev["dtm"]) - int(ev["stm"])) < 5


def test_base_event_backdated_dtm_keeps_stm_now():
    past_ms = int(_time.time() * 1000) - 30_000  # 30 s ago
    t0 = int(_time.time() * 1000)
    ev = simulate.base_event("u", "s", 1, dtm_ms=past_ms)
    t1 = int(_time.time() * 1000)
    assert int(ev["dtm"]) == past_ms
    assert t0 <= int(ev["stm"]) <= t1
    # stm − dtm ≈ 30 s, the gap enrich uses for derived_tstamp
    assert 29_000 < int(ev["stm"]) - int(ev["dtm"]) < 31_000
