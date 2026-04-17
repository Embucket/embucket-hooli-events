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


def test_page_ping_has_pp_event_type_and_web_page_context():
    pv_id = "11111111-2222-3333-4444-555555555555"
    ev = simulate.page_ping("u", "s", 1, pv_id,
                            url="https://hooli-events.com/",
                            title="Hooli Events",
                            pp_xoff=(0, 100),
                            pp_yoff=(0, 200))
    assert ev["e"] == "pp"
    assert ev["url"] == "https://hooli-events.com/"
    assert ev["page"] == "Hooli Events"
    assert ev["pp_mix"] == "0" and ev["pp_max"] == "100"
    assert ev["pp_miy"] == "0" and ev["pp_may"] == "200"
    decoded = decode_cx(ev["cx"])
    [web_page] = decoded["data"]
    assert web_page["data"]["id"] == pv_id


def test_page_ping_backdated_dtm_preserves_stm_now():
    now_ms = int(_time.time() * 1000)
    past_ms = now_ms - 20_000
    ev = simulate.page_ping("u", "s", 1, "pvid",
                            url="https://x/", title="x",
                            dtm_ms=past_ms)
    assert int(ev["dtm"]) == past_ms
    assert int(ev["stm"]) >= now_ms


UNSTRUCT_WRAPPER = "iglu:com.snowplowanalytics.snowplow/unstruct_event/jsonschema/1-0-0"


def test_unstruct_event_wraps_payload_in_self_describing_envelope():
    ev = simulate.unstruct_event(
        "u", "s", 1,
        ue_schema="iglu:com.example/test/jsonschema/1-0-0",
        ue_data={"foo": "bar"},
        page_view_id="pvid",
    )
    assert ev["e"] == "ue"
    pad = "=" * (-len(ev["ue_px"]) % 4)
    decoded = json.loads(base64.urlsafe_b64decode(ev["ue_px"] + pad))
    assert decoded["schema"] == UNSTRUCT_WRAPPER
    assert decoded["data"]["schema"] == "iglu:com.example/test/jsonschema/1-0-0"
    assert decoded["data"]["data"] == {"foo": "bar"}
    # web_page context attached when pv_id passed
    decoded_cx = decode_cx(ev["cx"])
    assert decoded_cx["data"][0]["data"]["id"] == "pvid"


def test_unstruct_event_omits_cx_when_no_pv_id():
    ev = simulate.unstruct_event("u", "s", 1,
                                 ue_schema="iglu:com.example/x/jsonschema/1-0-0",
                                 ue_data={})
    assert "cx" not in ev


def test_link_click_shape():
    ev = simulate.link_click("u", "s", 1, "pvid",
                             target_url="https://hooli-events.com/events.html",
                             element_id="featured-0")
    pad = "=" * (-len(ev["ue_px"]) % 4)
    decoded = json.loads(base64.urlsafe_b64decode(ev["ue_px"] + pad))
    assert decoded["data"]["schema"] == "iglu:com.snowplowanalytics.snowplow/link_click/jsonschema/1-0-1"
    assert decoded["data"]["data"]["targetUrl"] == "https://hooli-events.com/events.html"
    assert decoded["data"]["data"]["elementId"] == "featured-0"


def test_submit_form_shape():
    ev = simulate.submit_form("u", "s", 1, "pvid",
                              form_id="checkout",
                              elements=[{"name": "email", "value": "user@example.com",
                                         "nodeName": "INPUT", "type": "email"}])
    pad = "=" * (-len(ev["ue_px"]) % 4)
    decoded = json.loads(base64.urlsafe_b64decode(ev["ue_px"] + pad))
    assert decoded["data"]["schema"] == "iglu:com.snowplowanalytics.snowplow/submit_form/jsonschema/1-0-0"
    assert decoded["data"]["data"]["formId"] == "checkout"
    assert decoded["data"]["data"]["elements"][0]["name"] == "email"


def test_focus_form_shape():
    ev = simulate.focus_form("u", "s", 1, "pvid",
                             form_id="checkout", element_id="email")
    pad = "=" * (-len(ev["ue_px"]) % 4)
    decoded = json.loads(base64.urlsafe_b64decode(ev["ue_px"] + pad))
    assert decoded["data"]["schema"] == "iglu:com.snowplowanalytics.snowplow/focus_form/jsonschema/1-0-0"
    assert decoded["data"]["data"]["formId"] == "checkout"
    assert decoded["data"]["data"]["elementId"] == "email"
    assert decoded["data"]["data"]["nodeName"] == "INPUT"


def test_change_form_shape():
    ev = simulate.change_form("u", "s", 1, "pvid",
                              form_id="cart", element_id="qty",
                              new_value="3", node_name="INPUT", type_="number")
    pad = "=" * (-len(ev["ue_px"]) % 4)
    decoded = json.loads(base64.urlsafe_b64decode(ev["ue_px"] + pad))
    assert decoded["data"]["schema"] == "iglu:com.snowplowanalytics.snowplow/change_form/jsonschema/1-0-0"
    assert decoded["data"]["data"]["value"] == "3"
    assert decoded["data"]["data"]["type"] == "number"


@pytest.mark.asyncio
async def test_get_or_create_user_creates_new_when_pool_empty():
    simulate.USERS.clear()
    uid, sidx = await simulate.get_or_create_user(new_prob=0.0)
    assert sidx == 1
    assert uid in simulate.USERS
    assert simulate.USERS[uid] == 1


@pytest.mark.asyncio
async def test_get_or_create_user_returns_existing_with_new_prob_zero():
    simulate.USERS.clear()
    simulate.USERS["pre-existing-uid"] = 3
    uid, sidx = await simulate.get_or_create_user(new_prob=0.0)
    assert uid == "pre-existing-uid"
    assert sidx == 4
    assert simulate.USERS[uid] == 4


@pytest.mark.asyncio
async def test_get_or_create_user_creates_new_with_new_prob_one():
    simulate.USERS.clear()
    simulate.USERS["pre-existing-uid"] = 3
    uid, sidx = await simulate.get_or_create_user(new_prob=1.0)
    assert uid != "pre-existing-uid"
    assert sidx == 1


@pytest.mark.asyncio
async def test_get_or_create_user_evicts_oldest_at_cap(monkeypatch):
    simulate.USERS.clear()
    monkeypatch.setattr(simulate, "USER_POOL_MAX", 3)
    for _ in range(5):
        await simulate.get_or_create_user(new_prob=1.0)
    assert len(simulate.USERS) == 3


import httpx as _httpx


def _collect_calls():
    """Return (transport, calls) where calls is a list of sent JSON payloads."""
    calls = []

    def handler(request):
        calls.append(request.read())
        return _httpx.Response(200)

    return _httpx.MockTransport(handler), calls


@pytest.mark.asyncio
async def test_simulate_session_async_respects_max_events(monkeypatch):
    """Hard cap stays enforced even under pathological ping draws."""
    monkeypatch.setattr(simulate, "MAX_EVENTS_PER_SESSION", 5)
    monkeypatch.setattr(simulate, "BOUNCE_PROBABILITY", 0.0)
    monkeypatch.setattr(simulate, "INTERACTION_PROBABILITY_PER_PAGE", 0.0)
    # Force very-long engagement and always-continue to drive events upward
    monkeypatch.setattr(simulate, "CONTINUE_PAGE_PROBABILITY", 1.0)
    monkeypatch.setattr(simulate.random, "expovariate", lambda lam: 30.0)

    transport, calls = _collect_calls()
    async with _httpx.AsyncClient(transport=transport) as client:
        events = await simulate.simulate_session_async(client, "http://collector")
    assert events <= 5
    assert len(calls) == events


@pytest.mark.asyncio
async def test_simulate_session_async_bounce_is_single_event(monkeypatch):
    monkeypatch.setattr(simulate, "BOUNCE_PROBABILITY", 1.0)
    transport, calls = _collect_calls()
    async with _httpx.AsyncClient(transport=transport) as client:
        events = await simulate.simulate_session_async(client, "http://collector")
    assert events == 1  # one page_view, no pings, no interactions, no continuation
    decoded = json.loads(calls[0])
    assert decoded["data"][0]["e"] == "pv"


@pytest.mark.asyncio
async def test_simulate_session_async_pings_carry_parent_pv_id(monkeypatch):
    """All pings on a page share the parent page_view's web_page context id."""
    monkeypatch.setattr(simulate, "BOUNCE_PROBABILITY", 0.0)
    monkeypatch.setattr(simulate, "CONTINUE_PAGE_PROBABILITY", 0.0)   # single page session
    monkeypatch.setattr(simulate, "INTERACTION_PROBABILITY_PER_PAGE", 0.0)
    monkeypatch.setattr(simulate.random, "expovariate", lambda lam: 3.0)  # 3 pings

    transport, calls = _collect_calls()
    async with _httpx.AsyncClient(transport=transport) as client:
        await simulate.simulate_session_async(client, "http://collector")

    events = [json.loads(c)["data"][0] for c in calls]
    pvs = [e for e in events if e["e"] == "pv"]
    pps = [e for e in events if e["e"] == "pp"]
    assert len(pvs) == 1 and len(pps) == 3
    pv_id = decode_cx(pvs[0]["cx"])["data"][0]["data"]["id"]
    for pp in pps:
        assert decode_cx(pp["cx"])["data"][0]["data"]["id"] == pv_id


@pytest.mark.asyncio
async def test_simulate_session_async_ping_dtms_are_spaced(monkeypatch):
    """Ping dtm values are 10s-spaced (in the past) relative to the page view's dtm."""
    monkeypatch.setattr(simulate, "BOUNCE_PROBABILITY", 0.0)
    monkeypatch.setattr(simulate, "CONTINUE_PAGE_PROBABILITY", 0.0)
    monkeypatch.setattr(simulate, "INTERACTION_PROBABILITY_PER_PAGE", 0.0)
    monkeypatch.setattr(simulate, "PING_DTM_INTERVAL_SECONDS", 10.0)
    monkeypatch.setattr(simulate.random, "expovariate", lambda lam: 3.0)

    transport, calls = _collect_calls()
    async with _httpx.AsyncClient(transport=transport) as client:
        await simulate.simulate_session_async(client, "http://collector")

    events = [json.loads(c)["data"][0] for c in calls]
    pv = next(e for e in events if e["e"] == "pv")
    pps = [e for e in events if e["e"] == "pp"]
    assert len(pps) == 3
    dt_pv = int(pv["dtm"])
    dt_pps = sorted(int(e["dtm"]) for e in pps)
    assert dt_pps[0] - dt_pv == 10_000
    assert dt_pps[1] - dt_pv == 20_000
    assert dt_pps[2] - dt_pv == 30_000
