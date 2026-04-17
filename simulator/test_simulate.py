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
