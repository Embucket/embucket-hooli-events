"""
Snowplow event simulator for Hooli Events.

Generates realistic user sessions by sending Snowplow Tracker Protocol v2
payloads directly to the collector endpoint.

Usage:
    python simulate.py --endpoint http://<ALB_DNS>
    python simulate.py --endpoint http://<ALB_DNS> --sessions 100 --delay 0.05
"""

import argparse
import asyncio
import base64
import json
import os
import random
import signal
import time
import uuid

import requests

try:
    import httpx
except ImportError:  # pragma: no cover - only hit if running one-shot without httpx installed
    httpx = None

TRACKER_VERSION = "py-sim-0.1.0"
TRACKER_NAMESPACE = "hooli-sim"
APP_ID = "hooli-events"
COLLECTOR_PATH = "/com.snowplowanalytics.snowplow/tp2"

EVENTS = [
    {"id": "1", "name": "Neon Lights Festival", "price": 49.00},
    {"id": "2", "name": "Jazz Under the Stars", "price": 35.00},
    {"id": "3", "name": "Tech Summit 2026", "price": 199.00},
    {"id": "4", "name": "Street Food Carnival", "price": 15.00},
    {"id": "5", "name": "Charity Soccer Match", "price": 25.00},
    {"id": "6", "name": "Modern Art Exhibition", "price": 12.00},
]

PAGES = {
    "home": ("https://hooli-events.com/", "Hooli Events - Discover Live Events"),
    "listing": ("https://hooli-events.com/events.html", "Browse Events - Hooli Events"),
    "detail": ("https://hooli-events.com/event-detail.html?id={id}", "{name} - Hooli Events"),
    "cart": ("https://hooli-events.com/cart.html", "Cart - Hooli Events"),
    "checkout": ("https://hooli-events.com/checkout.html", "Order Confirmed - Hooli Events"),
}

SEARCH_TERMS = ["concert", "jazz", "tech", "food", "festival", "art", "soccer", "music"]
CATEGORIES = ["music", "tech", "food", "sports"]
RESOLUTIONS = ["1920x1080", "1440x900", "1366x768", "2560x1440", "390x844"]
LANGUAGES = ["en-US", "en-GB", "es-ES", "fr-FR", "de-DE"]

WEB_PAGE_SCHEMA = "iglu:com.snowplowanalytics.snowplow/web_page/jsonschema/1-0-0"
CONTEXTS_WRAPPER_SCHEMA = "iglu:com.snowplowanalytics.snowplow/contexts/jsonschema/1-0-0"


from collections import OrderedDict

USER_POOL_MAX = int(os.environ.get("USER_POOL_MAX", "100000"))
NEW_USER_PROBABILITY = float(os.environ.get("NEW_USER_PROBABILITY", "0.20"))

USERS: "OrderedDict[str, int]" = OrderedDict()
USERS_LOCK = asyncio.Lock()


async def get_or_create_user(new_prob=None):
    """Return (domain_userid, session_idx) from a per-task LRU pool.

    With probability (1 - new_prob) return a randomly-chosen existing user
    with session_idx bumped; otherwise create a new user with session_idx=1.
    """
    if new_prob is None:
        new_prob = NEW_USER_PROBABILITY
    async with USERS_LOCK:
        if USERS and random.random() > new_prob:
            uid = random.choice(list(USERS.keys()))
            USERS.move_to_end(uid)
            USERS[uid] += 1
            return uid, USERS[uid]
        uid = str(uuid.uuid4())
        USERS[uid] = 1
        if len(USERS) > USER_POOL_MAX:
            USERS.popitem(last=False)
        return uid, 1


def encode_cx(contexts):
    """Encode a list of self-describing contexts as the tp2 `cx` field.

    Returns base64url-encoded JSON of the contexts wrapper, without padding.
    This matches what the Snowplow JS tracker sends.
    """
    payload = {"schema": CONTEXTS_WRAPPER_SCHEMA, "data": list(contexts)}
    raw = json.dumps(payload, separators=(",", ":")).encode()
    return base64.urlsafe_b64encode(raw).rstrip(b"=").decode()


def web_page_context(page_view_id):
    return {"schema": WEB_PAGE_SCHEMA, "data": {"id": page_view_id}}


def base_event(domain_userid, session_id, session_idx, dtm_ms=None):
    now_ms = int(time.time() * 1000)
    if dtm_ms is None:
        dtm_ms = now_ms
    return {
        "tv": TRACKER_VERSION,
        "tna": TRACKER_NAMESPACE,
        "aid": APP_ID,
        "p": "web",
        "duid": domain_userid,
        "sid": session_id,
        "vid": str(session_idx),
        "dtm": str(dtm_ms),      # event time (possibly backdated)
        "stm": str(now_ms),      # send time — always now
        "tz": "America/New_York",
        "lang": random.choice(LANGUAGES),
        "res": random.choice(RESOLUTIONS),
        "cs": "UTF-8",
    }


def page_view(domain_userid, session_id, session_idx, page_key,
              event=None, page_view_id=None, dtm_ms=None):
    ev, _ = page_view_with_id(domain_userid, session_id, session_idx, page_key,
                              event=event, page_view_id=page_view_id, dtm_ms=dtm_ms)
    return ev


def page_view_with_id(domain_userid, session_id, session_idx, page_key,
                      event=None, page_view_id=None, dtm_ms=None):
    """Like page_view but also returns the generated page_view_id for reuse by later struct events."""
    url_tpl, title_tpl = PAGES[page_key]
    fmt = {}
    if event:
        fmt = {"id": event["id"], "name": event["name"]}
    url = url_tpl.format(**fmt) if fmt else url_tpl
    title = title_tpl.format(**fmt) if fmt else title_tpl

    if page_view_id is None:
        page_view_id = str(uuid.uuid4())

    ev = base_event(domain_userid, session_id, session_idx, dtm_ms=dtm_ms)
    ev["e"] = "pv"
    ev["url"] = url
    ev["page"] = title
    ev["cx"] = encode_cx([web_page_context(page_view_id)])
    return ev, page_view_id


def struct_event(domain_userid, session_id, session_idx, category, action,
                 label="", value=None, page_view_id=None, dtm_ms=None):
    ev = base_event(domain_userid, session_id, session_idx, dtm_ms=dtm_ms)
    ev["e"] = "se"
    ev["se_ca"] = category
    ev["se_ac"] = action
    if label:
        ev["se_la"] = label
    if value is not None:
        ev["se_va"] = str(value)
    if page_view_id is not None:
        ev["cx"] = encode_cx([web_page_context(page_view_id)])
    return ev


def page_ping(domain_userid, session_id, session_idx, page_view_id,
              url, title, pp_xoff=(0, 0), pp_yoff=(0, 0), dtm_ms=None):
    """Emit a Snowplow page_ping event carrying the parent page_view's web_page context.

    pp_xoff and pp_yoff are (min, max) pairs of pixel offsets that real trackers
    record as the scroll extent observed during the ping interval.
    """
    ev = base_event(domain_userid, session_id, session_idx, dtm_ms=dtm_ms)
    ev["e"] = "pp"
    ev["url"] = url
    ev["page"] = title
    ev["pp_mix"] = str(pp_xoff[0])
    ev["pp_max"] = str(pp_xoff[1])
    ev["pp_miy"] = str(pp_yoff[0])
    ev["pp_may"] = str(pp_yoff[1])
    ev["cx"] = encode_cx([web_page_context(page_view_id)])
    return ev


UNSTRUCT_WRAPPER_SCHEMA = "iglu:com.snowplowanalytics.snowplow/unstruct_event/jsonschema/1-0-0"
LINK_CLICK_SCHEMA       = "iglu:com.snowplowanalytics.snowplow/link_click/jsonschema/1-0-1"
SUBMIT_FORM_SCHEMA      = "iglu:com.snowplowanalytics.snowplow/submit_form/jsonschema/1-0-0"
FOCUS_FORM_SCHEMA       = "iglu:com.snowplowanalytics.snowplow/focus_form/jsonschema/1-0-0"
CHANGE_FORM_SCHEMA      = "iglu:com.snowplowanalytics.snowplow/change_form/jsonschema/1-0-0"


def _encode_ue(ue_schema, ue_data):
    payload = {"schema": UNSTRUCT_WRAPPER_SCHEMA,
               "data": {"schema": ue_schema, "data": ue_data}}
    raw = json.dumps(payload, separators=(",", ":")).encode()
    return base64.urlsafe_b64encode(raw).rstrip(b"=").decode()


def unstruct_event(domain_userid, session_id, session_idx,
                   ue_schema, ue_data, page_view_id=None, dtm_ms=None):
    ev = base_event(domain_userid, session_id, session_idx, dtm_ms=dtm_ms)
    ev["e"] = "ue"
    ev["ue_px"] = _encode_ue(ue_schema, ue_data)
    if page_view_id is not None:
        ev["cx"] = encode_cx([web_page_context(page_view_id)])
    return ev


def link_click(domain_userid, session_id, session_idx, page_view_id,
               target_url, element_id="", dtm_ms=None):
    return unstruct_event(domain_userid, session_id, session_idx,
                          LINK_CLICK_SCHEMA,
                          {"targetUrl": target_url, "elementId": element_id},
                          page_view_id=page_view_id, dtm_ms=dtm_ms)


def submit_form(domain_userid, session_id, session_idx, page_view_id,
                form_id, elements=None, dtm_ms=None):
    return unstruct_event(domain_userid, session_id, session_idx,
                          SUBMIT_FORM_SCHEMA,
                          {"formId": form_id, "formClasses": [],
                           "elements": elements or []},
                          page_view_id=page_view_id, dtm_ms=dtm_ms)


def focus_form(domain_userid, session_id, session_idx, page_view_id,
               form_id, element_id, node_name="INPUT", dtm_ms=None):
    return unstruct_event(domain_userid, session_id, session_idx,
                          FOCUS_FORM_SCHEMA,
                          {"formId": form_id, "elementId": element_id,
                           "nodeName": node_name, "elementClasses": [],
                           "value": None},
                          page_view_id=page_view_id, dtm_ms=dtm_ms)


def change_form(domain_userid, session_id, session_idx, page_view_id,
                form_id, element_id, new_value,
                node_name="INPUT", type_="text", dtm_ms=None):
    return unstruct_event(domain_userid, session_id, session_idx,
                          CHANGE_FORM_SCHEMA,
                          {"formId": form_id, "elementId": element_id,
                           "nodeName": node_name, "type": type_,
                           "elementClasses": [], "value": new_value},
                          page_view_id=page_view_id, dtm_ms=dtm_ms)


def send_events(endpoint, events_batch):
    payload = {
        "schema": "iglu:com.snowplowanalytics.snowplow/payload_data/jsonschema/1-0-4",
        "data": events_batch,
    }
    url = endpoint.rstrip("/") + COLLECTOR_PATH
    resp = requests.post(url, json=payload, timeout=10)
    return resp.status_code


class RateRegulator:
    """Paces session spawns at sessions_per_min, capped at `concurrency` active slots.

    A single producer task adds a permit to `permits` every `60/R` seconds,
    blocking on `asyncio.Queue.put` once `concurrency` permits are outstanding.
    Workers `await acquire()` before starting a session and `release()` when done.
    """

    def __init__(self, sessions_per_min, concurrency):
        if sessions_per_min <= 0:
            raise ValueError("sessions_per_min must be > 0")
        if concurrency <= 0:
            raise ValueError("concurrency must be > 0")
        self.interval = 60.0 / sessions_per_min
        self.concurrency = concurrency
        self.permits = asyncio.Queue(maxsize=concurrency)
        self.free = asyncio.Queue(maxsize=concurrency)
        self._producer = None
        self._stop = asyncio.Event()

    async def start(self):
        for i in range(self.concurrency):
            self.free.put_nowait(i)
        self._producer = asyncio.create_task(self._run())

    async def _run(self):
        while not self._stop.is_set():
            slot = await self.free.get()
            await self.permits.put(slot)
            try:
                await asyncio.wait_for(self._stop.wait(), timeout=self.interval)
                return
            except asyncio.TimeoutError:
                pass

    async def acquire(self):
        return await self.permits.get()

    def release(self, slot):
        self.free.put_nowait(slot)

    async def stop(self):
        self._stop.set()
        if self._producer is not None:
            self._producer.cancel()
            try:
                await self._producer
            except asyncio.CancelledError:
                pass


def simulate_session(endpoint, delay):
    domain_userid = str(uuid.uuid4())
    session_id = str(uuid.uuid4())
    session_idx = 1
    events = []

    # 1. Homepage
    ev, home_pv = page_view_with_id(domain_userid, session_id, session_idx, "home")
    events.append(ev)
    time.sleep(delay)

    # 2. Event listing
    ev, listing_pv = page_view_with_id(domain_userid, session_id, session_idx, "listing")
    events.append(ev)
    time.sleep(delay)

    # 3. Maybe search (on the listing page)
    if random.random() < 0.6:
        term = random.choice(SEARCH_TERMS)
        events.append(struct_event(domain_userid, session_id, session_idx, "search", "submit", term, page_view_id=listing_pv))
        time.sleep(delay)

    # 4. Maybe filter (on the listing page)
    if random.random() < 0.4:
        cat = random.choice(CATEGORIES)
        events.append(struct_event(domain_userid, session_id, session_idx, "filter", "apply", cat, page_view_id=listing_pv))
        time.sleep(delay)

    # 5. View event detail
    chosen_event = random.choice(EVENTS)
    ev, detail_pv = page_view_with_id(domain_userid, session_id, session_idx, "detail", chosen_event)
    events.append(ev)
    time.sleep(delay)

    # 6. Maybe add to cart (70% chance — on the detail page)
    if random.random() < 0.7:
        qty = random.randint(1, 4)
        total = chosen_event["price"] * qty
        events.append(struct_event(domain_userid, session_id, session_idx, "cart", "add_to_cart", chosen_event["name"], total, page_view_id=detail_pv))
        time.sleep(delay)

        # 7. View cart
        ev, cart_pv = page_view_with_id(domain_userid, session_id, session_idx, "cart")
        events.append(ev)
        time.sleep(delay)

        # 8. Maybe purchase (60% of those who add to cart — on the checkout page)
        if random.random() < 0.6:
            order_id = "HE-" + uuid.uuid4().hex[:8].upper()
            ev, checkout_pv = page_view_with_id(domain_userid, session_id, session_idx, "checkout")
            events.append(ev)
            events.append(struct_event(domain_userid, session_id, session_idx, "ecommerce", "purchase", order_id, total, page_view_id=checkout_pv))
            time.sleep(delay)

    # Send all events for this session in one batch
    status = send_events(endpoint, events)
    return len(events), status


async def send_events_async(client, endpoint, events_batch):
    payload = {
        "schema": "iglu:com.snowplowanalytics.snowplow/payload_data/jsonschema/1-0-4",
        "data": events_batch,
    }
    url = endpoint.rstrip("/") + COLLECTOR_PATH
    resp = await client.post(url, json=payload, timeout=10)
    return resp.status_code


async def simulate_session_async(client, endpoint, think_min=0.3, think_max=1.5):
    """Same narrative as simulate_session, but fires each event as its own HTTP
    request with think-time in between (more realistic than a single batch)."""

    async def post_one(ev):
        return await send_events_async(client, endpoint, [ev])

    async def think():
        await asyncio.sleep(random.uniform(think_min, think_max))

    domain_userid = str(uuid.uuid4())
    session_id = str(uuid.uuid4())
    session_idx = 1
    count = 0

    ev, home_pv = page_view_with_id(domain_userid, session_id, session_idx, "home")
    await post_one(ev); count += 1; await think()

    ev, listing_pv = page_view_with_id(domain_userid, session_id, session_idx, "listing")
    await post_one(ev); count += 1; await think()

    if random.random() < 0.6:
        await post_one(struct_event(domain_userid, session_id, session_idx, "search", "submit",
                                    random.choice(SEARCH_TERMS), page_view_id=listing_pv))
        count += 1; await think()

    if random.random() < 0.4:
        await post_one(struct_event(domain_userid, session_id, session_idx, "filter", "apply",
                                    random.choice(CATEGORIES), page_view_id=listing_pv))
        count += 1; await think()

    chosen_event = random.choice(EVENTS)
    ev, detail_pv = page_view_with_id(domain_userid, session_id, session_idx, "detail", chosen_event)
    await post_one(ev); count += 1; await think()

    if random.random() < 0.7:
        qty = random.randint(1, 4)
        total = chosen_event["price"] * qty
        await post_one(struct_event(domain_userid, session_id, session_idx, "cart", "add_to_cart",
                                    chosen_event["name"], total, page_view_id=detail_pv))
        count += 1; await think()

        ev, cart_pv = page_view_with_id(domain_userid, session_id, session_idx, "cart")
        await post_one(ev); count += 1; await think()

        if random.random() < 0.6:
            order_id = "HE-" + uuid.uuid4().hex[:8].upper()
            ev, checkout_pv = page_view_with_id(domain_userid, session_id, session_idx, "checkout")
            await post_one(ev); count += 1
            await post_one(struct_event(domain_userid, session_id, session_idx, "ecommerce", "purchase",
                                        order_id, total, page_view_id=checkout_pv))
            count += 1; await think()

    return count


async def run_continuous(endpoint, sessions_per_min, concurrency):
    """Long-running driver: spawn sessions at the given rate, cap at `concurrency`
    in-flight. Exits cleanly on SIGTERM/SIGINT."""
    if httpx is None:
        raise RuntimeError("httpx is required for continuous mode; pip install httpx")

    reg = RateRegulator(sessions_per_min=sessions_per_min, concurrency=concurrency)
    await reg.start()
    stop = asyncio.Event()
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, stop.set)

    totals = {"sessions": 0, "events": 0}
    async with httpx.AsyncClient() as client:
        tasks = set()

        async def run_one(slot):
            try:
                try:
                    # Stash the awaited result in a local first: `x += await ...` reads x
                    # BEFORE the await yields, so concurrent completions race on the
                    # read-modify-write and drop events from the counter.
                    session_events = await simulate_session_async(client, endpoint)
                    totals["events"] += session_events
                    totals["sessions"] += 1
                    if totals["sessions"] % 20 == 0:
                        print(f"  [continuous] sessions={totals['sessions']} events={totals['events']} errors={totals.get('errors', 0)}", flush=True)
                except Exception as e:
                    totals["errors"] = totals.get("errors", 0) + 1
                    # Sample-log every 20th error so transient spikes don't spam CloudWatch.
                    if totals["errors"] % 20 == 1:
                        print(f"  [continuous] session error ({type(e).__name__}): {e}", flush=True)
            finally:
                reg.release(slot)

        while not stop.is_set():
            try:
                slot = await asyncio.wait_for(reg.acquire(), timeout=0.5)
            except asyncio.TimeoutError:
                continue
            t = asyncio.create_task(run_one(slot))
            tasks.add(t)
            t.add_done_callback(tasks.discard)

        await reg.stop()
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)

    print(f"Stopped. Total sessions={totals['sessions']} events={totals['events']} errors={totals.get('errors', 0)}")


def main():
    parser = argparse.ArgumentParser(description="Snowplow event simulator for Hooli Events")
    parser.add_argument("--endpoint", default=os.environ.get("COLLECTOR_ENDPOINT"),
                        help="Collector URL (e.g. http://ALB_DNS). Env: COLLECTOR_ENDPOINT")
    parser.add_argument("--sessions", type=int, default=None,
                        help="One-shot mode: number of sessions to simulate then exit")
    parser.add_argument("--delay", type=float, default=0.1,
                        help="One-shot mode: delay between events in seconds")
    parser.add_argument("--sessions-per-min", type=float,
                        default=float(os.environ.get("SESSIONS_PER_MIN", "60")),
                        help="Continuous mode: target sessions per minute. Env: SESSIONS_PER_MIN")
    parser.add_argument("--concurrency", type=int,
                        default=int(os.environ.get("CONCURRENCY", "20")),
                        help="Continuous mode: max in-flight sessions. Env: CONCURRENCY")
    args = parser.parse_args()

    if not args.endpoint:
        parser.error("--endpoint (or COLLECTOR_ENDPOINT env var) is required")

    if args.sessions is not None:
        # One-shot mode
        print(f"Simulating {args.sessions} sessions against {args.endpoint}")
        total_events = 0
        for i in range(args.sessions):
            count, status = simulate_session(args.endpoint, args.delay)
            total_events += count
            print(f"  Session {i+1}/{args.sessions}: {count} events, HTTP {status}")
        print(f"Done. Sent {total_events} total events across {args.sessions} sessions.")
        return

    # Continuous mode (default when --sessions is absent)
    print(f"Continuous mode: target {args.sessions_per_min} sessions/min, "
          f"concurrency={args.concurrency}, endpoint={args.endpoint}")
    asyncio.run(run_continuous(args.endpoint, args.sessions_per_min, args.concurrency))


if __name__ == "__main__":
    main()
