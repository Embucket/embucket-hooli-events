"""
Snowplow event simulator for Hooli Events.

Generates realistic user sessions by sending Snowplow Tracker Protocol v2
payloads directly to the collector endpoint.

Usage:
    python simulate.py --endpoint http://<ALB_DNS>
    python simulate.py --endpoint http://<ALB_DNS> --sessions 100 --delay 0.05
"""

import argparse
import random
import time
import uuid

import requests

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


def timestamp_ms():
    return str(int(time.time() * 1000))


def base_event(domain_userid, session_id, session_idx):
    return {
        "tv": TRACKER_VERSION,
        "tna": TRACKER_NAMESPACE,
        "aid": APP_ID,
        "p": "web",
        "duid": domain_userid,
        "sid": session_id,
        "vid": str(session_idx),
        "dtm": timestamp_ms(),
        "stm": timestamp_ms(),
        "tz": "America/New_York",
        "lang": random.choice(LANGUAGES),
        "res": random.choice(RESOLUTIONS),
        "cs": "UTF-8",
    }


def page_view(domain_userid, session_id, session_idx, page_key, event=None):
    url_tpl, title_tpl = PAGES[page_key]
    fmt = {}
    if event:
        fmt = {"id": event["id"], "name": event["name"]}
    url = url_tpl.format(**fmt) if fmt else url_tpl
    title = title_tpl.format(**fmt) if fmt else title_tpl

    ev = base_event(domain_userid, session_id, session_idx)
    ev["e"] = "pv"
    ev["url"] = url
    ev["page"] = title
    return ev


def struct_event(domain_userid, session_id, session_idx, category, action, label="", value=None):
    ev = base_event(domain_userid, session_id, session_idx)
    ev["e"] = "se"
    ev["se_ca"] = category
    ev["se_ac"] = action
    if label:
        ev["se_la"] = label
    if value is not None:
        ev["se_va"] = str(value)
    return ev


def send_events(endpoint, events_batch):
    payload = {
        "schema": "iglu:com.snowplowanalytics.snowplow/payload_data/jsonschema/1-0-4",
        "data": events_batch,
    }
    url = endpoint.rstrip("/") + COLLECTOR_PATH
    resp = requests.post(url, json=payload, timeout=10)
    return resp.status_code


def simulate_session(endpoint, delay):
    domain_userid = str(uuid.uuid4())
    session_id = str(uuid.uuid4())
    session_idx = 1
    events = []

    # 1. Homepage
    events.append(page_view(domain_userid, session_id, session_idx, "home"))
    time.sleep(delay)

    # 2. Event listing
    events.append(page_view(domain_userid, session_id, session_idx, "listing"))
    time.sleep(delay)

    # 3. Maybe search
    if random.random() < 0.6:
        term = random.choice(SEARCH_TERMS)
        events.append(struct_event(domain_userid, session_id, session_idx, "search", "submit", term))
        time.sleep(delay)

    # 4. Maybe filter
    if random.random() < 0.4:
        cat = random.choice(CATEGORIES)
        events.append(struct_event(domain_userid, session_id, session_idx, "filter", "apply", cat))
        time.sleep(delay)

    # 5. View event detail
    chosen_event = random.choice(EVENTS)
    events.append(page_view(domain_userid, session_id, session_idx, "detail", chosen_event))
    time.sleep(delay)

    # 6. Maybe add to cart (70% chance)
    if random.random() < 0.7:
        qty = random.randint(1, 4)
        total = chosen_event["price"] * qty
        events.append(struct_event(domain_userid, session_id, session_idx, "cart", "add_to_cart", chosen_event["name"], total))
        time.sleep(delay)

        # 7. View cart
        events.append(page_view(domain_userid, session_id, session_idx, "cart"))
        time.sleep(delay)

        # 8. Maybe purchase (60% of those who add to cart)
        if random.random() < 0.6:
            order_id = "HE-" + uuid.uuid4().hex[:8].upper()
            events.append(page_view(domain_userid, session_id, session_idx, "checkout"))
            events.append(struct_event(domain_userid, session_id, session_idx, "ecommerce", "purchase", order_id, total))
            time.sleep(delay)

    # Send all events for this session in one batch
    status = send_events(endpoint, events)
    return len(events), status


def main():
    parser = argparse.ArgumentParser(description="Snowplow event simulator for Hooli Events")
    parser.add_argument("--endpoint", required=True, help="Collector URL (e.g. http://ALB_DNS)")
    parser.add_argument("--sessions", type=int, default=50, help="Number of sessions to simulate")
    parser.add_argument("--delay", type=float, default=0.1, help="Delay between events in seconds")
    args = parser.parse_args()

    print(f"Simulating {args.sessions} sessions against {args.endpoint}")
    total_events = 0

    for i in range(args.sessions):
        count, status = simulate_session(args.endpoint, args.delay)
        total_events += count
        print(f"  Session {i+1}/{args.sessions}: {count} events, HTTP {status}")

    print(f"Done. Sent {total_events} total events across {args.sessions} sessions.")


if __name__ == "__main__":
    main()
