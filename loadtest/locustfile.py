"""
Locust load generator for Hooli Events Snowplow pipeline.

Supports two modes:
    realistic: session-based browsing traffic with a daily shape
    synthetic: fixed-rate event firehose for throughput testing

Usage:
    cd loadtest
    uv run locust --host http://<ALB_DNS>
    # Open http://localhost:8089 to start the test

Synthetic examples:
    HOOLI_MODE=synthetic uv run locust --host http://<ALB_DNS> --headless -u 100 -r 20 -t 1m SyntheticEventsUser
    HOOLI_MODE=synthetic HOOLI_SYNTHETIC_EVENTS_PER_USER_PER_SEC=5 uv run locust --host http://<ALB_DNS> --headless -u 500 -r 100 -t 2m SyntheticEventsUser
    HOOLI_MODE=synthetic HOOLI_SYNTHETIC_BATCH_SIZE=10 uv run locust --host http://<ALB_DNS> --headless -u 200 -r 50 -t 2m SyntheticEventsUser
"""

import os
import random
import time
import uuid

from locust import LoadTestShape, between, constant_throughput, task
from locust.contrib.fasthttp import FastHttpUser

TRACKER_VERSION = "locust-0.1.0"
TRACKER_NAMESPACE = "hooli-load"
APP_ID = "hooli-events"
COLLECTOR_PATH = "/com.snowplowanalytics.snowplow/tp2"
MODE = os.getenv("HOOLI_MODE", "realistic").strip().lower()
ENABLE_DAILY_SHAPE = os.getenv("HOOLI_ENABLE_DAILY_SHAPE", "true" if MODE == "realistic" else "false").strip().lower() in {
    "1",
    "true",
    "yes",
    "on",
}
PEAK_USERS = int(os.getenv("HOOLI_PEAK_USERS", "500"))
SYNTHETIC_EVENTS_PER_USER_PER_SEC = float(os.getenv("HOOLI_SYNTHETIC_EVENTS_PER_USER_PER_SEC", "1.0"))
SYNTHETIC_BATCH_SIZE = int(os.getenv("HOOLI_SYNTHETIC_BATCH_SIZE", "1"))
SYNTHETIC_SESSION_LENGTH = int(os.getenv("HOOLI_SYNTHETIC_SESSION_LENGTH", "100"))

EVENT_THEMES = [
    ("Neon Lights Festival", "music", 49.00),
    ("Jazz Under the Stars", "music", 35.00),
    ("Tech Summit", "tech", 199.00),
    ("Street Food Carnival", "food", 15.00),
    ("Charity Soccer Match", "sports", 25.00),
    ("Modern Art Exhibition", "arts", 12.00),
    ("Indie Film Night", "arts", 22.00),
    ("Startup Pitch Forum", "tech", 89.00),
    ("Craft Beer Walk", "food", 31.00),
    ("Marathon Expo", "sports", 18.00),
    ("Classical Matinee", "music", 42.00),
    ("Design Leadership Lab", "tech", 129.00),
]
EVENT_CITIES = [
    "Austin", "Boston", "Chicago", "Denver", "Miami", "Nashville",
    "New York", "Portland", "San Diego", "Seattle", "Toronto", "Vancouver",
]
EVENT_VENUES = [
    "Civic Hall", "Riverfront Park", "Union Theater", "Warehouse 18",
    "Grand Pavilion", "Eastside Market", "North Pier", "Foundry Stage",
]


def build_event_catalog():
    events = []
    for theme_idx, (theme, category, base_price) in enumerate(EVENT_THEMES):
        for city_idx, city in enumerate(EVENT_CITIES):
            venue = EVENT_VENUES[(theme_idx + city_idx) % len(EVENT_VENUES)]
            event_id = f"{category[:2].upper()}-{theme_idx + 1:02d}-{city_idx + 1:02d}"
            price = base_price + ((city_idx % 5) * 4) + ((theme_idx % 3) * 2)
            events.append({
                "id": event_id,
                "name": f"{city} {theme}",
                "category": category,
                "venue": venue,
                "city": city,
                "price": round(price, 2),
            })
    return events


EVENTS = build_event_catalog()

PAGES = {
    "home": ("https://hooli-events.com/", "Hooli Events - Discover Live Events"),
    "listing": ("https://hooli-events.com/events.html", "Browse Events - Hooli Events"),
    "detail": ("https://hooli-events.com/event-detail.html?id={id}", "{name} - Hooli Events"),
    "cart": ("https://hooli-events.com/cart.html", "Cart - Hooli Events"),
    "checkout": ("https://hooli-events.com/checkout.html", "Order Confirmed - Hooli Events"),
}

SEARCH_TERMS = [
    "concert", "jazz", "tech", "food", "festival", "art", "soccer", "music",
    "startup events", "film night", "beer garden", "marathon", "gallery",
    "family events", "date night", "live music", "design workshop",
    "networking", "outdoor festival", "things to do", "weekend tickets",
]
CATEGORIES = sorted({event["category"] for event in EVENTS})
RESOLUTIONS = ["1920x1080", "1440x900", "1366x768", "2560x1440", "390x844"]
LANGUAGES = ["en-US", "en-GB", "es-ES", "fr-FR", "de-DE"]
USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 14_5) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.5 Safari/605.1.15",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    "Mozilla/5.0 (iPhone; CPU iPhone OS 17_5 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.5 Mobile/15E148 Safari/604.1",
    "Mozilla/5.0 (Linux; Android 14; Pixel 8) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/125.0.0.0 Mobile Safari/537.36",
    "Mozilla/5.0 (iPad; CPU OS 17_5 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.5 Mobile/15E148 Safari/604.1",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:126.0) Gecko/20100101 Firefox/126.0",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 14.5; rv:126.0) Gecko/20100101 Firefox/126.0",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36 Edg/125.0.0.0",
    "Mozilla/5.0 (Linux; Android 14; SM-S921U) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/125.0.0.0 Mobile Safari/537.36",
]


def _timestamp_ms():
    return str(int(time.time() * 1000))


def _base_payload(duid, sid):
    resolution = random.choice(RESOLUTIONS)
    return {
        "tv": TRACKER_VERSION,
        "tna": TRACKER_NAMESPACE,
        "aid": APP_ID,
        "p": "web",
        "eid": str(uuid.uuid4()),
        "duid": duid,
        "sid": sid,
        "vid": "1",
        "dtm": _timestamp_ms(),
        "stm": _timestamp_ms(),
        "tz": "America/New_York",
        "lang": random.choice(LANGUAGES),
        "res": resolution,
        "vp": resolution,
        "ua": random.choice(USER_AGENTS),
        "cs": "UTF-8",
    }


def _wrap_payload(event_data):
    return {
        "schema": "iglu:com.snowplowanalytics.snowplow/payload_data/jsonschema/1-0-4",
        "data": [event_data],
    }


class HooliEventsUser(FastHttpUser):
    """Simulates a single user browsing session on Hooli Events."""

    wait_time = between(1, 5)

    def on_start(self):
        self._new_session()

    def _new_session(self):
        self._duid = str(uuid.uuid4())
        self._sid = str(uuid.uuid4())
        self._step = 0

    def _send_event(self, event_data, name):
        self.client.post(
            COLLECTOR_PATH,
            json=_wrap_payload(event_data),
            name=name,
            timeout=10,
        )

    def _page_view(self, page_key, event=None):
        url_tpl, title_tpl = PAGES[page_key]
        fmt = {"id": event["id"], "name": event["name"]} if event else {}
        url = url_tpl.format(**fmt) if fmt else url_tpl
        title = title_tpl.format(**fmt) if fmt else title_tpl
        ev = _base_payload(self._duid, self._sid)
        ev["e"] = "pv"
        ev["url"] = url
        ev["page"] = title
        self._send_event(ev, f"pv:{page_key}")

    def _struct_event(self, category, action, label="", value=None):
        ev = _base_payload(self._duid, self._sid)
        ev["e"] = "se"
        ev["se_ca"] = category
        ev["se_ac"] = action
        if label:
            ev["se_la"] = label
        if value is not None:
            ev["se_va"] = str(value)
        self._send_event(ev, f"se:{category}/{action}")

    @task
    def browse_session(self):
        """Walk through a realistic browsing session step by step.

        Each call to this task advances one step in the session.
        When the session is complete, a new session starts.
        """
        step = self._step
        self._step += 1

        if step == 0:
            self._page_view("home")
            return

        if step == 1:
            self._page_view("listing")
            return

        if step == 2:
            if random.random() < 0.6:
                self._struct_event("search", "submit", random.choice(SEARCH_TERMS))
            return

        if step == 3:
            if random.random() < 0.4:
                self._struct_event("filter", "apply", random.choice(CATEGORIES))
            return

        if step == 4:
            self._chosen_event = random.choice(EVENTS)
            self._page_view("detail", self._chosen_event)
            return

        if step == 5:
            if random.random() < 0.7:
                qty = random.randint(1, 4)
                self._cart_total = self._chosen_event["price"] * qty
                self._struct_event("cart", "add_to_cart", self._chosen_event["name"], self._cart_total)
                self._added_to_cart = True
            else:
                self._added_to_cart = False
            return

        if step == 6:
            if getattr(self, "_added_to_cart", False):
                self._page_view("cart")
            return

        if step == 7:
            if getattr(self, "_added_to_cart", False) and random.random() < 0.6:
                order_id = "HE-" + uuid.uuid4().hex[:8].upper()
                self._page_view("checkout")
                self._struct_event("ecommerce", "purchase", order_id, getattr(self, "_cart_total", 0))
            self._new_session()
            return

        self._new_session()


class SyntheticEventsUser(FastHttpUser):
    """Generates simple, high-rate synthetic events for throughput testing."""

    wait_time = constant_throughput(SYNTHETIC_EVENTS_PER_USER_PER_SEC)

    def on_start(self):
        self._new_session()

    def _new_session(self):
        self._duid = str(uuid.uuid4())
        self._sid = str(uuid.uuid4())
        self._events_in_session = 0

    def _send_events(self, event_data_list, name):
        self.client.post(
            COLLECTOR_PATH,
            json=_wrap_payload(event_data_list[0]) if len(event_data_list) == 1 else {
                "schema": "iglu:com.snowplowanalytics.snowplow/payload_data/jsonschema/1-0-4",
                "data": event_data_list,
            },
            name=name,
            timeout=10,
        )

    @task
    def firehose_event(self):
        batch = []
        for _ in range(SYNTHETIC_BATCH_SIZE):
            ev = _base_payload(self._duid, self._sid)
            ev["e"] = "se"
            ev["se_ca"] = "synthetic"
            ev["se_ac"] = "throughput"
            ev["se_la"] = "loadtest"
            ev["se_va"] = "1"
            batch.append(ev)
        request_name = "synthetic:event" if SYNTHETIC_BATCH_SIZE == 1 else f"synthetic:batch:{SYNTHETIC_BATCH_SIZE}"
        self._send_events(batch, request_name)
        self._events_in_session += SYNTHETIC_BATCH_SIZE
        if self._events_in_session >= SYNTHETIC_SESSION_LENGTH:
            self._new_session()


if ENABLE_DAILY_SHAPE:
    class DailyTrafficShape(LoadTestShape):
        """Modulates user count over a 24-hour cycle.

        The shape loops every 24 hours (86400 seconds). Time is wall-clock
        relative to when the test started, so starting at 10am means the
        shape begins at the "10:00" point in the curve.

        Target: ~10M events/day = ~115 ev/s average.
        At peak (100%) with ~500 users each producing ~1 ev/2.5s = ~200 ev/s.
        """

        CURVE = [
            (0, 0.10),
            (6, 0.10),
            (10, 0.50),
            (10, 1.00),
            (14, 1.00),
            (14, 0.70),
            (18, 0.70),
            (18, 1.00),
            (22, 1.00),
            (24, 0.10),
        ]

        def tick(self):
            run_time = self.get_run_time()
            hour = (run_time % 86400) / 3600.0

            fraction = self.CURVE[0][1]
            for i in range(1, len(self.CURVE)):
                h0, f0 = self.CURVE[i - 1]
                h1, f1 = self.CURVE[i]
                if h0 <= hour < h1:
                    t = (hour - h0) / (h1 - h0) if h1 != h0 else 0
                    fraction = f0 + t * (f1 - f0)
                    break

            users = max(1, int(PEAK_USERS * fraction))
            spawn_rate = max(1, users // 10)
            return users, spawn_rate
