# ==============================================================================
# COMP90024 Team 39 collaborative project
# Contributor attribution: see CONTRIBUTIONS.md
#
# Description: Fission function entrypoint for social media data cleaning (main.py).
# ==============================================================================
"""
Fission function: Incremental social media data cleaner.

Reads new posts from mastodon_original_data / bsky_original_data
(newer than watermark), cleans and writes to social_media_clean.

Deployed as a Fission timer (e.g. every 10 minutes).
"""

import os
import html
import re
from datetime import datetime, timezone
from elasticsearch import Elasticsearch, helpers
from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer

# =========================
# Configuration
# =========================

SOURCE_INDICES = ["mastodon_original_data", "bsky_original_data"]
TARGET_INDEX   = "social_media_clean"
STATE_INDEX    = "social_media_clean_state"
WATERMARK_ID   = "watermark"
BATCH_SIZE     = 500
MIN_CONTENT_LEN = 10

FUEL_WORDS = [
    "petrol", "fuel", "diesel", "unleaded", "gasoline",
    "bowser", "servo", "pump",
    "fuel prices", "petrol prices", "diesel prices", "gas prices",
    "fuel price", "petrol price", "diesel price",
    "fuel excise", "petrol excise", "fuel tax", "fuel subsidy",
    "cost of living", "price gouging", "e10", "e85", "excise",
    "petrol station", "service station", "gas station",
]

# Narrow exclusions: only obvious non-retail-fuel contexts
EXCLUDE_PATTERNS = [
    r"\bdiesel\s+(loco|locomotive|train|engine|electric|railcar|multiple|unit)\b",
    r"\bDiesel\s+[A-Z0-9]",
    r"\bfuel\s+(rod|cell)\b",
    r"\bpetrol\s+bomb\b",
    r"\b(natural|cooking with|shale)\s+gas\b",
]

AU_TERMS = [
    "australia", "australian", "aussie", "auspol",
    "melbourne", "sydney", "brisbane", "perth", "adelaide",
    "hobart", "darwin", "canberra",
    "queensland", "victoria", "new south wales", "western australia",
    "tasmania", "south australia",
    "nsw", "vic", "qld", "wa", "sa", "act", "nt",
    "woolworths", "coles", "bunnings", "fuelwatch", "alp",
    "rba", "reserve bank", "albanese", "federal government",
]

CITY_PATTERNS = [
    ("Melbourne", r"\bmelbourne\b"),
    ("Sydney",    r"\bsydney\b"),
    ("Brisbane",  r"\bbrisbane\b"),
    ("Perth",     r"\bperth\b"),
    ("Adelaide",  r"\badelaide\b"),
    ("Hobart",    r"\bhobart\b"),
    ("Darwin",    r"\bdarwin\b"),
    ("Canberra",  r"\bcanberra\b"),
]

fuel_regex    = re.compile(r"\b(?:" + "|".join(re.escape(k) for k in FUEL_WORDS) + r")\b", re.IGNORECASE)
au_regex      = re.compile(r"\b(?:" + "|".join(re.escape(t) for t in AU_TERMS) + r")\b", re.IGNORECASE)
exclude_regex = re.compile("|".join(EXCLUDE_PATTERNS), re.IGNORECASE)
city_regex = [(name, re.compile(pat, re.IGNORECASE)) for name, pat in CITY_PATTERNS]

URL_RE      = re.compile(r"https?://\S+|www\.\S+")
HASHTAG_RE  = re.compile(r"#\w+")
MENTION_RE  = re.compile(r"@[\w\.]+")
HTML_TAG_RE = re.compile(r"<[^<]+?>")
WHITESPACE_RE = re.compile(r"\s+")

analyzer = SentimentIntensityAnalyzer()
es_client = None


# =========================
# Fission secret reader
# =========================

def read_fission_secret(key, secret_name="cleaner-secrets"):
    paths = [
        f"/secrets/default/{secret_name}/{key}",
        f"/secrets/{secret_name}/{key}",
    ]
    for path in paths:
        if os.path.exists(path):
            with open(path, "r", encoding="utf-8") as f:
                return f.read().strip()

    val = os.environ.get(key)
    if val:
        return val
    raise FileNotFoundError(f"Secret not found: {secret_name}/{key}")


# =========================
# ES init
# =========================

def init_es():
    global es_client
    if es_client:
        return es_client

    es_host = "https://elasticsearch-es-http.elastic:9200"
    es_user = read_fission_secret("ES_USER")
    es_pass = read_fission_secret("ES_PASS")

    es_client = Elasticsearch(
        [es_host],
        basic_auth=(es_user, es_pass),
        verify_certs=False,
        request_timeout=120,
    )
    return es_client


# =========================
# Cleaning helpers
# =========================

def clean_text(text):
    if not text:
        return ""
    text = html.unescape(text)
    text = HTML_TAG_RE.sub(" ", text)
    text = URL_RE.sub(" ", text)
    text = HASHTAG_RE.sub(" ", text)
    text = MENTION_RE.sub(" ", text)
    text = WHITESPACE_RE.sub(" ", text).strip()
    return text


def extract_city(content, description, location_raw):
    for name, regex in city_regex:
        if regex.search(content or ""):
            return name
    for name, regex in city_regex:
        if regex.search(description or ""):
            return name
    if location_raw and "australia" in location_raw.lower():
        return "Australia"
    return ""


def normalize_date(value):
    if not value:
        return ""
    if isinstance(value, datetime):
        return value.strftime("%Y-%m-%d")
    s = str(value).strip()
    if not s:
        return ""
    s_norm = s.replace("Z", "+00:00")
    try:
        dt = datetime.fromisoformat(s_norm)
        return dt.strftime("%Y-%m-%d")
    except ValueError:
        pass
    m = re.match(r"(\d{4}-\d{2}-\d{2})", s)
    return m.group(1) if m else ""


def location_implies_australia(location, location_raw):
    """Align with ingest: many docs set location to Australia without repeating AU words in text."""
    blob = f"{location or ''} {location_raw or ''}".lower()
    return "australia" in blob


def is_relevant(content, description="", location="", location_raw=""):
    """Fuel in text; AU via text terms OR source location fields (matches ingest tagging)."""
    text = f"{content} {description or ''}".strip()
    if len(text) < MIN_CONTENT_LEN:
        return False
    if not fuel_regex.search(text):
        return False
    if not au_regex.search(text) and not location_implies_australia(location, location_raw):
        return False
    if exclude_regex.search(text):
        return False
    return True


# =========================
# Watermark & mock_id
# =========================

def get_watermark(es):
    try:
        doc = es.get(index=STATE_INDEX, id=WATERMARK_ID)
        return doc["_source"].get("last_cleaned_at", "")
    except Exception:
        return ""


def set_watermark(es, ts):
    if not ts:
        return
    es.index(
        index=STATE_INDEX,
        id=WATERMARK_ID,
        document={"last_cleaned_at": ts, "updated_at": datetime.now(timezone.utc).isoformat()},
        refresh=True,
    )


def get_max_mock_id(es):
    try:
        res = es.search(index=TARGET_INDEX, size=0,
                        aggs={"max_id": {"max": {"field": "mock_id"}}})
        v = res["aggregations"]["max_id"].get("value")
        return int(v) if v else 0
    except Exception:
        return 0


# =========================
# Core logic
# =========================

def build_source_query(watermark):
    fuel_q = " OR ".join(f'"{k}"' for k in FUEL_WORDS)
    au_q   = " OR ".join(f'"{t}"' for t in AU_TERMS)
    text_fields = ["content", "description"]

    au_or_loc = {
        "bool": {
            "should": [
                {"query_string": {"query": au_q, "fields": text_fields}},
                {"query_string": {"query": "Australia", "fields": ["location", "location_raw"]}},
            ],
            "minimum_should_match": 1,
        }
    }

    must = [
        {"query_string": {"query": fuel_q, "fields": text_fields}},
        au_or_loc,
    ]
    if watermark:
        must.append({"range": {"created_at": {"gt": watermark}}})
    return {"bool": {"must": must}}


def clean_and_index(es, watermark):
    query = build_source_query(watermark)

    counter = get_max_mock_id(es)
    seen_ids = set()
    max_ts = ""
    bulk_actions = []
    skipped = 0

    for src_index in SOURCE_INDICES:
        for hit in helpers.scan(es, index=src_index, query={"query": query},
                                size=BATCH_SIZE, preserve_order=False):
            s = hit.get("_source", {})
            ts = s.get("created_at", "")
            if ts and ts > max_ts:
                max_ts = ts

            raw_desc = s.get("description", "") or ""
            content = clean_text(s.get("content", ""))
            desc_clean = clean_text(raw_desc)
            src_location = s.get("location", "") or ""
            src_location_raw = s.get("location_raw", "") or ""
            if not is_relevant(content, desc_clean, src_location, src_location_raw):
                skipped += 1
                continue

            lang = (s.get("language", "") or "").lower()
            if lang and lang != "en":
                skipped += 1
                continue

            date = normalize_date(s.get("created_at", ""))
            if not date:
                skipped += 1
                continue

            platform = (s.get("platform", "") or "").lower()
            if platform == "bluesky":
                platform_norm = "bluesky"
            elif platform == "mastodon":
                platform_norm = "mastodon"
            else:
                platform_norm = platform or "unknown"

            post_id = s.get("post_id", "")
            doc_id = f"{platform_norm}_{post_id}"

            if doc_id in seen_ids:
                continue
            seen_ids.add(doc_id)

            counter += 1
            location_raw = s.get("location_raw", "") or s.get("location", "")
            location = extract_city(content, desc_clean, location_raw)
            polarity = analyzer.polarity_scores(content)["compound"]

            bulk_actions.append({
                "_index": TARGET_INDEX,
                "_id": doc_id,
                "_source": {
                    "mock_id": counter,
                    "content": content,
                    "location": location,
                    "polarity_scores": round(polarity, 4),
                    "platform": platform_norm,
                    "date": date,
                },
            })

    indexed = 0
    if bulk_actions:
        indexed, _ = helpers.bulk(es, bulk_actions, stats_only=True)

    return indexed, skipped, max_ts


# =========================
# Fission entry point
# =========================

def main():
    try:
        es = init_es()
    except Exception as e:
        return {"error": f"ES init failed: {str(e)}"}

    watermark = get_watermark(es)
    indexed, skipped, max_ts = clean_and_index(es, watermark)

    if max_ts:
        set_watermark(es, max_ts)

    return {
        "status": "success",
        "indexed": indexed,
        "skipped": skipped,
        "watermark": max_ts or watermark or "(none)",
    }
