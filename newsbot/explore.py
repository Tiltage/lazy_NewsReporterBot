"""explore.py - Exploration layer: fetch feeds, filter, dedupe, return standardised items.

Zero LLM tokens. Public API:
    explore(sources, filters=None, seen_path="data/seen.json") -> list[dict]
    mark_sent(items, seen_path="data/seen.json") -> None     # call only AFTER a successful send

Item contract (the only thing downstream modules rely on):
    {"id": int, "title": str, "url": str, "snippet": str,
     "source": str, "section": str, "published": "YYYY-MM-DDTHH:MM:SSZ"}   # UTC
Items come back grouped by section, in the order of filters.section_quota.

Per-source overrides (optional keys on a source): max_age_hours, cap.

Run standalone (read-only unless --mark-sent):
    python newsbot/explore.py --check          # health table for every feed: prune what fails
    python newsbot/explore.py                  # candidates as JSON on stdout (reads config.yaml)
    python newsbot/explore.py > candidates.json
"""
from __future__ import annotations

import argparse
import hashlib
import html
import json
import logging
import os
import re
import statistics
import sys
import tempfile
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta, timezone
from itertools import zip_longest
from pathlib import Path
from urllib.parse import parse_qsl, urlencode, urlsplit, urlunsplit

import feedparser
import requests

log = logging.getLogger("newsbot.explore")

USER_AGENT = "lazy_NewsReporterBot/1.0 (+https://github.com/Tiltage/lazy_NewsReporterBot)"
HTTP_TIMEOUT = (5, 20)          # (connect, read) seconds
MAX_WORKERS = 6                 # parallel feed downloads
DEFAULT_SEEN_PATH = "data/seen.json"
SEEN_KEEP_DAYS = 14

DEFAULT_FILTERS = {
    "max_age_hours": 36,        # drop items older than this (per-source override: max_age_hours)
    "per_source_cap": 6,        # newest N per source (per-source override: cap)
    "snippet_chars": 200,       # truncate snippets (token gate)
    "max_candidates": 60,       # split evenly across sections if section_quota is not given
    "section_quota": {},        # e.g. {"AI": 24, "Crypto": 14, "Finance": 22}
    "exclude_keywords": [],     # case-insensitive, matched against the title only
}

_TAG_RE = re.compile(r"<[^>]+>")
_WS_RE = re.compile(r"\s+")
_BOILERPLATE = [
    re.compile(r"\s*The post .{0,300}? appeared first on .{0,120}$", re.I),          # WordPress feeds
    re.compile(r"\s*(?:\[?(?:…|\.\.\.)\]?\s*)?(?:Read more|Continue reading)\b.{0,100}$", re.I),
]
_TRACKING_PARAMS = {"fbclid", "gclid", "mc_cid", "mc_eid", "ref", "ref_src", "cmpid"}


# ---------------------------------------------------------------- helpers

def _clean_text(raw: str | None) -> str:
    """Strip HTML, unescape entities, collapse whitespace."""
    text = _TAG_RE.sub(" ", raw or "")
    text = html.unescape(text)
    text = _TAG_RE.sub(" ", text)          # second pass catches escaped markup
    return _WS_RE.sub(" ", text).strip()


def _snippet(raw: str | None, title: str, limit: int) -> str:
    text = _clean_text(raw)
    for pat in _BOILERPLATE:
        text = pat.sub("", text).strip()
    if len(text) > limit:
        text = text[:limit].rsplit(" ", 1)[0].rstrip(",.;:- ") + "…"
    if len(text) < 20 or text.lower() == title.lower():
        return ""                           # e.g. HN's bare "Comments" link
    return text


def _normalize_url(url: str) -> str:
    parts = urlsplit(url.strip())
    query = [
        (k, v) for k, v in parse_qsl(parts.query, keep_blank_values=True)
        if not k.lower().startswith("utm_") and k.lower() not in _TRACKING_PARAMS
    ]
    path = parts.path.rstrip("/") or "/"
    return urlunsplit((parts.scheme.lower(), parts.netloc.lower(), path, urlencode(query), ""))


def _url_key(url: str) -> str:
    return hashlib.sha1(_normalize_url(url).encode("utf-8")).hexdigest()[:12]


def _entry_time(entry) -> datetime | None:
    for key in ("published_parsed", "updated_parsed", "created_parsed"):
        t = entry.get(key)
        if t:
            try:
                return datetime(*t[:6], tzinfo=timezone.utc)
            except (TypeError, ValueError):
                continue
    return None


def _utc_today() -> str:
    return datetime.now(timezone.utc).date().isoformat()


# ---------------------------------------------------------------- fetch

def _download(source: dict):
    """-> (feedparser result | None, error string). Never raises."""
    try:
        resp = requests.get(source["url"], headers={"User-Agent": USER_AGENT}, timeout=HTTP_TIMEOUT)
        resp.raise_for_status()
    except (requests.RequestException, KeyError) as e:
        return None, str(e)
    parsed = feedparser.parse(resp.content)
    if not parsed.entries:
        return None, "empty or unparseable"
    return parsed, ""


def fetch_source(source: dict, snippet_chars: int) -> list[dict]:
    """Fetch one feed into raw items. Returns [] on any failure (dead feeds are normal)."""
    name = source.get("name") or source.get("url", "?")
    parsed, err = _download(source)
    if parsed is None:
        log.warning("feed failed: %s (%s)", name, err)
        return []

    fetched_at = datetime.now(timezone.utc)
    items = []
    for e in parsed.entries:
        title = _clean_text(e.get("title"))
        url = (e.get("link") or "").strip()
        if not title or not url.startswith(("http://", "https://")):
            continue
        raw = e.get("summary") or e.get("description") or ""
        if not raw and e.get("content"):
            raw = e["content"][0].get("value", "")
        items.append({
            "title": title,
            "url": url,
            "snippet": _snippet(raw, title, snippet_chars),
            "source": name,
            "section": source.get("section", ""),
            "published": _entry_time(e) or fetched_at,   # fallback: fetch time if feed has no date
        })
    return items


# ---------------------------------------------------------------- seen store

def _load_seen(path: str | Path, keep_days: int = SEEN_KEEP_DAYS) -> dict[str, str]:
    """{url_key: 'YYYY-MM-DD'}. Missing/corrupt file => empty. Old entries are dropped."""
    try:
        data = json.loads(Path(path).read_text(encoding="utf-8"))
    except (OSError, ValueError):
        return {}
    if not isinstance(data, dict):
        return {}
    cutoff = (datetime.now(timezone.utc).date() - timedelta(days=keep_days)).isoformat()
    return {k: v for k, v in data.items() if isinstance(v, str) and v >= cutoff}


def mark_sent(items: list[dict], seen_path: str | Path = DEFAULT_SEEN_PATH,
              keep_days: int = SEEN_KEEP_DAYS) -> None:
    """Record items as sent (and prune old entries). Atomic write."""
    seen = _load_seen(seen_path, keep_days)
    today = _utc_today()
    for it in items:
        seen[_url_key(it["url"])] = today
    path = Path(seen_path)
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, tmp = tempfile.mkstemp(dir=path.parent, suffix=".tmp")
    with os.fdopen(fd, "w", encoding="utf-8") as fh:
        json.dump(seen, fh)
    os.replace(tmp, path)


# ---------------------------------------------------------------- select

def select(sources: list[dict], items_by_source: list[list[dict]],
           filters: dict, seen: dict[str, str]) -> list[dict]:
    """Per-source age/keyword/seen filters + cap -> per-section round-robin with quota -> dedupe -> number."""
    now = datetime.now(timezone.utc)
    excluded = [k.lower() for k in (filters.get("exclude_keywords") or [])]

    by_section: dict[str, list[list[dict]]] = {}
    for src, items in zip(sources, items_by_source):
        cutoff = now - timedelta(hours=src.get("max_age_hours", filters["max_age_hours"]))
        cap = src.get("cap", filters["per_source_cap"])
        keep = []
        for it in sorted(items, key=lambda x: x["published"], reverse=True):
            if it["published"] < cutoff:
                continue
            if any(k in it["title"].lower() for k in excluded):
                continue
            if _url_key(it["url"]) in seen:
                continue
            keep.append(it)
            if len(keep) >= cap:
                break
        by_section.setdefault(src.get("section", ""), []).append(keep)

    configured = filters.get("section_quota") or {}
    share = max(filters["max_candidates"] // max(len(by_section), 1), 1)
    quota = {sec: configured.get(sec, share) for sec in by_section}
    order = [s for s in configured if s in by_section] + [s for s in by_section if s not in configured]

    merged, urls, titles = [], set(), set()
    for sec in order:
        taken = 0
        for round_ in zip_longest(*by_section[sec]):       # one from each source per round => fair mix
            for it in round_:
                if it is None:
                    continue
                key, title_key = _url_key(it["url"]), it["title"].lower()
                if key in urls or title_key in titles:     # same story across feeds
                    continue
                urls.add(key)
                titles.add(title_key)
                merged.append(it)
                taken += 1
                if taken >= quota[sec]:
                    break
            if taken >= quota[sec]:
                break

    return [
        {
            "id": i,
            "title": it["title"],
            "url": it["url"],
            "snippet": it["snippet"],
            "source": it["source"],
            "section": it["section"],
            "published": it["published"].strftime("%Y-%m-%dT%H:%M:%SZ"),
        }
        for i, it in enumerate(merged, start=1)
    ]


# ---------------------------------------------------------------- public entry

def explore(sources: list[dict], filters: dict | None = None,
            seen_path: str | Path = DEFAULT_SEEN_PATH) -> list[dict]:
    """Fetch all sources (in parallel) and return numbered, filtered candidates. Read-only (see mark_sent)."""
    f = {**DEFAULT_FILTERS, **(filters or {})}
    seen = _load_seen(seen_path)
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as pool:
        fetched = list(pool.map(lambda s: fetch_source(s, f["snippet_chars"]), sources))
    for s, items in zip(sources, fetched):
        log.info("%-26s %3d entries", s.get("name", s.get("url", "?")), len(items))
    return select(sources, fetched, f, seen)


# ---------------------------------------------------------------- feed health check

def check_sources(sources: list[dict]) -> None:
    """Print one row per feed: does it load, how fresh, and does it carry real text?"""
    now = datetime.now(timezone.utc)

    def probe(s):
        parsed, err = _download(s)
        if parsed is None:
            return s, err, None
        ages, snips, conts = [], [], []
        for e in parsed.entries:
            t = _entry_time(e)
            if t:
                ages.append((now - t).total_seconds() / 3600)
            snips.append(len(_clean_text(e.get("summary") or e.get("description") or "")))
            conts.append(len(_clean_text(e["content"][0].get("value", ""))) if e.get("content") else 0)
        return s, "", (len(parsed.entries), min(ages) if ages else None,
                       int(statistics.median(snips)), int(statistics.median(conts)))

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as pool:
        results = list(pool.map(probe, sources))

    print(f"{'section':8} {'source':26} {'status':6} {'items':>5} {'newest_h':>8} {'snip_med':>8} {'full_med':>8}")
    for s, err, stats in results:
        if stats is None:
            print(f"{s.get('section', ''):8} {s.get('name', '?'):26} FAIL   {err[:70]}")
        else:
            n, newest, snip, full = stats
            newest_s = f"{newest:.0f}" if newest is not None else "-"
            print(f"{s.get('section', ''):8} {s.get('name', '?'):26} OK     {n:>5} {newest_s:>8} {snip:>8} {full:>8}")
    print("\nnewest_h = hours since newest item | snip_med = median summary chars | "
          "full_med = median full-content chars (>~1500 means the feed carries whole articles)")


# ---------------------------------------------------------------- isolated test

def _main() -> None:
    ap = argparse.ArgumentParser(description="Run the exploration layer on its own.")
    ap.add_argument("--config", default="config.yaml", help="uses its `sources:` and `filters:` blocks")
    ap.add_argument("--check", action="store_true", help="print a health table for every feed and exit")
    ap.add_argument("--seen", default=DEFAULT_SEEN_PATH, help="seen-URL file (default: data/seen.json)")
    ap.add_argument("--mark-sent", action="store_true",
                    help="record the printed items as sent (default is read-only, so runs are repeatable)")
    args = ap.parse_args()

    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s", stream=sys.stderr)

    import yaml  # only needed here
    cfg = yaml.safe_load(Path(args.config).read_text(encoding="utf-8")) or {}
    sources, filters = cfg.get("sources") or [], cfg.get("filters") or {}
    if not sources:
        sys.exit(f"no `sources:` found in {args.config}")

    if args.check:
        check_sources(sources)
        return

    items = explore(sources, filters, args.seen)
    print(json.dumps(items, indent=2, ensure_ascii=False))     # JSON -> stdout, logs -> stderr

    counts: dict[str, int] = {}
    for i in items:
        counts[i["section"]] = counts.get(i["section"], 0) + 1
    approx_tokens = sum(len(i["title"]) + len(i["snippet"]) for i in items) // 4
    log.info("%d candidates %s, ~%d tokens if sent to the model as title+snippet",
             len(items), counts, approx_tokens)

    if args.mark_sent:
        mark_sent(items, args.seen)
        log.info("marked %d items as sent in %s", len(items), args.seen)


if __name__ == "__main__":
    _main()