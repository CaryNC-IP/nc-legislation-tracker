#!/usr/bin/env python3
"""
nc_feed_builder.py
==================
Builds a JSON feed of North Carolina General Assembly bills relevant to building
codes, permits, inspections, code-enforcement licensing, and related topics —
pulled DIRECTLY from ncleg.gov (no browser / no search-engine middle layer).

Output: feed.json  (drop next to the tracker HTML; the tracker can load it)

Usage
-----
    python nc_feed_builder.py                      # default: 2025 session, keyword-filtered
    python nc_feed_builder.py --session 2025 -o feed.json
    python nc_feed_builder.py --all                # keep every bill, don't keyword-filter
    python nc_feed_builder.py --serve              # write feed.json AND serve it on :8765 (CORS-enabled)

Scheduling (so it stays current on its own)
-------------------------------------------
Windows Task Scheduler:  run  pythonw nc_feed_builder.py --out "C:\\path\\feed.json"  daily.
macOS/Linux cron:        0 6 * * *  /usr/bin/python3 /path/nc_feed_builder.py --out /path/feed.json

Dependencies
------------
    pip install requests beautifulsoup4

Notes on ncleg.gov
------------------
ncleg publishes a machine-readable master listing per session at:
    https://www.ncleg.gov/Legislation/Legislation/BillsByType/{session}/{chamber}
and a per-bill history/status page at:
    https://www.ncleg.gov/BillLookUp/{session}/{billid}
This script primarily parses the per-session bill index, then enriches each hit
with its latest action from the BillLookUp page. Site markup shifts occasionally;
the two parse points that may need updating are flagged with  # >>> VERIFY  below.
"""

import argparse
import datetime as dt
import json
import re
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed

try:
    import requests
    from bs4 import BeautifulSoup
except ImportError:
    sys.exit("Missing deps. Run:  pip install requests beautifulsoup4")

BASE = "https://www.ncleg.gov"


def _now_iso():
    """Current time as a timezone-aware UTC ISO string (e.g. 2026-08-05T20:21:00+00:00).
    The +00:00 marker lets the tracker's browser JS convert it to each viewer's local time."""
    return dt.datetime.now(dt.timezone.utc).isoformat(timespec="seconds")


WEBSVC = "https://webservices.ncleg.gov"   # ncleg's data service (found via browser Network tab)
HEADERS = {
    # A normal browser UA — this is the difference-maker vs. the in-browser search that was failing.
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                  "(KHTML, like Gecko) Chrome/125.0 Safari/537.36",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9",
}
TIMEOUT = 25
RETRIES = 3

# ---------------------------------------------------------------------------
# Topic taxonomy — mirrors the tracker's filters. Priority tags (p=1) first.
# ---------------------------------------------------------------------------
PRIORITY_TAGS = [
    "Building Code", "Permits & Approvals", "Licensing & Qualifications",
    "Inspections & Enforcement", "Local Gov. Authority",
]

# tag -> list of lowercase keyword/phrase triggers matched against title+summary
TAG_RULES = {
    "Building Code":              ["building code", "state building code", "residential code",
                                   "code council", "single-exit", "single stair", "egress",
                                   "occupancy classification", "fire-resistant", "energizing buildings",
                                   "electrical code", "ungraded lumber"],
    "Permits & Approvals":        ["building permit", "permit applic", "plan review",
                                   "development approval", "sealed", "permitting"],
    "Licensing & Qualifications": ["licens", "qualification board", "coqb", "apprenticeship",
                                   "general contractor", "home inspector", "code official",
                                   "code-enforcement official", "code enforcement official"],
    "Inspections & Enforcement":  ["inspection", "inspector", "code enforcement",
                                   "code-enforcement", "private inspect"],
    "Local Gov. Authority":       ["local government", "cities shall not", "municipal",
                                   "restrict local", "local ordinance", "local act",
                                   "extraterritorial", "planning jurisdiction", "development regulation"],
    "Land Development":           ["land development", "subdivision", "development",
                                   "site plan", "land use"],
    "Zoning & Land Use":          ["zoning", "zoned", "down-zoning", "nonconform",
                                   "etj", "middle housing", "special use permit"],
    "Housing & ADUs":             ["housing", "accessory dwelling", "adu", "dwelling unit",
                                   "affordable housing"],
    "Disaster Recovery":          ["disaster", "helene", "hurricane", "flood"],
    "Fire / OSFM":                ["fire marshal", "fire prevention", "fire and rescue",
                                   "osfm", "state fire"],
    "Stormwater & Environment":   ["stormwater", "erosion", "sediment", "built-upon",
                                   "environmental", "area of environmental concern"],
    "Utilities & Infrastructure": ["water and sewer", "water andsewer", "sewer", "utility",
                                   "utilities", "transportation", "multimodal", "road"],
    "Child Care Facilities":      ["child care", "childcare", "child-care"],
    "Firearms-Related":           ["firearm", "gun ", "gun dealer", "concealed", "handgun",
                                   "door lock exemption"],
    "Budget / Appropriations":    ["appropriation", "base budget", "current operations"],
}

# A bill is KEPT (unless --all) if it matches any tag in this set.
RELEVANT_TAGS = set(PRIORITY_TAGS) | {
    "Land Development", "Zoning & Land Use", "Housing & ADUs", "Disaster Recovery",
    "Fire / OSFM", "Stormwater & Environment", "Utilities & Infrastructure",
    "Child Care Facilities", "Firearms-Related",
}

FIREARMS_TRIGGERS = ["firearm", "gun ", "gun dealer", "concealed", "handgun"]


# ---------------------------------------------------------------------------
# HTTP with retry/backoff
# ---------------------------------------------------------------------------
def fetch(url, session):
    last = None
    for attempt in range(RETRIES):
        try:
            r = session.get(url, headers=HEADERS, timeout=TIMEOUT)
            if r.status_code == 200:
                return r.text
            last = f"HTTP {r.status_code}"
        except requests.RequestException as e:
            last = type(e).__name__
        time.sleep(1.5 * (attempt + 1))  # backoff — polite during high-volume periods
    print(f"  ! could not fetch {url} ({last})", file=sys.stderr)
    return None


# ---------------------------------------------------------------------------
# Parsing
# ---------------------------------------------------------------------------
def classify(text):
    """Return (tags_list, is_firearms). Priority tags ordered first."""
    t = text.lower()
    hits = [tag for tag, kws in TAG_RULES.items() if any(k in t for k in kws)]
    firearms = any(k in t for k in FIREARMS_TRIGGERS)
    if firearms:
        # Firearms bills live only under the Firearms-Related filter (matches the tracker).
        return ["Firearms-Related"], True
    ordered = [x for x in PRIORITY_TAGS if x in hits] + [x for x in hits if x not in PRIORITY_TAGS]
    return ordered, False


def _sentence_case(s):
    """ncleg titles/summaries are ALL CAPS. Convert to readable sentence case:
    lowercase everything except recognized acronyms, then capitalize the first letter."""
    if not s:
        return s
    keep = {"NC", "N.C.", "OSFM", "COQB", "ADU", "ADUS", "ETJ", "CCRC", "DOI",
            "US", "U.S.", "LLC", "HOA", "EMS", "GIS", "SL", "S.L.", "PAH", "ERRC",
            "UNC", "NCDOI", "NCCOQB"}
    out = []
    for w in s.split():
        core = re.sub(r"[^\w.]", "", w).upper()
        if core in keep:
            out.append(w.upper() if w.isupper() else w)
        elif w.isupper():
            out.append(w.lower())
        else:
            out.append(w)  # mixed-case already; leave as-is
    res = " ".join(out)
    # Capitalize first alphabetical character.
    for i, ch in enumerate(res):
        if ch.isalpha():
            res = res[:i] + ch.upper() + res[i + 1:]
            break
    return res


def make_bullets(title, summary, keywords=""):
    """
    Build prioritized bullets. If a long 'AN ACT TO ...' summary exists, split it into
    clauses. Otherwise (ncleg bill pages give only a short title), build bullets from the
    bill's official keywords, which are descriptive (e.g. "BUILDING CODE COUNCIL;
    BUILDING CODES; COUNCILS"). Code/permit/inspection/licensing items are marked p=1.
    """
    p1_kw = ["building code", "permit", "inspection", "inspector", "licens",
             "code-enforcement", "code enforcement", "code official", "qualification board",
             "plan review", "electrical code", "egress", "fire-resistant", "occupancy",
             "certificat"]

    src = (summary or "").strip()
    is_long = bool(re.match(r"\s*AN ACT", src, re.I)) or len(src) > 90
    bullets = []

    if is_long:
        s = re.sub(r"\s+\d+\s+", " ", " " + src + " ")
        s = re.sub(r"\s+", " ", s).strip().rstrip(".")
        body = re.sub(r"^AN ACT (TO|PROVIDING|MANDATING|ESTABLISHING|AUTHORIZING)\s+", "",
                      s, flags=re.I)
        parts = re.split(r";\s+(?:AND\s+)?TO\s+|,?\s+AND\s+TO\s+|;\s+", body, flags=re.I)
        parts = [p.strip(" .,") for p in parts if len(p.strip()) > 8]
        for p in parts[:10]:
            pr = 1 if any(k in p.lower() for k in p1_kw) else 2
            bullets.append({"t": _sentence_case(p), "p": pr})

    if not bullets and keywords:
        # Build from keywords: split on ; , and dedupe, priority items first.
        kws = [k.strip() for k in re.split(r"[;,]", keywords) if len(k.strip()) > 2]
        # Drop procedural noise that isn't topical.
        noise = {"presented", "public", "ratified", "chaptered", "local"}
        kws = [k for k in kws if k.lower() not in noise]
        seen, ordered = set(), []
        for k in kws:
            kl = k.lower()
            if kl in seen:
                continue
            seen.add(kl)
            pr = 1 if any(p in kl for p in p1_kw) else 2
            ordered.append((pr, _sentence_case(k)))
        ordered.sort(key=lambda x: x[0])          # priority items first
        for pr, txt in ordered[:8]:
            bullets.append({"t": txt, "p": pr})

    if not bullets:
        bullets = [{"t": _sentence_case(title) or "See bill text", "p": 2}]
    return bullets


def parse_bill_index(html):
    """
    Parse a bill index/search page into rows. Recognizes bill links in any of ncleg's
    formats: /BillLookUp/{session}/{Hxxx}, ?BillID=Hxxx, /Bills/House/HTML/Hxxx, etc.
    """
    soup = BeautifulSoup(html, "html.parser")
    rows = {}
    # Match a bill id (H123 / S45 / HB123 / SB45) appearing anywhere in an href.
    link_rx = re.compile(r"(?:BillLookUp/\d+/|BillID=|/)([HS]B?\d{1,4})(?:[/?&\"']|$)", re.I)
    for a in soup.find_all("a", href=True):
        href = a["href"]
        m = link_rx.search(href)
        if not m:
            continue
        rawid = m.group(1).upper().replace("B", "")  # HB123 -> H123
        if not re.fullmatch(r"[HS]\d{1,4}", rawid):
            continue
        bid = rawid
        container = a.find_parent(["tr", "li", "div"]) or a.parent
        text = " ".join(container.get_text(" ", strip=True).split())
        title = a.get_text(" ", strip=True)
        if re.fullmatch(r"[HS]B?\d+", title, re.I) or len(title) < 4:
            title = text  # link text was just the number; use the row text
        rows.setdefault(bid, {"id": bid, "title": title[:300], "context": text[:600]})
    return list(rows.values())


def parse_bill_page(html):
    """
    Extract long title, latest action + date, and status from a BillLookUp page.
    # >>> VERIFY: the history/actions table lives in a panel; each action row has a
    # date and description. We take the most recent (last) row as 'lastAction'.
    """
    soup = BeautifulSoup(html, "html.parser")
    text = soup.get_text("\n", strip=True)

    # Long title: usually the "AN ACT ..." line.
    long_title = None
    m = re.search(r"(AN ACT [^\n]{5,600})", text, re.I)
    if m:
        long_title = re.sub(r"\s+", " ", m.group(1)).strip()

    # Session law chapter, if enacted.
    session_law = None
    m = re.search(r"S\.?L\.?\s*(\d{4}-\d+)|Session Law\s*(\d{4}-\d+)|Ch(?:apter)?\.?\s*SL?\s*(\d{4}-\d+)",
                  text, re.I)
    if m:
        session_law = next(g for g in m.groups() if g)

    # Latest action: scan lines shaped like a date + description.
    last_action, last_date = None, None
    date_line = re.compile(r"(\d{1,2}/\d{1,2}/\d{4})\s+(.{4,120})")
    iso_line = re.compile(r"(\d{4}-\d{2}-\d{2})\s+(.{4,120})")
    candidates = []
    for line in text.split("\n"):
        for rx, fmt in ((date_line, "%m/%d/%Y"), (iso_line, "%Y-%m-%d")):
            mm = rx.match(line.strip())
            if mm:
                try:
                    d = dt.datetime.strptime(mm.group(1), fmt).date()
                    candidates.append((d, mm.group(2).strip()))
                except ValueError:
                    pass
    if candidates:
        candidates.sort(key=lambda x: x[0])
        last_date = candidates[-1][0].isoformat()
        last_action = candidates[-1][1]

    # Coarse stage inference from action keywords.
    joined = text.lower()
    if session_law or "ch. sl" in joined or "became law" in joined:
        stage = "law"
    elif "vetoed" in joined:
        stage = "vetoed"
    elif "ch. res" in joined or "ratified" in joined:
        stage = "passed_both"
    elif "passed 3rd reading" in joined and "senate" in joined and "house" in joined:
        stage = "passed_both"
    elif "passed 3rd reading" in joined:
        stage = "passed_house"       # coarse; refine if chamber known
    elif "ref to com" in joined or "committee" in joined:
        stage = "committee"
    elif "filed" in joined:
        stage = "filed"
    else:
        stage = "filed"

    return {
        "long_title": long_title,
        "sessionLaw": session_law,
        "lastAction": last_action,
        "lastActionDate": last_date,
        "stage": stage,
    }


# ---------------------------------------------------------------------------
# Bill list from the ncleg web service (AllBills/{session})
# ---------------------------------------------------------------------------
def _first(d, *names):
    """Return the first present, non-empty value among candidate key names (case-insensitive)."""
    if not isinstance(d, dict):
        return None
    lower = {k.lower(): v for k, v in d.items()}
    for n in names:
        v = lower.get(n.lower())
        if v not in (None, "", []):
            return v
    return None


def _norm_id(raw):
    """'H 768' / 'HB768' / 'House Bill 768' -> 'H768'."""
    if raw is None:
        return None
    t = str(raw).upper()
    m = re.search(r"\b([HS])(?:OUSE|ENATE)?\s*B?(?:ILL)?\s*0*(\d{1,4})\b", t)
    if m:
        return m.group(1) + m.group(2)
    m = re.search(r"\b([HS])\s*0*(\d{1,4})\b", t)
    return (m.group(1) + m.group(2)) if m else None


def _to_iso(val):
    """Normalize a date value (various formats or ISO datetime) to YYYY-MM-DD, or None."""
    if not val:
        return None
    sval = str(val).strip()
    if not sval:
        return None
    # Already ISO date or datetime.
    m = re.match(r"(\d{4})-(\d{2})-(\d{2})", sval)
    if m:
        return f"{m.group(1)}-{m.group(2)}-{m.group(3)}"
    # .NET style /Date(...)/ epoch millis.
    m = re.search(r"/Date\((\d+)", sval)
    if m:
        try:
            return dt.datetime.utcfromtimestamp(int(m.group(1)) / 1000).date().isoformat()
        except (ValueError, OSError):
            return None
    for fmt in ("%m/%d/%Y", "%m/%d/%y", "%b %d, %Y", "%B %d, %Y", "%Y/%m/%d"):
        try:
            return dt.datetime.strptime(sval[:len(fmt) + 4], fmt).date().isoformat()
        except ValueError:
            continue
    m = re.search(r"(\d{1,2})/(\d{1,2})/(\d{4})", sval)
    if m:
        try:
            return dt.date(int(m.group(3)), int(m.group(1)), int(m.group(2))).isoformat()
        except ValueError:
            return None
    return None


def fetch_all_bills(session, s):
    """
    Pull the full bill list from https://webservices.ncleg.gov/AllBills/{session}.
    Handles JSON or XML. Returns list of {id,title,context,...} rows.
    On unexpected shapes, prints a diagnostic sample and returns [].
    """
    url = f"{WEBSVC}/AllBills/{session}"
    hdrs = dict(HEADERS)
    hdrs["Accept"] = "application/json, text/xml, application/xml;q=0.9, */*;q=0.8"
    text = None
    for attempt in range(RETRIES):
        try:
            r = s.get(url, headers=hdrs, timeout=TIMEOUT)
            if r.status_code == 200:
                text = r.text
                ctype = r.headers.get("Content-Type", "")
                break
            print(f"  ! {url} -> HTTP {r.status_code}", file=sys.stderr)
        except requests.RequestException as e:
            print(f"  ! {url} -> {type(e).__name__}", file=sys.stderr)
        time.sleep(1.5 * (attempt + 1))
    if not text:
        return []

    records = []

    # --- Try JSON first ---
    parsed = None
    try:
        parsed = json.loads(text)
    except ValueError:
        parsed = None

    def collect_from_obj(obj):
        """Walk a JSON structure and pull out anything that looks like a bill record.
        Handles ncleg's AllBills shape: {"chamber":"H","billNumber":768}."""
        out = []
        def walk(x):
            if isinstance(x, dict):
                # ncleg AllBills: chamber ('H'/'S') + billNumber (int).
                ch = _first(x, "chamber", "Chamber")
                num = _first(x, "billNumber", "BillNumber", "Number")
                bid = None
                if ch and num is not None and str(ch).strip().upper() in ("H", "S"):
                    bid = str(ch).strip().upper() + str(num).strip()
                if not bid:
                    bid = _norm_id(_first(x, "BillID", "Bill", "billID"))
                title = _first(x, "ShortTitle", "Title", "Caption", "shortTitle", "Description")
                if bid:
                    out.append({"id": bid, "title": str(title or bid),
                                "context": str(title or ""),
                                "chamber": ch,
                                "lastAction": _first(x, "LatestAction", "LastAction", "Action"),
                                "lastActionDate": _first(x, "LatestActionDate", "ActionDate", "LastActionDate"),
                                "introduced": _first(x, "FiledDate", "IntroducedDate", "DateIntroduced")})
                for v in x.values():
                    walk(v)
            elif isinstance(x, list):
                for v in x:
                    walk(v)
        walk(obj)
        return out

    if parsed is not None:
        records = collect_from_obj(parsed)

    # --- Fall back to XML ---
    if not records and ("<" in text[:200]):
        try:
            soup = BeautifulSoup(text, "xml")
        except Exception:
            soup = BeautifulSoup(text, "html.parser")
        # Each bill is typically its own element; scan all tags for a bill-id child/attr.
        for el in soup.find_all(True):
            blob = " ".join(el.get_text(" ", strip=True).split()) if hasattr(el, "get_text") else ""
            bid = _norm_id(el.get("BillID") if el.has_attr and el.has_attr("BillID") else None) \
                  or _norm_id(blob[:20])
            if not bid:
                continue
            records.append({"id": bid, "title": blob[:300], "context": blob[:600],
                            "chamber": None, "lastAction": None,
                            "lastActionDate": None, "introduced": None})

    # De-dupe by id.
    dedup = {}
    for r in records:
        if r["id"] and r["id"] not in dedup:
            dedup[r["id"]] = r

    if not dedup:
        print(f"\n--- DIAGNOSTIC: {url} returned data but no bills parsed ---", file=sys.stderr)
        print(f"Content-Type: {ctype}", file=sys.stderr)
        print("First 2000 chars:\n" + text[:2000], file=sys.stderr)
        return []

    return list(dedup.values())


# ---------------------------------------------------------------------------
# Build
# ---------------------------------------------------------------------------
_detail_diag_done = [False]

def _stage_from_action(action, session_law):
    a = (action or "").lower()
    if session_law or "ch. sl" in a or "became law" in a or "signed by gov" in a:
        return "law"
    if "veto" in a:
        return "vetoed"
    if "ratified" in a or "ch. res" in a:
        return "passed_both"
    if "passed 3rd reading" in a or "passed third reading" in a:
        return "passed_house"
    if "com" in a and ("ref" in a or "re-ref" in a):
        return "committee"
    if "filed" in a:
        return "filed"
    return "filed"


def fetch_bill_detail(session, bid, s):
    """
    Fetch a single bill's detail by scraping its ncleg.gov page:
        https://www.ncleg.gov/BillLookUp/{session}/{bid}
    Individual bill pages ARE server-rendered (unlike the JS-loaded master list),
    so the title, last action, keywords, etc. are present in the HTML.
    Structure confirmed from a real page: label/value pairs in <div class="misc-info-label">,
    short title in the first PDF <a> link, session law in <title>.
    Returns a normalized dict, or {}.
    """
    url = f"{BASE}/BillLookUp/{session}/{bid}"
    html = fetch(url, s)
    if not html:
        return {}
    soup = BeautifulSoup(html, "html.parser")
    out = {"long_title": "", "short_title": "", "keywords": "",
           "lastAction": None, "lastActionDate": None, "sessionLaw": None, "stage": None}

    # Session law from the <title> tag.
    t = soup.find("title")
    ttl = t.get_text(" ", strip=True) if t else ""
    m = re.search(r"SL\s*(\d{4}-\d+)", ttl)
    if m:
        out["sessionLaw"] = m.group(1)

    # Short title: the first PDF link under the header.
    a = soup.find("a", href=re.compile(r"/Bills/.*\.pdf", re.I))
    if a:
        out["short_title"] = a.get_text(" ", strip=True).rstrip(".")

    # Label/value pairs (Last Action, Sponsors, Keywords, Statutes, ...).
    labels = {}
    for lab in soup.find_all("div", class_="misc-info-label"):
        key = lab.get_text(" ", strip=True).rstrip(":").lower()
        val = lab.find_next_sibling("div")
        if val:
            labels[key] = val.get_text(" ", strip=True)

    la = labels.get("last action")
    if la:
        out["lastAction"] = la
        out["lastActionDate"] = _to_iso(la)   # _to_iso pulls the m/d/Y out of the string
    out["keywords"] = labels.get("keywords", "")
    out["long_title"] = out["short_title"]    # bill pages carry the short title; that's the display title

    # Stage from last-action text (+ session law).
    al = (la or "").lower()
    if out["sessionLaw"] or "ch. sl" in al or "became law" in al:
        out["stage"] = "law"
    elif "veto" in al:
        out["stage"] = "vetoed"
    elif "ratified" in al or "ch. res" in al:
        out["stage"] = "passed_both"
    elif "passed 3rd reading" in al or "passed third reading" in al:
        out["stage"] = "passed_house"
    elif "com" in al and ("ref" in al or "re-ref" in al):
        out["stage"] = "committee"
    else:
        out["stage"] = "filed"

    return out


def build(session, keep_all, workers=6):
    s = requests.Session()
    fetch(f"{BASE}/", s)   # prime session cookies

    raw = {}
    for row in fetch_all_bills(session, s):
        raw.setdefault(row["id"], row)
    print(f"AllBills/{session} yielded {len(raw)} bills.")

    if not raw:
        print("No bills returned from the web service. See any DIAGNOSTIC output above.",
              file=sys.stderr)
        return None

    print(f"Enriching + filtering {len(raw)} bills…")

    # One-time diagnostic: show the first row so we can see exactly what AllBills provides.
    sample = next(iter(raw.values()))
    print("Sample list row:", json.dumps({k: (str(v)[:80] if v else v) for k, v in sample.items()}))

    bills = []

    def process(row):
        bid = row["id"]
        list_title = (row.get("title") or "").strip()
        detail = fetch_bill_detail(session, bid, s)
        # One-time diagnostic: dump the first bill's raw detail so field names are visible
        # in the log even if my mapping missed something.
        if not _detail_diag_done[0]:
            _detail_diag_done[0] = True
            print("Sample detail parsed:", json.dumps({
                k: (str(v)[:90] if v else v) for k, v in (detail or {}).items()}), file=sys.stderr)
        info = detail or {}
        # Prefer the detail's short title for display; fall back to list title or id.
        title = (info.get("short_title") or list_title or bid).strip()
        long_title = info.get("long_title") or title
        keywords = info.get("keywords") or ""
        # Classify on title + long title + keywords (ncleg keywords are rich, e.g.
        # "BUILDING CODE COUNCIL; BUILDING CODES").
        tags, firearms = classify(f"{title} {long_title} {keywords}")
        chamber = row.get("chamber")
        chamber = ("Senate" if str(chamber).lower().startswith("s") else "House") if chamber else \
                  ("House" if bid.startswith("H") else "Senate")
        la = info.get("lastAction") or row.get("lastAction")
        lad = _to_iso(info.get("lastActionDate") or row.get("lastActionDate"))
        stage = info.get("stage") or _stage_from_action(la, info.get("sessionLaw"))
        return {
            "id": bid,
            "chamber": chamber,
            "title": _sentence_case(title.rstrip(".")),
            "summary": _sentence_case(long_title),
            "bullets": make_bullets(title, long_title, keywords),
            "tags": tags,
            "firearms": firearms,
            "stage": stage,
            "sessionLaw": info.get("sessionLaw"),
            "lastAction": la,
            "lastActionDate": lad,
            "introduced": _to_iso(row.get("introduced")),
            "checkedAt": _now_iso(),
            "discovered": True,
        }

    with ThreadPoolExecutor(max_workers=workers) as ex:
        futs = {ex.submit(process, r): r for r in raw.values()}
        for i, fut in enumerate(as_completed(futs), 1):
            try:
                b = fut.result()
            except Exception as e:
                print(f"  ! {futs[fut]['id']} failed: {e}", file=sys.stderr)
                continue
            if keep_all or (set(b["tags"]) & RELEVANT_TAGS):
                bills.append(b)
            if i % 25 == 0:
                print(f"  …processed {i}/{len(raw)}")

    # Sort by most recent activity.
    bills.sort(key=lambda b: (b["lastActionDate"] or "0000-00-00"), reverse=True)
    return bills


def main():
    ap = argparse.ArgumentParser(description="Build feed.json of NC building/permit legislation from ncleg.gov")
    ap.add_argument("--session", default="2025", help="Session year (default 2025)")
    ap.add_argument("-o", "--out", default="feed.json", help="Output path (default feed.json)")
    ap.add_argument("--all", action="store_true", help="Keep every bill (skip topic filtering)")
    ap.add_argument("--serve", action="store_true", help="After building, serve the file on :8765 with CORS")
    ap.add_argument("--port", type=int, default=8765)
    args = ap.parse_args()

    print(f"Building feed for {args.session} session from {BASE} …")
    bills = build(args.session, keep_all=args.all)
    if not bills:
        # ncleg.gov unreachable or markup changed. Do NOT overwrite a good existing feed —
        # leave the previous feed.json in place so the site keeps showing the last good data.
        import os
        if os.path.exists(args.out):
            print("Fetch returned nothing; keeping the existing feed.json unchanged.", file=sys.stderr)
            sys.exit(0)
        # No prior feed at all: write an empty-but-valid feed so the page has something to read.
        payload = {
            "generatedAt": _now_iso(),
            "session": args.session, "source": f"{BASE}/Legislation",
            "count": 0, "bills": [],
            "note": "No bills fetched on this run — ncleg.gov may have been unreachable.",
        }
        with open(args.out, "w", encoding="utf-8") as f:
            json.dump(payload, f, ensure_ascii=False, indent=2)
        print("Wrote empty placeholder feed.json.")
        sys.exit(0)

    payload = {
        "generatedAt": _now_iso(),
        "session": args.session,
        "source": f"{BASE}/Legislation",
        "count": len(bills),
        "bills": bills,
    }
    with open(args.out, "w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, indent=2)
    print(f"Wrote {len(bills)} bills to {args.out}")

    if args.serve:
        serve(args.out, args.port)


def serve(path, port):
    """Tiny CORS-enabled static server so the tracker HTML can fetch() the feed locally."""
    import http.server, socketserver, os
    directory = os.path.dirname(os.path.abspath(path)) or "."

    class Handler(http.server.SimpleHTTPRequestHandler):
        def __init__(self, *a, **kw):
            super().__init__(*a, directory=directory, **kw)
        def end_headers(self):
            self.send_header("Access-Control-Allow-Origin", "*")
            self.send_header("Cache-Control", "no-store")
            super().end_headers()
        def log_message(self, *a):
            pass

    with socketserver.TCPServer(("127.0.0.1", port), Handler) as httpd:
        print(f"Serving {directory} at http://127.0.0.1:{port}/  (Ctrl+C to stop)")
        print(f"  feed URL: http://127.0.0.1:{port}/{os.path.basename(path)}")
        try:
            httpd.serve_forever()
        except KeyboardInterrupt:
            print("\nStopped.")


if __name__ == "__main__":
    main()
