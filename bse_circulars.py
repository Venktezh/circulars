"""
BSE India Circulars Fetcher
============================
Fetches circulars for a given date range from BSE.

Two endpoints exist on BSE:
  - Recent  : NoticesCirculars.aspx        (current ~2 weeks)
  - Archive : NoticesCircularsArchive.aspx (older dates)

Usage
-----
  python bse_circulars.py                          # today
  python bse_circulars.py --date 12/01/2026        # single date
  python bse_circulars.py --from 01/01/2026 --to 12/01/2026  # range
  python bse_circulars.py --date 12/01/2026 --out my_file.json

Date format: DD/MM/YYYY
"""

import re
import sys
import json
import time
import random
import argparse
from datetime import datetime, date, timedelta
from html import unescape
import requests

# ── Date config (edit these) ─────────────────────────────────────────────────
FROM_DATE = "01/01/2026"   # DD/MM/YYYY
TO_DATE   = "12/01/2026"   # DD/MM/YYYY  (same as FROM_DATE for a single day)

# ── Constants ────────────────────────────────────────────────────────────────
BASE_URL     = "https://www.bseindia.com"
RECENT_URL   = f"{BASE_URL}/markets/MarketInfo/NoticesCirculars.aspx"
ARCHIVE_URL  = f"{BASE_URL}/markets/MarketInfo/NoticesCircularsArchive.aspx"

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/123.0.0.0 Safari/537.36"
    ),
    "Accept": (
        "text/html,application/xhtml+xml,application/xml;"
        "q=0.9,image/avif,image/webp,*/*;q=0.8"
    ),
    "Accept-Language": "en-US,en;q=0.9",
    "Accept-Encoding": "gzip, deflate, br",
    "Connection":      "keep-alive",
    "Upgrade-Insecure-Requests": "1",
}

NOTICE_PATTERN = re.compile(r"^\d{8}-\d+$")   # e.g. 20260318-49
FMT = "%d/%m/%Y"


# ── Date helpers ──────────────────────────────────────────────────────────────
def parse_date(s: str) -> date:
    return datetime.strptime(s.strip(), FMT).date()

def fmt_date(d: date) -> str:
    return d.strftime(FMT)


# ── HTML helpers ──────────────────────────────────────────────────────────────
def strip_tags(html_frag: str) -> str:
    return unescape(" ".join(re.sub(r"<[^>]+>", " ", html_frag).split()))


def extract_hidden(html: str) -> dict:
    fields = {}
    for name in ["__VIEWSTATE", "__VIEWSTATEGENERATOR",
                 "__EVENTVALIDATION", "__VIEWSTATEENCRYPTED"]:
        m = re.search(rf'id="{re.escape(name)}" value="([^"]*)"', html)
        fields[name] = m.group(1) if m else ""
    return fields


def pdf_url_from_notice(notice_no: str) -> str:
    """Construct the canonical BSE PDF URL from a notice number."""
    return f"{BASE_URL}/downloads/UploadDocs/Notices/{notice_no}/{notice_no}.pdf"


# ── Parsing ───────────────────────────────────────────────────────────────────
def _parse_rows(rows: list) -> list:
    results = []
    for row in rows:
        cells = re.findall(r"<td[^>]*>(.*?)</td>", row, re.DOTALL | re.I)
        if len(cells) < 5:
            continue

        notice_no = strip_tags(cells[0])
        if not NOTICE_PATTERN.match(notice_no):
            continue

        subject  = strip_tags(cells[1])
        segment  = strip_tags(cells[2])
        category = strip_tags(cells[3])
        dept     = strip_tags(cells[4])

        # Try direct PDF href first, then construct from notice_no
        pdf_m = re.search(r'href="([^"]+\.pdf[^"]*)"', cells[1], re.I)
        if not pdf_m:
            for extra in cells[2:]:
                pdf_m = (
                    re.search(r'href="([^"]+\.pdf[^"]*)"', extra, re.I) or
                    re.search(r'value="(https?://[^"]+\.pdf[^"]*)"', extra, re.I)
                )
                if pdf_m:
                    break

        if pdf_m:
            pdf_url = pdf_m.group(1)
            if pdf_url.startswith("/"):
                pdf_url = BASE_URL + pdf_url
        else:
            # POST responses have no direct PDF href — construct from notice_no
            pdf_url = pdf_url_from_notice(notice_no)

        results.append({
            "notice_no":  notice_no,
            "subject":    subject,
            "segment":    segment,
            "category":   category,
            "department": dept,
            "pdf_url":    pdf_url,
        })
    return results


def parse_html(html: str) -> list:
    """
    Extract circulars from any BSE response layout.
    GET  responses use ContentPlaceHolder1_GridView1
    POST responses use ContentPlaceHolder1_GridView2
    """
    for gv_id in ("ContentPlaceHolder1_GridView1",
                  "ContentPlaceHolder1_GridView2"):
        m = re.search(
            rf'id="{re.escape(gv_id)}"[^>]*>(.*?)</table>',
            html, re.DOTALL | re.I,
        )
        if m:
            rows = re.findall(r"<tr[^>]*>(.*?)</tr>",
                              m.group(1), re.DOTALL | re.I)
            results = _parse_rows(rows)
            if results:
                return results

    # Last resort — scan all tables
    results = []
    for tbl in re.findall(r"<table[^>]*>(.*?)</table>",
                          html, re.DOTALL | re.I):
        rows = re.findall(r"<tr[^>]*>(.*?)</tr>", tbl, re.DOTALL | re.I)
        results.extend(_parse_rows(rows))
    return results


def get_pager_pages(html: str) -> list:
    pager_m = re.search(
        r'class="[^"]*GridPager[^"]*".*?<tr[^>]*>(.*?)</tr>',
        html, re.DOTALL | re.I,
    )
    if not pager_m:
        return []
    return re.findall(r'href="[^"]*Page\$(\d+)"', pager_m.group(1), re.I)


# ── HTTP ──────────────────────────────────────────────────────────────────────
def make_session() -> requests.Session:
    s = requests.Session()
    s.headers.update(HEADERS)
    return s


def get_page(session: requests.Session, url: str) -> str:
    resp = session.get(url, params={"id": "0", "txtscripcd": "",
                                    "pagecont": "", "subject": ""},
                       timeout=30)
    resp.raise_for_status()
    return resp.text


def post_filter(session: requests.Session, url: str, prev_html: str,
                from_date: str, to_date: str,
                event_target: str = "", event_arg: str = "") -> str:
    hidden = extract_hidden(prev_html)
    data = {
        "__EVENTTARGET":          event_target,
        "__EVENTARGUMENT":        event_arg,
        "__LASTFOCUS":            "",
        "__VIEWSTATE":            hidden.get("__VIEWSTATE", ""),
        "__VIEWSTATEGENERATOR":   hidden.get("__VIEWSTATEGENERATOR", ""),
        "__VIEWSTATEENCRYPTED":   "",
        "__EVENTVALIDATION":      hidden.get("__EVENTVALIDATION", ""),
        "ctl00$ContentPlaceHolder1$hdnNoticeFilter":       "",
        "ctl00$ContentPlaceHolder1$txtDate":               from_date,
        "ctl00$ContentPlaceHolder1$hidCurrentDate":        "",
        "ctl00$ContentPlaceHolder1$txtTodate":             to_date,
        "ctl00$ContentPlaceHolder1$txtNoticeNo":           "",
        "ctl00$ContentPlaceHolder1$GetQuote1_hdnCode":     "",
        "ctl00$ContentPlaceHolder1$SmartSearch$hdnCode":   "",
        "ctl00$ContentPlaceHolder1$SmartSearch$smartSearch": "",
        "ctl00$ContentPlaceHolder1$hf_scripcode":          "",
        "ctl00$ContentPlaceHolder1$txtSub":                "",
    }
    if not event_target:
        data["ctl00$ContentPlaceHolder1$btnSubmit"] = "Submit"

    resp = session.post(
        url, data=data,
        headers={
            "Content-Type": "application/x-www-form-urlencoded",
            "Referer": url + "?id=0&txtscripcd=&pagecont=&subject=",
            "Origin":  BASE_URL,
        },
        timeout=30,
    )
    resp.raise_for_status()
    return resp.text


# ── Page default date ─────────────────────────────────────────────────────────
def page_default_date(html: str):
    m = (
        re.search(r'name="ctl00\$ContentPlaceHolder1\$txtDate"[^>]*'
                  r'value="(\d{2}/\d{2}/\d{4})"', html) or
        re.search(r'value="(\d{2}/\d{2}/\d{4})"[^>]*'
                  r'id="ContentPlaceHolder1_txtDate"', html)
    )
    return parse_date(m.group(1)) if m else None


# ── Core fetcher ──────────────────────────────────────────────────────────────
def fetch_for_url(session: requests.Session, url: str,
                  from_date: str, to_date: str) -> list:
    print(f"[*] Loading {url.split('/')[-1]} ...")
    get_html    = get_page(session, url)
    default_dt  = page_default_date(get_html)
    from_dt     = parse_date(from_date)
    to_dt       = parse_date(to_date)

    print(f"    Page default date : {fmt_date(default_dt) if default_dt else 'unknown'}")

    # Use GET response directly only when it already shows the exact target day
    use_get = (from_dt == to_dt == default_dt)
    if use_get:
        print("    Date matches page default — using GET response directly.")
        html = get_html
    else:
        time.sleep(random.uniform(1.0, 2.0))
        print(f"    POST filter: {from_date} -> {to_date}")
        html = post_filter(session, url, get_html, from_date, to_date)

    all_rows = parse_html(html)
    print(f"    Page 1: {len(all_rows)} circulars")

    for pg in get_pager_pages(html):
        time.sleep(random.uniform(1.0, 2.0))
        print(f"    Fetching page {pg} ...")
        html = post_filter(
            session, url, html, from_date, to_date,
            event_target="ctl00$ContentPlaceHolder1$GridView1",
            event_arg=f"Page${pg}",
        )
        batch = parse_html(html)
        print(f"    Page {pg}: {len(batch)} circulars")
        all_rows.extend(batch)

    return all_rows


# ── Routing ───────────────────────────────────────────────────────────────────
def get_archive_cutoff(session: requests.Session):
    """Archive page default date = last archived date (exclusive boundary for recent URL)."""
    try:
        html = get_page(session, ARCHIVE_URL)
        return page_default_date(html)
    except Exception:
        return None


def split_range(from_dt: date, to_dt: date, cutoff: date):
    """Split [from_dt, to_dt] into (archive_range, recent_range) at cutoff."""
    archive = (from_dt, min(to_dt, cutoff))       if from_dt <= cutoff else None
    recent  = (max(from_dt, cutoff + timedelta(1)), to_dt) if to_dt > cutoff else None
    return archive, recent


# ── Main ──────────────────────────────────────────────────────────────────────
def main():
    if hasattr(sys.stdout, "reconfigure"):
        sys.stdout.reconfigure(encoding="utf-8", errors="replace")

    parser = argparse.ArgumentParser(
        description="Fetch BSE Notices & Circulars for a date or date range."
    )
    grp = parser.add_mutually_exclusive_group()
    grp.add_argument("--date",  metavar="DD/MM/YYYY", help="Single date (default: today)")
    grp.add_argument("--from",  dest="from_date", metavar="DD/MM/YYYY", help="Start of range")
    parser.add_argument("--to", dest="to_date",   metavar="DD/MM/YYYY", help="End of range")
    parser.add_argument("--out", metavar="FILE",  help="Output JSON filename (auto if omitted)")
    args = parser.parse_args()

    today = date.today()

    def prompt_date(label: str, default: date) -> date:
        default_str = fmt_date(default)
        while True:
            raw = input(f"  {label} [{default_str}]: ").strip()
            if not raw:
                return default
            try:
                return parse_date(raw)
            except ValueError:
                print("  Invalid format — use DD/MM/YYYY")

    if args.date:
        from_dt = to_dt = parse_date(args.date)
    elif args.from_date:
        if not args.to_date:
            parser.error("--to is required when --from is used")
        from_dt = parse_date(args.from_date)
        to_dt   = parse_date(args.to_date)
        if from_dt > to_dt:
            parser.error("--from must be on or before --to")
    else:
        from_dt = parse_date(FROM_DATE)
        to_dt   = parse_date(TO_DATE)

    from_str = fmt_date(from_dt)
    to_str   = fmt_date(to_dt)
    CACHE_FILE = "bse_circulars_cache.json"          # ← single cache file
    out_file = args.out or CACHE_FILE

    print(f"[*] Date range : {from_str}  ->  {to_str}")
    print(f"[*] Output     : {out_file}")

    session = make_session()
    all_circulars = []

    # Detect archive/recent boundary dynamically
    print("[*] Detecting archive cutoff ...")
    cutoff = get_archive_cutoff(session)
    if cutoff:
        print(f"    Archive covers dates up to (and including): {fmt_date(cutoff)}")
    else:
        print("    Could not detect cutoff — treating all dates as recent.")
        cutoff = from_dt - timedelta(days=1)   # no-archive fallback

    time.sleep(random.uniform(0.8, 1.5))

    archive_range, recent_range = split_range(from_dt, to_dt, cutoff)

    if archive_range:
        a_from, a_to = archive_range
        print(f"\n[*] Archive: {fmt_date(a_from)} -> {fmt_date(a_to)}")
        rows = fetch_for_url(session, ARCHIVE_URL, fmt_date(a_from), fmt_date(a_to))
        all_circulars.extend(rows)
        if recent_range:
            time.sleep(random.uniform(1.0, 2.0))

    if recent_range:
        r_from, r_to = recent_range
        print(f"\n[*] Recent: {fmt_date(r_from)} -> {fmt_date(r_to)}")
        rows = fetch_for_url(session, RECENT_URL, fmt_date(r_from), fmt_date(r_to))
        all_circulars.extend(rows)

    # De-duplicate and sort descending by notice_no
    seen, unique = set(), []
    for c in all_circulars:
        if c["notice_no"] not in seen:
            seen.add(c["notice_no"])
            unique.append(c)
    all_circulars = sorted(unique, key=lambda x: x["notice_no"], reverse=True)

    # Display
    sep = "=" * 70
    range_label = from_str if from_str == to_str else f"{from_str} to {to_str}"
    print(f"\n{sep}")
    print(f"  BSE Circulars  {range_label}  (total: {len(all_circulars)})")
    print(sep)
    for i, c in enumerate(all_circulars, 1):
        print(f"\n[{i:03d}] {c['notice_no']}  |  {c['subject']}")
        print(f"      Segment  : {c['segment']}")
        print(f"      Category : {c['category']}")
        print(f"      Dept     : {c['department']}")
        print(f"      PDF      : {c['pdf_url']}")

    # Save — merge new results into the cache file (no duplicates)
    try:
        with open(out_file, encoding="utf-8") as f:
            cache = json.load(f)
    except (FileNotFoundError, json.JSONDecodeError):
        cache = []

    existing_ids = {c["notice_no"] for c in cache}
    added = [c for c in all_circulars if c["notice_no"] not in existing_ids]
    cache.extend(added)
    cache = sorted(cache, key=lambda x: x["notice_no"], reverse=True)

    with open(out_file, "w", encoding="utf-8") as f:
        json.dump(cache, f, indent=2, ensure_ascii=False)
    print(f"\n[+] {len(added)} new  |  {len(cache)} total in cache -> {out_file}")


if __name__ == "__main__":
    main()
