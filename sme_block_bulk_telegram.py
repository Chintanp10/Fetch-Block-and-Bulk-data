#!/usr/bin/env python3
"""Scan NSE/BSE block and bulk deals for SME stocks and post updates to Telegram.

Environment variables:
- TELEGRAM_BOT_TOKEN: Bot token from BotFather
- TELEGRAM_CHAT_ID: Target chat/channel ID
Optional:
- LOOKBACK_DAYS (default 1)
- MAX_ROWS_PER_SECTION (default 20)
"""

from __future__ import annotations

import csv
import datetime as dt
import io
import json
import os
import ssl
import sys
import urllib.error
import urllib.parse
import urllib.request
from dataclasses import dataclass
from http.cookiejar import CookieJar
from typing import Iterable

USER_AGENT = (
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36"
)


@dataclass
class DealRow:
    exchange: str
    deal_type: str
    symbol: str
    date: str
    quantity: str
    price: str
    buyer: str
    seller: str


def _today() -> dt.date:
    return dt.datetime.now(dt.timezone.utc).date()


def _http_get_text(url: str, headers: dict[str, str] | None = None, timeout: int = 20, opener=None) -> str:
    req = urllib.request.Request(url, headers=headers or {})
    with (opener.open(req, timeout=timeout) if opener else urllib.request.urlopen(req, timeout=timeout)) as resp:
        raw = resp.read()
        encoding = resp.headers.get_content_charset() or "utf-8"
    return raw.decode(encoding, errors="replace")


def build_nse_opener() -> urllib.request.OpenerDirector:
    jar = CookieJar()
    opener = urllib.request.build_opener(
        urllib.request.HTTPCookieProcessor(jar),
        urllib.request.HTTPSHandler(context=ssl.create_default_context()),
    )
    opener.addheaders = [
        ("User-Agent", USER_AGENT),
        ("Accept", "application/json,text/html,application/xhtml+xml"),
        ("Accept-Language", "en-US,en;q=0.9"),
        ("Referer", "https://www.nseindia.com/"),
    ]
    # Warm-up for cookies.
    opener.open("https://www.nseindia.com", timeout=20).read(1)
    return opener


def parse_json_rows(payload: dict, exchange: str, deal_type: str) -> list[DealRow]:
    data = payload.get("data")
    if not isinstance(data, list):
        return []
    rows: list[DealRow] = []
    for item in data:
        if not isinstance(item, dict):
            continue
        rows.append(
            DealRow(
                exchange=exchange,
                deal_type=deal_type,
                symbol=str(item.get("symbol") or item.get("scripName") or item.get("Security") or "").strip(),
                date=str(item.get("date") or item.get("dt") or item.get("DealDate") or "").strip(),
                quantity=str(item.get("quantityTraded") or item.get("qty") or item.get("Quantity") or "").strip(),
                price=str(item.get("pricePerShare") or item.get("price") or item.get("Price") or "").strip(),
                buyer=str(item.get("clientName") or item.get("buyerName") or item.get("Buyer") or "").strip(),
                seller=str(item.get("sellerName") or item.get("Seller") or "").strip(),
            )
        )
    return rows


def fetch_nse_sme_symbols(opener) -> set[str]:
    # NSE security master. SME symbols are generally in SM/ST series.
    url = "https://nsearchives.nseindia.com/content/equities/EQUITY_L.csv"
    text = _http_get_text(url, headers={"User-Agent": USER_AGENT}, opener=opener)
    out: set[str] = set()
    for row in csv.DictReader(io.StringIO(text)):
        series = (row.get(" SERIES") or row.get("SERIES") or "").strip().upper()
        if series in {"SM", "ST"}:
            sym = (row.get("SYMBOL") or "").strip().upper()
            if sym:
                out.add(sym)
    return out


def fetch_nse_deals(opener, from_date: dt.date, to_date: dt.date) -> list[DealRow]:
    fm = from_date.strftime("%d-%m-%Y")
    to = to_date.strftime("%d-%m-%Y")
    candidates = [
        ("bulk", f"https://www.nseindia.com/api/historicalOR/bulk-deals?from={fm}&to={to}"),
        ("block", f"https://www.nseindia.com/api/historicalOR/block-deals?from={fm}&to={to}"),
        ("bulk", f"https://www.nseindia.com/api/historicalOR/bulk-block-short-deals?optionType=bulk_deals&from={fm}&to={to}"),
        ("block", f"https://www.nseindia.com/api/historicalOR/bulk-block-short-deals?optionType=block_deals&from={fm}&to={to}"),
    ]
    rows: list[DealRow] = []
    for deal_type, url in candidates:
        try:
            body = _http_get_text(url, opener=opener)
            payload = json.loads(body)
        except Exception:
            continue
        rows.extend(parse_json_rows(payload, "NSE", deal_type.upper()))
    # De-duplicate by tuple
    uniq = {(r.exchange, r.deal_type, r.symbol, r.date, r.quantity, r.price, r.buyer, r.seller): r for r in rows}
    return list(uniq.values())


def fetch_bse_sme_symbols() -> set[str]:
    # Best-effort endpoint; if unavailable, return empty set and caller can fallback.
    urls = [
        "https://api.bseindia.com/BseIndiaAPI/api/ListofScripData/w?group=&Scripcode=&industry=&segment=SME",
        "https://api.bseindia.com/BseIndiaAPI/api/ListofScripData/w?Group=&Scripcode=&industry=&segment=SME",
    ]
    out: set[str] = set()
    for url in urls:
        try:
            text = _http_get_text(url, headers={"User-Agent": USER_AGENT, "Referer": "https://www.bseindia.com/"})
            payload = json.loads(text)
        except Exception:
            continue
        for key in ("Table", "Data", "table"):
            vals = payload.get(key)
            if isinstance(vals, list):
                for row in vals:
                    if isinstance(row, dict):
                        sym = str(row.get("SecurityId") or row.get("scrip_cd") or row.get("symbol") or "").strip().upper()
                        if sym:
                            out.add(sym)
        if out:
            return out
    return out


def fetch_bse_deals(as_on: dt.date) -> list[DealRow]:
    ymd = as_on.strftime("%Y%m%d")
    candidates = [
        ("bulk", f"https://api.bseindia.com/BseIndiaAPI/api/MktWatchBulkDealData/w?strType=B&strDate={ymd}"),
        ("block", f"https://api.bseindia.com/BseIndiaAPI/api/MktWatchBulkDealData/w?strType=BL&strDate={ymd}"),
    ]
    rows: list[DealRow] = []
    for deal_type, url in candidates:
        try:
            body = _http_get_text(url, headers={"User-Agent": USER_AGENT, "Referer": "https://www.bseindia.com/"})
            payload = json.loads(body)
        except Exception:
            continue
        table = payload.get("Table") or payload.get("Data") or []
        if not isinstance(table, list):
            continue
        for item in table:
            if not isinstance(item, dict):
                continue
            rows.append(
                DealRow(
                    exchange="BSE",
                    deal_type=deal_type.upper(),
                    symbol=str(item.get("Security") or item.get("scripname") or item.get("ScripName") or "").strip(),
                    date=str(item.get("Date") or item.get("DealDate") or as_on.isoformat()).strip(),
                    quantity=str(item.get("Qty") or item.get("Quantity") or "").strip(),
                    price=str(item.get("Price") or item.get("DealPrice") or "").strip(),
                    buyer=str(item.get("BuyerName") or item.get("ClientName") or "").strip(),
                    seller=str(item.get("SellerName") or "").strip(),
                )
            )
    return rows


def filter_sme(rows: Iterable[DealRow], symbols: set[str]) -> list[DealRow]:
    if not symbols:
        return list(rows)
    return [r for r in rows if r.symbol.upper() in symbols]


def format_rows(title: str, rows: list[DealRow], max_rows: int) -> str:
    if not rows:
        return f"{title}\n- No SME deals found"
    lines = [title]
    for r in rows[:max_rows]:
        lines.append(f"- {r.date} | {r.deal_type} | {r.symbol} | Qty: {r.quantity} | Px: {r.price}")
    if len(rows) > max_rows:
        lines.append(f"- ... and {len(rows) - max_rows} more")
    return "\n".join(lines)


def send_telegram_message(token: str, chat_id: str, text: str) -> None:
    data = urllib.parse.urlencode({"chat_id": chat_id, "text": text}).encode()
    req = urllib.request.Request(
        f"https://api.telegram.org/bot{token}/sendMessage",
        data=data,
        headers={"Content-Type": "application/x-www-form-urlencoded"},
    )
    with urllib.request.urlopen(req, timeout=20) as resp:
        body = resp.read().decode("utf-8", errors="replace")
    payload = json.loads(body)
    if not payload.get("ok"):
        raise RuntimeError(f"Telegram API error: {payload}")


def main() -> int:
    token = os.getenv("TELEGRAM_BOT_TOKEN", "").strip()
    chat_id = os.getenv("TELEGRAM_CHAT_ID", "").strip()
    if not token or not chat_id:
        print("Set TELEGRAM_BOT_TOKEN and TELEGRAM_CHAT_ID first.", file=sys.stderr)
        return 2

    lookback_days = int(os.getenv("LOOKBACK_DAYS", "1"))
    max_rows = int(os.getenv("MAX_ROWS_PER_SECTION", "20"))

    to_date = _today()
    from_date = to_date - dt.timedelta(days=max(lookback_days, 1) - 1)

    notes: list[str] = []
    nse_rows: list[DealRow] = []
    bse_rows: list[DealRow] = []

    # NSE
    try:
        nse_opener = build_nse_opener()
        nse_sme = fetch_nse_sme_symbols(nse_opener)
        nse_rows = filter_sme(fetch_nse_deals(nse_opener, from_date, to_date), nse_sme)
    except Exception as exc:
        notes.append(f"NSE fetch warning: {exc}")

    # BSE
    try:
        bse_sme = fetch_bse_sme_symbols()
        # BSE endpoint is usually day-wise; iterate date range.
        cursor = from_date
        all_rows: list[DealRow] = []
        while cursor <= to_date:
            all_rows.extend(fetch_bse_deals(cursor))
            cursor += dt.timedelta(days=1)
        bse_rows = filter_sme(all_rows, bse_sme)
    except Exception as exc:
        notes.append(f"BSE fetch warning: {exc}")

    header = f"SME block/bulk scan ({from_date.isoformat()} to {to_date.isoformat()})"
    message_parts = [header, "", format_rows("NSE", nse_rows, max_rows), "", format_rows("BSE", bse_rows, max_rows)]
    if notes:
        message_parts.extend(["", "Warnings:"] + [f"- {n}" for n in notes])
    message = "\n".join(message_parts)

    send_telegram_message(token, chat_id, message)
    print("Telegram update sent successfully.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
