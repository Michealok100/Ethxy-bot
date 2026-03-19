"""
Ethereum Wallet Analyzer Telegram Bot  ·  v2.0
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
Fully backward-compatible upgrade over v1 (bot__4_.py).

NEW COMMANDS
  /trace   <wallet>          – top-1 recipient (original behaviour, untouched)
  /top     <wallet> [N]      – top-N recipients ranked by value (default 10)
  /profile <wallet>          – deep behaviour profile + risk score
  /tokens  <wallet>          – ERC-20 token breakdown per wallet
  /time    <wallet> [h|d]    – hourly or daily volume buckets
  /risk    <wallet>          – suspicious-activity report

PRESERVED FROM v1
  /start                     – welcome message (extended)
  /trace                     – single top-recipient (unchanged output)
  USD pricing, wallet stats, InlineKeyboard "Copy / Etherscan" buttons
"""

from __future__ import annotations

import json
import logging
import os
import re
import statistics
import time
from collections import defaultdict
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any

import requests
from dotenv import load_dotenv
from telegram import InlineKeyboardButton, InlineKeyboardMarkup, Update
from telegram.constants import ParseMode
from telegram.ext import (
    Application,
    CallbackQueryHandler,
    CommandHandler,
    ContextTypes,
)

# ── Environment ───────────────────────────────────────────────────────────────
load_dotenv()

TELEGRAM_BOT_TOKEN: str = os.environ.get("TELEGRAM_BOT_TOKEN", "")
ETHERSCAN_API_KEY:  str = os.environ.get("ETHERSCAN_API_KEY",  "")
RISKY_ADDRESSES_RAW: str = os.environ.get("RISKY_ADDRESSES", "")   # comma-separated

if not TELEGRAM_BOT_TOKEN:
    raise SystemExit(
        "\n❌ ERROR: TELEGRAM_BOT_TOKEN is not set.\n"
        "  • Local: add it to your .env file\n"
        "  • Railway: add it in the Variables tab\n"
    )
if not ETHERSCAN_API_KEY:
    raise SystemExit(
        "\n❌ ERROR: ETHERSCAN_API_KEY is not set.\n"
        "  • Local: add it to your .env file\n"
        "  • Railway: add it in the Variables tab\n"
    )

KNOWN_RISKY_ADDRESSES: set[str] = {
    a.strip().lower()
    for a in RISKY_ADDRESSES_RAW.split(",")
    if a.strip()
}

# ── Constants ─────────────────────────────────────────────────────────────────
ETHERSCAN_BASE_URL  = "https://api.etherscan.io/v2/api"
COINGECKO_PRICE_URL = "https://api.coingecko.com/api/v3/simple/price"
MAX_TRANSACTIONS    = 1000
ETH_ADDRESS_RE      = re.compile(r"^0x[0-9a-fA-F]{40}$")
SHORT_CYCLE_SECS    = 300       # 5 min  → "rapid cycle"
MICRO_TX_ETH        = 0.001     # below this = micro-transaction
SPIKE_MULTIPLIER    = 3.0       # bucket > N× avg → spike
DEFAULT_TOP_N       = 10

# ── Logging ───────────────────────────────────────────────────────────────────
logging.basicConfig(
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
    level=logging.INFO,
)
logger = logging.getLogger(__name__)


# ══════════════════════════════════════════════════════════════════════════════
#  ETH PRICE  (cached 60 s – identical to v1)
# ══════════════════════════════════════════════════════════════════════════════

_price_cache: dict = {"price": 0.0, "updated": 0.0}
_PRICE_TTL = 60


def fetch_eth_price_usd() -> float:
    now = time.time()
    if now - _price_cache["updated"] < _PRICE_TTL and _price_cache["price"]:
        return _price_cache["price"]
    try:
        resp = requests.get(
            COINGECKO_PRICE_URL,
            params={"ids": "ethereum", "vs_currencies": "usd"},
            timeout=10,
        )
        resp.raise_for_status()
        price = float(resp.json()["ethereum"]["usd"])
        _price_cache.update({"price": price, "updated": now})
        logger.info("ETH price refreshed: $%.2f", price)
        return price
    except Exception as exc:
        logger.warning("Could not fetch ETH price: %s", exc)
        return _price_cache["price"] or 0.0


def eth_to_usd(eth: float, price: float) -> float:
    return eth * price


def fmt_usd(usd: float) -> str:
    if usd >= 1_000_000:
        return f"${usd/1_000_000:.2f}M"
    if usd >= 1_000:
        return f"${usd:,.0f}"
    return f"${usd:.2f}"


def fmt_eth(val: float) -> str:
    return f"{val:.4f}".rstrip("0").rstrip(".")


# ══════════════════════════════════════════════════════════════════════════════
#  DATA LAYER  (v1 functions preserved; ERC-20 fetcher added)
# ══════════════════════════════════════════════════════════════════════════════

def _etherscan_get(params: dict, timeout: int = 20) -> dict:
    """Shared Etherscan GET helper with unified error handling."""
    try:
        resp = requests.get(ETHERSCAN_BASE_URL, params=params, timeout=timeout)
        resp.raise_for_status()
    except requests.exceptions.Timeout:
        raise RuntimeError("Etherscan API timed out. Please try again shortly.")
    except requests.exceptions.RequestException as exc:
        raise RuntimeError(f"Network error contacting Etherscan: {exc}")
    return resp.json()


def fetch_transactions(wallet: str) -> list[dict]:
    """Fetch normal ETH transactions (v1 – unchanged signature)."""
    params = {
        "chainid": "1", "module": "account", "action": "txlist",
        "address": wallet, "startblock": "0", "endblock": "99999999",
        "page": "1", "offset": str(MAX_TRANSACTIONS), "sort": "desc",
        "apikey": ETHERSCAN_API_KEY,
    }
    logger.info("Fetching transactions for %s", wallet)
    payload = _etherscan_get(params)
    if payload.get("status") == "0":
        msg = payload.get("message", "")
        result = payload.get("result", "")
        if "No transactions found" in (msg + str(result)):
            return []
        raise RuntimeError(f"Etherscan error: {payload.get('result', msg)}")
    if payload.get("status") != "1":
        raise RuntimeError(f"Unexpected Etherscan response: {payload}")
    return payload["result"]


def fetch_erc20_transfers(wallet: str) -> list[dict]:
    """Fetch ERC-20 token transfer events for `wallet`."""
    params = {
        "chainid": "1", "module": "account", "action": "tokentx",
        "address": wallet, "startblock": "0", "endblock": "99999999",
        "page": "1", "offset": str(MAX_TRANSACTIONS), "sort": "desc",
        "apikey": ETHERSCAN_API_KEY,
    }
    try:
        payload = _etherscan_get(params)
        if payload.get("status") == "1":
            return payload["result"]
    except Exception as exc:
        logger.warning("ERC-20 fetch failed: %s", exc)
    return []


def fetch_wallet_stats(address: str) -> dict:
    """Balance + sent/received stats (v1 – unchanged)."""
    # balance
    bal_params = {
        "chainid": "1", "module": "account", "action": "balance",
        "address": address, "tag": "latest", "apikey": ETHERSCAN_API_KEY,
    }
    try:
        bp = _etherscan_get(bal_params)
        current_balance_eth = (
            int(bp["result"]) / 1e18 if bp.get("status") == "1" else None
        )
    except Exception:
        current_balance_eth = None

    # tx list
    tx_params = {
        "chainid": "1", "module": "account", "action": "txlist",
        "address": address, "startblock": "0", "endblock": "99999999",
        "page": "1", "offset": str(MAX_TRANSACTIONS), "sort": "desc",
        "apikey": ETHERSCAN_API_KEY,
    }
    try:
        tp = _etherscan_get(tx_params)
        txns = tp["result"] if tp.get("status") == "1" else []
    except Exception:
        txns = []

    addr_lower = address.lower()
    total_received_wei = sum(
        int(tx.get("value", 0)) for tx in txns
        if tx.get("to", "").lower() == addr_lower and tx.get("isError") != "1"
    )
    total_sent_wei = sum(
        int(tx.get("value", 0)) for tx in txns
        if tx.get("from", "").lower() == addr_lower and tx.get("isError") != "1"
    )
    return {
        "current_balance_eth": current_balance_eth,
        "total_received_eth":  total_received_wei / 1e18 if txns else None,
        "total_sent_eth":      total_sent_wei      / 1e18 if txns else None,
        "sample_size":         len(txns),
    }


# ══════════════════════════════════════════════════════════════════════════════
#  ANALYSIS LAYER v2
# ══════════════════════════════════════════════════════════════════════════════

# ── data containers ───────────────────────────────────────────────────────────

@dataclass
class TokenFlow:
    token_address: str
    token_symbol:  str
    inflow:   float = 0.0
    outflow:  float = 0.0
    tx_count: int   = 0

    @property
    def net(self) -> float:
        return self.inflow - self.outflow


@dataclass
class TimeBucket:
    timestamp: str
    volume:   float = 0.0
    tx_count: int   = 0


@dataclass
class WalletProfile:
    address: str

    total_inflow:  float = 0.0
    total_outflow: float = 0.0
    tx_count_in:   int   = 0
    tx_count_out:  int   = 0
    tx_amounts:    list[float] = field(default_factory=list)

    first_seen: datetime | None = None
    last_seen:  datetime | None = None

    # {token_address -> TokenFlow}
    token_flows: dict[str, TokenFlow] = field(default_factory=dict)

    # {bucket_key -> TimeBucket}
    hourly_buckets: dict[str, TimeBucket] = field(default_factory=dict)
    daily_buckets:  dict[str, TimeBucket] = field(default_factory=dict)

    # risk signals
    rapid_cycles:       int  = 0
    micro_tx_count:     int  = 0
    spike_detected:     bool = False
    risky_interactions: list[str] = field(default_factory=list)

    # ── derived ──────────────────────────────────────────────────────────────

    @property
    def net_balance_change(self) -> float:
        return self.total_inflow - self.total_outflow

    @property
    def avg_tx_size(self) -> float:
        return statistics.mean(self.tx_amounts) if self.tx_amounts else 0.0

    @property
    def tx_count(self) -> int:
        return self.tx_count_in + self.tx_count_out

    @property
    def active_seconds(self) -> float:
        if self.first_seen and self.last_seen:
            return (self.last_seen - self.first_seen).total_seconds()
        return 0.0

    @property
    def tx_per_hour(self) -> float:
        hours = self.active_seconds / 3600
        return self.tx_count / hours if hours > 0 else float(self.tx_count)

    def _coeff_variation(self) -> float:
        if len(self.tx_amounts) < 2:
            return 0.0
        mean = statistics.mean(self.tx_amounts)
        return (statistics.stdev(self.tx_amounts) / mean) if mean else 0.0

    @property
    def classification(self) -> str:
        if self.tx_per_hour > 60 and self._coeff_variation() < 0.1:
            return "bot-like"
        if self.total_inflow > 10 and self.net_balance_change > 0.8 * self.total_inflow:
            return "accumulator"
        if self.total_outflow > self.total_inflow * 1.5:
            return "distributor"
        if self.total_inflow > 50:
            return "whale"
        return "retail"

    @property
    def most_interacted_tokens(self) -> list[str]:
        return sorted(
            self.token_flows,
            key=lambda a: self.token_flows[a].tx_count,
            reverse=True,
        )[:5]

    @property
    def risk_flags(self) -> list[str]:
        flags: list[str] = []
        if self.rapid_cycles >= 3:
            flags.append("rapid_inflow_outflow_cycles")
        if self.micro_tx_count > 20:
            flags.append("high_frequency_microtransactions")
        if self.risky_interactions:
            flags.append(f"interacted_with_risky_addresses({len(self.risky_interactions)})")
        if self.spike_detected:
            flags.append("activity_spike_detected")
        if self.classification == "bot-like":
            flags.append("bot_like_behavior")
        return flags

    @property
    def risk_score(self) -> int:
        score = 0
        score += min(self.rapid_cycles * 10, 30)
        score += min(self.micro_tx_count // 2, 20)
        score += len(self.risky_interactions) * 15
        score += 10 if self.spike_detected else 0
        score += 10 if self.classification == "bot-like" else 0
        return min(score, 100)

    def to_dict(self) -> dict[str, Any]:
        return {
            "address":          self.address,
            "classification":   self.classification,
            "inflow_eth":       round(self.total_inflow, 6),
            "outflow_eth":      round(self.total_outflow, 6),
            "net_balance_eth":  round(self.net_balance_change, 6),
            "tx_count":         self.tx_count,
            "tx_count_in":      self.tx_count_in,
            "tx_count_out":     self.tx_count_out,
            "avg_tx_size_eth":  round(self.avg_tx_size, 6),
            "tx_per_hour":      round(self.tx_per_hour, 4),
            "first_seen":       self.first_seen.isoformat() if self.first_seen else None,
            "last_seen":        self.last_seen.isoformat()  if self.last_seen  else None,
            "token_breakdown": {
                addr: {
                    "symbol":   tf.token_symbol,
                    "inflow":   round(tf.inflow,  6),
                    "outflow":  round(tf.outflow, 6),
                    "net":      round(tf.net,     6),
                    "tx_count": tf.tx_count,
                }
                for addr, tf in self.token_flows.items()
            },
            "most_interacted_tokens": self.most_interacted_tokens,
            "risk_score":  self.risk_score,
            "risk_flags":  self.risk_flags,
            "hourly_volume": {
                k: {"volume": round(v.volume, 6), "tx_count": v.tx_count}
                for k, v in sorted(self.hourly_buckets.items())
            },
            "daily_volume": {
                k: {"volume": round(v.volume, 6), "tx_count": v.tx_count}
                for k, v in sorted(self.daily_buckets.items())
            },
        }


# ── core engine ───────────────────────────────────────────────────────────────

class WalletAnalyzer:
    """
    Multi-wallet analysis engine.
    · results()[0]  →  backward-compatible top-recipient
    · results(n=10) →  top-N ranked list
    """

    def __init__(
        self,
        top_n: int = DEFAULT_TOP_N,
        risky_addresses: set[str] | None = None,
    ):
        self.top_n = top_n
        self.risky_addresses: set[str] = risky_addresses or KNOWN_RISKY_ADDRESSES
        self._profiles: dict[str, WalletProfile] = {}
        self._last_inflow_ts: dict[str, datetime] = {}

    # ── public ───────────────────────────────────────────────────────────────

    def ingest_eth_tx(self, tx: dict, perspective_wallet: str) -> None:
        """
        Process one normal ETH transaction from Etherscan txlist.
        `perspective_wallet` is the address that was originally queried.
        """
        sender    = (tx.get("from") or "").lower()
        recipient = (tx.get("to")   or "").lower()
        if not recipient or tx.get("isError") == "1":
            return

        value_eth = int(tx.get("value", 0)) / 1e18
        ts        = self._parse_ts(tx.get("timeStamp"))

        self._record(
            sender=sender,
            recipient=recipient,
            value=value_eth,
            ts=ts,
            token_addr="0xNATIVE",
            token_sym="ETH",
        )

    def ingest_erc20_tx(self, tx: dict) -> None:
        """Process one ERC-20 transfer row from Etherscan tokentx."""
        sender    = (tx.get("from") or "").lower()
        recipient = (tx.get("to")   or "").lower()
        if not recipient:
            return

        decimals   = int(tx.get("tokenDecimal", 18) or 18)
        raw_value  = int(tx.get("value", 0) or 0)
        value      = raw_value / (10 ** decimals)
        ts         = self._parse_ts(tx.get("timeStamp"))
        token_addr = (tx.get("contractAddress") or "").lower()
        token_sym  = tx.get("tokenSymbol", "UNKNOWN")

        self._record(
            sender=sender,
            recipient=recipient,
            value=value,
            ts=ts,
            token_addr=token_addr,
            token_sym=token_sym,
        )

    def results(
        self,
        n:       int | None = None,
        rank_by: str = "inflow",    # "inflow" | "tx_count"
    ) -> list[WalletProfile]:
        self._detect_spikes()
        limit   = n if n is not None else self.top_n
        key_fn  = (
            (lambda p: p.total_inflow)
            if rank_by == "inflow"
            else (lambda p: p.tx_count_in)
        )
        return sorted(self._profiles.values(), key=key_fn, reverse=True)[:limit]

    def to_json_report(self, n: int | None = None, rank_by: str = "inflow") -> dict[str, Any]:
        top = self.results(n=n, rank_by=rank_by)
        return {
            "meta": {
                "generated_at":        datetime.now(timezone.utc).isoformat(),
                "total_wallets_seen":  len(self._profiles),
                "top_n":               len(top),
                "ranked_by":           rank_by,
            },
            "top_wallets": [p.to_dict() for p in top],
            "summary":     self._summary(top),
        }

    # ── internals ─────────────────────────────────────────────────────────────

    def _get_profile(self, address: str) -> WalletProfile:
        if address not in self._profiles:
            self._profiles[address] = WalletProfile(address=address)
        return self._profiles[address]

    def _record(
        self,
        sender: str,
        recipient: str,
        value: float,
        ts: datetime | None,
        token_addr: str,
        token_sym: str,
    ) -> None:
        # ── recipient (inflow) ────────────────────────────────────────────────
        rp = self._get_profile(recipient)
        rp.total_inflow += value
        rp.tx_count_in  += 1
        rp.tx_amounts.append(value)
        self._touch_ts(rp, ts)
        self._add_token(rp, token_addr, token_sym, inflow=value)
        self._add_bucket(rp, ts, value)
        self._check_risky(rp, sender)

        if recipient in self._last_inflow_ts and ts:
            delta = abs((ts - self._last_inflow_ts[recipient]).total_seconds())
            if delta < SHORT_CYCLE_SECS and rp.total_outflow > 0:
                rp.rapid_cycles += 1
        if ts:
            self._last_inflow_ts[recipient] = ts

        if value < MICRO_TX_ETH:
            rp.micro_tx_count += 1

        # ── sender (outflow) ─────────────────────────────────────────────────
        if sender:
            sp = self._get_profile(sender)
            sp.total_outflow += value
            sp.tx_count_out  += 1
            sp.tx_amounts.append(value)
            self._touch_ts(sp, ts)
            self._add_token(sp, token_addr, token_sym, outflow=value)
            self._add_bucket(sp, ts, value)
            self._check_risky(sp, recipient)

    @staticmethod
    def _parse_ts(raw) -> datetime | None:
        if raw is None:
            return None
        try:
            return datetime.fromtimestamp(int(raw), tz=timezone.utc)
        except (ValueError, TypeError):
            pass
        if isinstance(raw, str):
            try:
                return datetime.fromisoformat(raw.replace("Z", "+00:00"))
            except ValueError:
                pass
        return None

    @staticmethod
    def _touch_ts(p: WalletProfile, ts: datetime | None) -> None:
        if ts is None:
            return
        if p.first_seen is None or ts < p.first_seen:
            p.first_seen = ts
        if p.last_seen is None or ts > p.last_seen:
            p.last_seen = ts

    @staticmethod
    def _add_token(
        p: WalletProfile, addr: str, sym: str,
        inflow: float = 0.0, outflow: float = 0.0,
    ) -> None:
        if addr not in p.token_flows:
            p.token_flows[addr] = TokenFlow(addr, sym)
        tf = p.token_flows[addr]
        tf.inflow  += inflow
        tf.outflow += outflow
        tf.tx_count += 1

    @staticmethod
    def _add_bucket(p: WalletProfile, ts: datetime | None, value: float) -> None:
        if ts is None:
            return
        h_key = ts.strftime("%Y-%m-%dT%H:00Z")
        d_key = ts.strftime("%Y-%m-%d")
        for key, store in ((h_key, p.hourly_buckets), (d_key, p.daily_buckets)):
            if key not in store:
                store[key] = TimeBucket(timestamp=key)
            store[key].volume   += value
            store[key].tx_count += 1

    def _check_risky(self, p: WalletProfile, counterparty: str) -> None:
        if counterparty in self.risky_addresses:
            if counterparty not in p.risky_interactions:
                p.risky_interactions.append(counterparty)

    def _detect_spikes(self) -> None:
        for p in self._profiles.values():
            if len(p.hourly_buckets) < 2:
                continue
            vols = [b.volume for b in p.hourly_buckets.values()]
            avg  = statistics.mean(vols)
            if avg > 0 and max(vols) > SPIKE_MULTIPLIER * avg:
                p.spike_detected = True

    @staticmethod
    def _summary(top: list[WalletProfile]) -> dict[str, Any]:
        if not top:
            return {}
        return {
            "total_inflow_top_n": round(sum(p.total_inflow for p in top), 6),
            "classifications": {
                cls: sum(1 for p in top if p.classification == cls)
                for cls in {"accumulator", "distributor", "bot-like", "whale", "retail"}
            },
            "high_risk_wallets": [
                {"address": p.address, "risk_score": p.risk_score}
                for p in top if p.risk_score >= 40
            ],
        }


# ══════════════════════════════════════════════════════════════════════════════
#  LEGACY ANALYSIS (v1 – preserved verbatim for /trace backward compat)
# ══════════════════════════════════════════════════════════════════════════════

def analyze_wallet(wallet: str, transactions: list[dict]) -> dict | None:
    """Original single-top-recipient function — unchanged from v1."""
    wallet_lower = wallet.lower()
    recipient_txns: dict[str, list[dict]] = defaultdict(list)

    for tx in transactions:
        sender = tx.get("from", "").lower()
        to     = tx.get("to",   "").lower()
        if sender != wallet_lower or not to or tx.get("isError") == "1":
            continue
        recipient_txns[to].append(tx)

    if not recipient_txns:
        return None

    top_address = max(recipient_txns, key=lambda addr: len(recipient_txns[addr]))
    top_txns    = recipient_txns[top_address]

    total_eth = sum(int(tx.get("value", 0)) for tx in top_txns) / 1e18

    timestamps = [
        datetime.fromtimestamp(int(tx["timeStamp"]), tz=timezone.utc)
        for tx in top_txns if tx.get("timeStamp")
    ]
    first_ts = min(timestamps) if timestamps else None
    last_ts  = max(timestamps) if timestamps else None

    return {
        "address":        top_address,
        "tx_count":       len(top_txns),
        "total_eth":      total_eth,
        "first_ts":       first_ts,
        "last_ts":        last_ts,
        "total_out_txns": sum(len(v) for v in recipient_txns.values()),
    }


# ══════════════════════════════════════════════════════════════════════════════
#  SHARED HELPERS FOR BUILDING TOP-N ANALYZER
# ══════════════════════════════════════════════════════════════════════════════

def build_analyzer(
    wallet: str,
    transactions: list[dict],
    erc20_transfers: list[dict] | None = None,
    top_n: int = DEFAULT_TOP_N,
) -> WalletAnalyzer:
    """Run transactions + optional ERC-20 through the v2 engine."""
    az = WalletAnalyzer(top_n=top_n)
    for tx in transactions:
        az.ingest_eth_tx(tx, perspective_wallet=wallet)
    for tx in (erc20_transfers or []):
        az.ingest_erc20_tx(tx)
    return az


# ══════════════════════════════════════════════════════════════════════════════
#  FORMATTING LAYER  (v1 helpers preserved; v2 formatters added)
# ══════════════════════════════════════════════════════════════════════════════

def esc(s: str) -> str:
    specials = r"\_*[]()~`>#+-=|{}.!"
    return "".join(f"\\{c}" if c in specials else c for c in str(s))


def eth_line(label: str, eth_val: float | None, price: float, suffix: str = "") -> str:
    if eth_val is None:
        return f"{label} N/A"
    usd_val = eth_to_usd(eth_val, price)
    eth_str = esc(fmt_eth(eth_val))
    usd_str = esc(fmt_usd(usd_val)) if price else "USD N/A"
    return f"{label} {eth_str} ETH \\(≈ {usd_str}\\){esc(suffix)}"


def _fmt_month(dt: datetime | None) -> str:
    return dt.strftime("%b %Y") if dt else "N/A"


def _fmt_dt(dt: datetime | None) -> str:
    return dt.strftime("%d %b %Y %H:%M UTC") if dt else "N/A"


def _risk_emoji(score: int) -> str:
    if score >= 70: return "🔴"
    if score >= 40: return "🟠"
    if score >= 15: return "🟡"
    return "🟢"


def _class_emoji(cls: str) -> str:
    return {
        "accumulator": "🏦",
        "distributor": "📤",
        "bot-like":    "🤖",
        "whale":       "🐳",
        "retail":      "👤",
    }.get(cls, "❓")


# ── /trace result (v1 – unchanged) ───────────────────────────────────────────

def format_result(
    queried_wallet:  str,
    analysis:        dict | None,
    total_txns:      int,
    sender_stats:    dict | None = None,
    recipient_stats: dict | None = None,
    eth_price:       float = 0.0,
) -> tuple[str, InlineKeyboardMarkup | None]:
    if analysis is None:
        return (
            "🔍 *Wallet Analysis Complete*\n\n"
            f"Wallet `{queried_wallet}` has *no outgoing transactions* "
            f"in the latest {MAX_TRANSACTIONS} records\\.\n\n"
            "_This wallet may only receive funds or has no activity yet\\._",
            None,
        )

    addr      = analysis["address"]
    tx_count  = analysis["tx_count"]
    eth_val   = analysis["total_eth"]
    first_str = _fmt_month(analysis["first_ts"])
    last_str  = _fmt_month(analysis["last_ts"])
    total_out = analysis["total_out_txns"]
    period    = (
        first_str if first_str == last_str
        else f"{first_str} → {last_str}"
    ) if analysis["first_ts"] and analysis["last_ts"] else "Unknown"

    price_note = (
        f"_ETH price used: {esc(fmt_usd(eth_price))}_"
        if eth_price else "_\\(USD conversion unavailable\\)_"
    )

    lines = [
        "🔎 *Top Recipient Wallet*", "",
        price_note, "",
        f"📬 *Address:* `{addr}`",
        f"🔁 *Transactions to this address:* {esc(str(tx_count))}",
        eth_line("💸 *ETH sent to recipient:*", eth_val, eth_price),
        f"📅 *Activity Period:* {esc(period)}",
    ]

    if sender_stats:
        sample = sender_stats.get("sample_size", 0)
        suf    = f" (last {sample:,} txns)" if sample else ""
        lines += [
            "", "─────────────────────────",
            f"👤 *Sender Wallet Stats* \\(`{esc(queried_wallet[:8])}…`\\)",
            eth_line("🏦 *Current Balance:*", sender_stats.get("current_balance_eth"), eth_price),
            eth_line("📤 *Total Sent:*",      sender_stats.get("total_sent_eth"),      eth_price, suf),
            eth_line("📥 *Total Received:*",  sender_stats.get("total_received_eth"),  eth_price, suf),
        ]

    if recipient_stats:
        sample = recipient_stats.get("sample_size", 0)
        suf    = f" (last {sample:,} txns)" if sample else ""
        lines += [
            "", "─────────────────────────",
            f"🎯 *Recipient Wallet Stats* \\(`{esc(addr[:8])}…`\\)",
            eth_line("🏦 *Current Balance:*", recipient_stats.get("current_balance_eth"), eth_price),
            eth_line("📥 *Total Received:*",  recipient_stats.get("total_received_eth"),  eth_price, suf),
            eth_line("📤 *Total Sent:*",      recipient_stats.get("total_sent_eth"),      eth_price, suf),
        ]

    lines += [
        "", "─────────────────────────",
        f"📊 Scanned wallet sent *{esc(str(tx_count))}* txns to this address",
        f"   out of *{esc(str(total_out))}* total outgoing txns analysed\\.",
        "",
        f"🔗 [View on Etherscan](https://etherscan.io/address/{addr})",
    ]

    keyboard = InlineKeyboardMarkup([
        [InlineKeyboardButton("📋 Copy Address",      callback_data=f"copy:{addr}")],
        [InlineKeyboardButton("🔗 View on Etherscan", url=f"https://etherscan.io/address/{addr}")],
    ])
    return "\n".join(lines), keyboard


# ── /top result ───────────────────────────────────────────────────────────────

def format_top_n(
    queried_wallet: str,
    profiles: list[WalletProfile],
    eth_price: float,
    rank_by: str,
    total_txns: int,
) -> tuple[str, InlineKeyboardMarkup | None]:
    if not profiles:
        return (
            "🔍 *No outgoing transactions found* in the latest "
            f"{MAX_TRANSACTIONS} records for `{queried_wallet}`\\.",
            None,
        )

    price_note = f"_ETH price: {esc(fmt_usd(eth_price))}_" if eth_price else ""
    lines = [
        f"🏆 *Top {len(profiles)} Recipient Wallets*",
        f"_Ranked by: {'ETH received' if rank_by == 'inflow' else 'transaction count'}_",
        price_note, "",
    ]

    for i, p in enumerate(profiles, 1):
        usd = eth_to_usd(p.total_inflow, eth_price)
        cls_em = _class_emoji(p.classification)
        risk_em = _risk_emoji(p.risk_score)
        lines += [
            f"*{i}\\. `{esc(p.address[:10])}…`* {cls_em} {risk_em}",
            f"   💸 {esc(fmt_eth(p.total_inflow))} ETH \\(≈ {esc(fmt_usd(usd))}\\)"
                if eth_price else f"   💸 {esc(fmt_eth(p.total_inflow))} ETH",
            f"   🔁 {esc(str(p.tx_count_in))} txns in  ·  🏷 {esc(p.classification)}  ·  ⚠️ risk {esc(str(p.risk_score))}/100",
            f"   🔗 [Etherscan](https://etherscan.io/address/{p.address})",
            "",
        ]

    lines += [
        "─────────────────────────",
        f"📊 Analysed *{esc(str(total_txns))}* txns for `{esc(queried_wallet[:10])}…`",
    ]

    top_addr = profiles[0].address
    keyboard = InlineKeyboardMarkup([
        [InlineKeyboardButton("📋 Copy Top Address", callback_data=f"copy:{top_addr}")],
        [InlineKeyboardButton("🔗 Top Address on Etherscan",
                               url=f"https://etherscan.io/address/{top_addr}")],
    ])
    return "\n".join(lines), keyboard


# ── /profile result ───────────────────────────────────────────────────────────

def format_profile(
    queried_wallet: str,
    p: WalletProfile,
    eth_price: float,
) -> tuple[str, InlineKeyboardMarkup | None]:
    cls_em  = _class_emoji(p.classification)
    risk_em = _risk_emoji(p.risk_score)

    usd_in  = fmt_usd(eth_to_usd(p.total_inflow,  eth_price)) if eth_price else "N/A"
    usd_out = fmt_usd(eth_to_usd(p.total_outflow, eth_price)) if eth_price else "N/A"
    usd_net = fmt_usd(eth_to_usd(abs(p.net_balance_change), eth_price)) if eth_price else "N/A"
    net_sign = "+" if p.net_balance_change >= 0 else "-"

    top_tokens = []
    for addr in p.most_interacted_tokens[:3]:
        tf = p.token_flows[addr]
        top_tokens.append(f"{esc(tf.token_symbol)} \\({tf.tx_count} txns\\)")

    lines = [
        f"🔬 *Wallet Profile*",
        f"`{p.address}`",
        "",
        f"🏷 *Classification:* {cls_em} {esc(p.classification.title())}",
        f"⚠️ *Risk Score:* {risk_em} {esc(str(p.risk_score))}/100",
        "",
        "━━━━━━━━━━━━━━━━━━━━━━",
        "📊 *Flow Summary*",
        f"  📥 Inflow:  {esc(fmt_eth(p.total_inflow))} ETH  \\(≈ {esc(usd_in)}\\)",
        f"  📤 Outflow: {esc(fmt_eth(p.total_outflow))} ETH  \\(≈ {esc(usd_out)}\\)",
        f"  📈 Net:     {esc(net_sign)}{esc(fmt_eth(abs(p.net_balance_change)))} ETH  \\(≈ {esc(usd_net)}\\)",
        "",
        "━━━━━━━━━━━━━━━━━━━━━━",
        "⚡ *Activity*",
        f"  🔁 Total txns:    {esc(str(p.tx_count))}  \\(in: {esc(str(p.tx_count_in))} / out: {esc(str(p.tx_count_out))}\\)",
        f"  📏 Avg tx size:   {esc(fmt_eth(p.avg_tx_size))} ETH",
        f"  🕒 Freq:          {esc(f'{p.tx_per_hour:.2f}')} txns/hr",
        f"  🗓 First seen:    {esc(_fmt_dt(p.first_seen))}",
        f"  🗓 Last seen:     {esc(_fmt_dt(p.last_seen))}",
    ]

    if top_tokens:
        lines += [
            "",
            "━━━━━━━━━━━━━━━━━━━━━━",
            "🪙 *Top Tokens Interacted*",
            "  " + "  ·  ".join(top_tokens),
        ]

    if p.risk_flags:
        lines += [
            "",
            "━━━━━━━━━━━━━━━━━━━━━━",
            "🚨 *Risk Flags*",
        ]
        for flag in p.risk_flags:
            lines.append(f"  • {esc(flag)}")

    lines += [
        "",
        f"🔗 [View on Etherscan](https://etherscan.io/address/{p.address})",
    ]

    keyboard = InlineKeyboardMarkup([
        [InlineKeyboardButton("📋 Copy Address", callback_data=f"copy:{p.address}")],
        [InlineKeyboardButton("🔗 View on Etherscan",
                               url=f"https://etherscan.io/address/{p.address}")],
    ])
    return "\n".join(lines), keyboard


# ── /tokens result ────────────────────────────────────────────────────────────

def format_tokens(
    queried_wallet: str,
    p: WalletProfile,
) -> tuple[str, InlineKeyboardMarkup | None]:
    if not p.token_flows:
        return (
            f"🪙 *No token activity found* for `{esc(queried_wallet[:10])}…`\\.",
            None,
        )

    sorted_tokens = sorted(
        p.token_flows.values(),
        key=lambda tf: tf.tx_count,
        reverse=True,
    )[:15]

    lines = [
        f"🪙 *Token & Asset Breakdown*",
        f"`{queried_wallet}`",
        "",
    ]

    for tf in sorted_tokens:
        net_sign = "+" if tf.net >= 0 else ""
        lines += [
            f"*{esc(tf.token_symbol)}*",
            f"  📥 In: {esc(f'{tf.inflow:.4f}')}  📤 Out: {esc(f'{tf.outflow:.4f}')}  "
            f"📈 Net: {esc(net_sign + f'{tf.net:.4f}')}",
            f"  🔁 {esc(str(tf.tx_count))} txns",
            "",
        ]

    keyboard = InlineKeyboardMarkup([
        [InlineKeyboardButton("📋 Copy Address", callback_data=f"copy:{queried_wallet}")],
    ])
    return "\n".join(lines), keyboard


# ── /risk result ──────────────────────────────────────────────────────────────

def format_risk(
    queried_wallet: str,
    profiles: list[WalletProfile],
) -> tuple[str, InlineKeyboardMarkup | None]:
    if not profiles:
        return "No wallets to assess\\.", None

    lines = [
        "🚨 *Suspicious Activity Report*",
        f"`{queried_wallet}`", "",
    ]

    flagged = [p for p in profiles if p.risk_flags]

    if not flagged:
        lines.append("✅ No suspicious activity detected in top wallets\\.")
    else:
        for p in sorted(flagged, key=lambda x: x.risk_score, reverse=True):
            em = _risk_emoji(p.risk_score)
            lines += [
                f"{em} `{p.address[:12]}…` — Score: *{esc(str(p.risk_score))}/100*",
            ]
            for flag in p.risk_flags:
                lines.append(f"   • {esc(flag)}")
            lines.append("")

    return "\n".join(lines), None


# ── /time result ──────────────────────────────────────────────────────────────

def format_time(
    queried_wallet: str,
    p: WalletProfile,
    granularity: str = "d",
) -> tuple[str, InlineKeyboardMarkup | None]:
    buckets = p.daily_buckets if granularity == "d" else p.hourly_buckets
    label   = "Daily" if granularity == "d" else "Hourly"

    if not buckets:
        return f"📅 *No time data* available for `{esc(queried_wallet[:10])}…`\\.", None

    sorted_b = sorted(buckets.items())
    # Detect peaks
    vols = [b.volume for _, b in sorted_b]
    avg  = statistics.mean(vols) if vols else 0
    peak_key = max(buckets, key=lambda k: buckets[k].volume)

    lines = [
        f"📅 *{label} Volume Analysis*",
        f"`{queried_wallet}`", "",
        f"📊 Peak: *{esc(peak_key)}*  \\({esc(fmt_eth(buckets[peak_key].volume))} ETH\\)",
        f"📈 Avg/bucket: {esc(fmt_eth(avg))} ETH",
        f"{'⚡ Activity spike detected\\!' if p.spike_detected else ''}",
        "",
        f"*{label} breakdown \\(last {min(len(sorted_b), 14)} periods\\):*",
    ]

    for key, b in sorted_b[-14:]:
        bar_len = int((b.volume / (max(vols) or 1)) * 10)
        bar     = "█" * bar_len + "░" * (10 - bar_len)
        spike_m = " ⚡" if b.volume > SPIKE_MULTIPLIER * avg else ""
        lines.append(
            f"`{esc(key[-10:])}` {esc(bar)} "
            f"{esc(fmt_eth(b.volume))} ETH  {esc(str(b.tx_count))}tx{esc(spike_m)}"
        )

    return "\n".join(lines), None


# ══════════════════════════════════════════════════════════════════════════════
#  VALIDATION HELPERS  (v1 – unchanged)
# ══════════════════════════════════════════════════════════════════════════════

def is_valid_eth_address(addr: str) -> bool:
    return bool(ETH_ADDRESS_RE.match(addr))


# ══════════════════════════════════════════════════════════════════════════════
#  SHARED FETCH+ANALYSE PIPELINE  (used by all new commands)
# ══════════════════════════════════════════════════════════════════════════════

async def _fetch_and_analyse(
    wallet: str,
    scanning_msg,
    top_n: int = DEFAULT_TOP_N,
    include_erc20: bool = False,
) -> tuple[WalletAnalyzer, list[dict], float]:
    """
    Fetch data, run analyzer, return (analyzer, transactions, eth_price).
    Updates scanning_msg with progress.
    """
    await scanning_msg.edit_text(
        f"💱 *Fetching ETH price…*\n\n`{wallet}`",
        parse_mode=ParseMode.MARKDOWN_V2,
    )
    eth_price = fetch_eth_price_usd()

    await scanning_msg.edit_text(
        f"🔄 *Fetching transactions…*\n\n`{wallet}`",
        parse_mode=ParseMode.MARKDOWN_V2,
    )
    transactions = fetch_transactions(wallet)

    erc20_transfers: list[dict] = []
    if include_erc20 and transactions:
        await scanning_msg.edit_text(
            f"🪙 *Fetching ERC\\-20 transfers…*\n\n`{wallet}`",
            parse_mode=ParseMode.MARKDOWN_V2,
        )
        erc20_transfers = fetch_erc20_transfers(wallet)

    await scanning_msg.edit_text(
        f"⚙️ *Analysing {len(transactions):,} transactions…*\n\n`{wallet}`",
        parse_mode=ParseMode.MARKDOWN_V2,
    )
    az = build_analyzer(wallet, transactions, erc20_transfers, top_n=top_n)
    return az, transactions, eth_price


# ══════════════════════════════════════════════════════════════════════════════
#  TELEGRAM HANDLERS
# ══════════════════════════════════════════════════════════════════════════════

async def cmd_start(update: Update, _ctx: ContextTypes.DEFAULT_TYPE) -> None:
    text = (
        "👋 *Welcome to the Ethereum Wallet Analyzer Bot v2\\!*\n\n"
        "I trace Ethereum wallet activity with deep analytics, risk scoring, "
        "token breakdowns, and time\\-series analysis\\.\n\n"
        "━━━━━━━━━━━━━━━━━━━━━━\n"
        "🚀 *Commands*\n\n"
        "`/trace  <wallet>`  — Top recipient \\(original\\)\n"
        "`/top    <wallet> [N]` — Top\\-N recipients \\(default 10\\)\n"
        "`/profile <wallet>` — Deep behaviour profile \\+ risk\n"
        "`/tokens  <wallet>` — ERC\\-20 token breakdown\n"
        "`/time    <wallet> [h|d]` — Hourly/daily volume chart\n"
        "`/risk    <wallet>` — Suspicious activity report\n\n"
        "━━━━━━━━━━━━━━━━━━━━━━\n"
        "📊 *What you get \\(per wallet\\):*\n"
        "• Classification: accumulator / distributor / bot / whale / retail\n"
        "• Total inflow, outflow, net balance change \\+ USD\n"
        "• Avg tx size, tx frequency \\(tx/hr\\)\n"
        "• ERC\\-20 token breakdown\n"
        "• Risk score 0–100 with specific flags\n"
        "• Time\\-series volume with spike detection\n\n"
        "_Analysis covers up to the latest 1,000 transactions\\._"
    )
    await update.message.reply_text(text, parse_mode=ParseMode.MARKDOWN_V2)


# ── /trace  (v1 – logic unchanged) ───────────────────────────────────────────

async def cmd_trace(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not context.args:
        await update.message.reply_text(
            "⚠️ Usage: `/trace <wallet_address>`",
            parse_mode=ParseMode.MARKDOWN_V2,
        )
        return

    wallet = context.args[0].strip()
    if not is_valid_eth_address(wallet):
        await update.message.reply_text(
            "❌ *Invalid Ethereum address\\.*\n\n"
            "Must start with `0x` followed by 40 hex characters\\.",
            parse_mode=ParseMode.MARKDOWN_V2,
        )
        return

    scanning_msg = await update.message.reply_text(
        f"🔄 *Scanning wallet…*\n\n`{wallet}`\n\n"
        f"_Fetching up to {MAX_TRANSACTIONS:,} transactions\\.\\.\\._",
        parse_mode=ParseMode.MARKDOWN_V2,
    )

    try:
        await scanning_msg.edit_text(
            f"💱 *Fetching ETH price…*\n\n`{wallet}`",
            parse_mode=ParseMode.MARKDOWN_V2,
        )
        eth_price = fetch_eth_price_usd()

        await scanning_msg.edit_text(
            f"🔄 *Scanning wallet…*\n\n`{wallet}`\n\n"
            f"_Fetching up to {MAX_TRANSACTIONS:,} transactions\\.\\.\\._",
            parse_mode=ParseMode.MARKDOWN_V2,
        )
        transactions = fetch_transactions(wallet)

        await scanning_msg.edit_text(
            f"⚙️ *Analysing {len(transactions):,} transactions…*\n\n`{wallet}`",
            parse_mode=ParseMode.MARKDOWN_V2,
        )
        analysis = analyze_wallet(wallet, transactions)

        await scanning_msg.edit_text(
            f"📊 *Fetching sender wallet stats…*\n\n`{wallet}`",
            parse_mode=ParseMode.MARKDOWN_V2,
        )
        sender_stats = fetch_wallet_stats(wallet)

        recipient_stats: dict | None = None
        if analysis:
            await scanning_msg.edit_text(
                f"📥 *Fetching recipient wallet stats…*\n\n`{wallet}`",
                parse_mode=ParseMode.MARKDOWN_V2,
            )
            recipient_stats = fetch_wallet_stats(analysis["address"])

        result, keyboard = format_result(
            queried_wallet=wallet,
            analysis=analysis,
            total_txns=len(transactions),
            sender_stats=sender_stats,
            recipient_stats=recipient_stats,
            eth_price=eth_price,
        )
        await scanning_msg.delete()
        await update.message.reply_text(
            result,
            parse_mode=ParseMode.MARKDOWN_V2,
            reply_markup=keyboard,
        )

    except RuntimeError as exc:
        logger.warning("RuntimeError /trace %s: %s", wallet, exc)
        await scanning_msg.edit_text(
            f"❌ *Error during analysis*\n\n{esc(str(exc))}",
            parse_mode=ParseMode.MARKDOWN_V2,
        )
    except Exception:
        logger.exception("Unexpected error /trace %s", wallet)
        await scanning_msg.edit_text(
            "❌ *An unexpected error occurred\\.*\n_Please try again\\._",
            parse_mode=ParseMode.MARKDOWN_V2,
        )


# ── /top ──────────────────────────────────────────────────────────────────────

async def cmd_top(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    args = context.args or []
    if not args:
        await update.message.reply_text(
            "⚠️ Usage: `/top <wallet_address> [N]`\n_N defaults to 10_",
            parse_mode=ParseMode.MARKDOWN_V2,
        )
        return

    wallet = args[0].strip()
    if not is_valid_eth_address(wallet):
        await update.message.reply_text(
            "❌ *Invalid Ethereum address\\.*", parse_mode=ParseMode.MARKDOWN_V2
        )
        return

    top_n = DEFAULT_TOP_N
    if len(args) >= 2:
        try:
            top_n = max(1, min(int(args[1]), 50))
        except ValueError:
            pass

    scanning_msg = await update.message.reply_text(
        f"🔍 *Fetching top\\-{top_n} recipients…*\n\n`{wallet}`",
        parse_mode=ParseMode.MARKDOWN_V2,
    )
    try:
        az, transactions, eth_price = await _fetch_and_analyse(
            wallet, scanning_msg, top_n=top_n
        )
        profiles = [p for p in az.results() if p.address != wallet.lower()][:top_n]

        result, keyboard = format_top_n(
            queried_wallet=wallet,
            profiles=profiles,
            eth_price=eth_price,
            rank_by="inflow",
            total_txns=len(transactions),
        )
        await scanning_msg.delete()
        await update.message.reply_text(
            result, parse_mode=ParseMode.MARKDOWN_V2, reply_markup=keyboard
        )
    except RuntimeError as exc:
        await scanning_msg.edit_text(
            f"❌ *Error:* {esc(str(exc))}", parse_mode=ParseMode.MARKDOWN_V2
        )
    except Exception:
        logger.exception("Unexpected error /top %s", wallet)
        await scanning_msg.edit_text(
            "❌ *An unexpected error occurred\\.*", parse_mode=ParseMode.MARKDOWN_V2
        )


# ── /profile ──────────────────────────────────────────────────────────────────

async def cmd_profile(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    args = context.args or []
    if not args:
        await update.message.reply_text(
            "⚠️ Usage: `/profile <wallet_address>`",
            parse_mode=ParseMode.MARKDOWN_V2,
        )
        return

    wallet = args[0].strip()
    if not is_valid_eth_address(wallet):
        await update.message.reply_text(
            "❌ *Invalid Ethereum address\\.*", parse_mode=ParseMode.MARKDOWN_V2
        )
        return

    scanning_msg = await update.message.reply_text(
        f"🔬 *Building wallet profile…*\n\n`{wallet}`",
        parse_mode=ParseMode.MARKDOWN_V2,
    )
    try:
        az, _, eth_price = await _fetch_and_analyse(
            wallet, scanning_msg, include_erc20=True
        )
        profile = az._get_profile(wallet.lower())
        az._detect_spikes()

        result, keyboard = format_profile(wallet, profile, eth_price)
        await scanning_msg.delete()
        await update.message.reply_text(
            result, parse_mode=ParseMode.MARKDOWN_V2, reply_markup=keyboard
        )
    except RuntimeError as exc:
        await scanning_msg.edit_text(
            f"❌ *Error:* {esc(str(exc))}", parse_mode=ParseMode.MARKDOWN_V2
        )
    except Exception:
        logger.exception("Unexpected error /profile %s", wallet)
        await scanning_msg.edit_text(
            "❌ *An unexpected error occurred\\.*", parse_mode=ParseMode.MARKDOWN_V2
        )


# ── /tokens ───────────────────────────────────────────────────────────────────

async def cmd_tokens(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    args = context.args or []
    if not args:
        await update.message.reply_text(
            "⚠️ Usage: `/tokens <wallet_address>`",
            parse_mode=ParseMode.MARKDOWN_V2,
        )
        return

    wallet = args[0].strip()
    if not is_valid_eth_address(wallet):
        await update.message.reply_text(
            "❌ *Invalid Ethereum address\\.*", parse_mode=ParseMode.MARKDOWN_V2
        )
        return

    scanning_msg = await update.message.reply_text(
        f"🪙 *Fetching token transfers…*\n\n`{wallet}`",
        parse_mode=ParseMode.MARKDOWN_V2,
    )
    try:
        az, _, _ = await _fetch_and_analyse(
            wallet, scanning_msg, include_erc20=True
        )
        profile = az._get_profile(wallet.lower())

        result, keyboard = format_tokens(wallet, profile)
        await scanning_msg.delete()
        await update.message.reply_text(
            result, parse_mode=ParseMode.MARKDOWN_V2, reply_markup=keyboard
        )
    except RuntimeError as exc:
        await scanning_msg.edit_text(
            f"❌ *Error:* {esc(str(exc))}", parse_mode=ParseMode.MARKDOWN_V2
        )
    except Exception:
        logger.exception("Unexpected error /tokens %s", wallet)
        await scanning_msg.edit_text(
            "❌ *An unexpected error occurred\\.*", parse_mode=ParseMode.MARKDOWN_V2
        )


# ── /time ─────────────────────────────────────────────────────────────────────

async def cmd_time(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    args = context.args or []
    if not args:
        await update.message.reply_text(
            "⚠️ Usage: `/time <wallet_address> [h|d]`\n"
            "_h = hourly buckets, d = daily buckets \\(default\\)_",
            parse_mode=ParseMode.MARKDOWN_V2,
        )
        return

    wallet = args[0].strip()
    if not is_valid_eth_address(wallet):
        await update.message.reply_text(
            "❌ *Invalid Ethereum address\\.*", parse_mode=ParseMode.MARKDOWN_V2
        )
        return

    granularity = "d"
    if len(args) >= 2 and args[1].lower() in ("h", "hour", "hourly"):
        granularity = "h"

    scanning_msg = await update.message.reply_text(
        f"📅 *Building time\\-series analysis…*\n\n`{wallet}`",
        parse_mode=ParseMode.MARKDOWN_V2,
    )
    try:
        az, _, _ = await _fetch_and_analyse(wallet, scanning_msg)
        profile  = az._get_profile(wallet.lower())
        az._detect_spikes()

        result, keyboard = format_time(wallet, profile, granularity)
        await scanning_msg.delete()
        await update.message.reply_text(
            result, parse_mode=ParseMode.MARKDOWN_V2, reply_markup=keyboard
        )
    except RuntimeError as exc:
        await scanning_msg.edit_text(
            f"❌ *Error:* {esc(str(exc))}", parse_mode=ParseMode.MARKDOWN_V2
        )
    except Exception:
        logger.exception("Unexpected error /time %s", wallet)
        await scanning_msg.edit_text(
            "❌ *An unexpected error occurred\\.*", parse_mode=ParseMode.MARKDOWN_V2
        )


# ── /risk ─────────────────────────────────────────────────────────────────────

async def cmd_risk(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    args = context.args or []
    if not args:
        await update.message.reply_text(
            "⚠️ Usage: `/risk <wallet_address>`",
            parse_mode=ParseMode.MARKDOWN_V2,
        )
        return

    wallet = args[0].strip()
    if not is_valid_eth_address(wallet):
        await update.message.reply_text(
            "❌ *Invalid Ethereum address\\.*", parse_mode=ParseMode.MARKDOWN_V2
        )
        return

    scanning_msg = await update.message.reply_text(
        f"🚨 *Running risk analysis…*\n\n`{wallet}`",
        parse_mode=ParseMode.MARKDOWN_V2,
    )
    try:
        az, _, _ = await _fetch_and_analyse(wallet, scanning_msg)
        profiles = az.results()

        result, keyboard = format_risk(wallet, profiles)
        await scanning_msg.delete()
        await update.message.reply_text(
            result, parse_mode=ParseMode.MARKDOWN_V2, reply_markup=keyboard
        )
    except RuntimeError as exc:
        await scanning_msg.edit_text(
            f"❌ *Error:* {esc(str(exc))}", parse_mode=ParseMode.MARKDOWN_V2
        )
    except Exception:
        logger.exception("Unexpected error /risk %s", wallet)
        await scanning_msg.edit_text(
            "❌ *An unexpected error occurred\\.*", parse_mode=ParseMode.MARKDOWN_V2
        )


# ── callback: copy address (v1 – unchanged) ───────────────────────────────────

async def handle_copy_callback(update: Update, _ctx: ContextTypes.DEFAULT_TYPE) -> None:
    query = update.callback_query
    await query.answer()
    if query.data and query.data.startswith("copy:"):
        address = query.data.split("copy:", 1)[1]
        await query.message.reply_text(
            f"📋 *Tap and hold to copy:*\n\n`{address}`",
            parse_mode=ParseMode.MARKDOWN_V2,
        )


# ══════════════════════════════════════════════════════════════════════════════
#  ENTRY POINT
# ══════════════════════════════════════════════════════════════════════════════

def main() -> None:
    logger.info("Starting Ethereum Wallet Analyzer Bot v2…")

    app = Application.builder().token(TELEGRAM_BOT_TOKEN).build()

    # ── command handlers ──────────────────────────────────────────────────────
    app.add_handler(CommandHandler("start",   cmd_start))
    app.add_handler(CommandHandler("trace",   cmd_trace))    # v1 – unchanged
    app.add_handler(CommandHandler("top",     cmd_top))      # NEW
    app.add_handler(CommandHandler("profile", cmd_profile))  # NEW
    app.add_handler(CommandHandler("tokens",  cmd_tokens))   # NEW
    app.add_handler(CommandHandler("time",    cmd_time))     # NEW
    app.add_handler(CommandHandler("risk",    cmd_risk))     # NEW

    # ── callback handlers ─────────────────────────────────────────────────────
    app.add_handler(CallbackQueryHandler(handle_copy_callback, pattern=r"^copy:"))

    logger.info("Bot is running. Press Ctrl+C to stop.")
    app.run_polling(allowed_updates=["message", "callback_query"])


if __name__ == "__main__":
    main()
