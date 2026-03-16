"""
Ethereum Wallet Analyzer Telegram Bot
Traces wallet activity and identifies top transaction recipients via Etherscan API.
Now shows USD values and current balance (ETH + USD) for both wallets.
"""

import os
import re
import time
import logging
from collections import defaultdict
from datetime import datetime, timezone

import requests
from dotenv import load_dotenv
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import (
    Application,
    CommandHandler,
    CallbackQueryHandler,
    ContextTypes,
)
from telegram.constants import ParseMode

# ── Environment ──────────────────────────────────────────────────────────────
load_dotenv()

TELEGRAM_BOT_TOKEN: str = os.environ.get("TELEGRAM_BOT_TOKEN", "")
ETHERSCAN_API_KEY:  str = os.environ.get("ETHERSCAN_API_KEY",  "")

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

ETHERSCAN_BASE_URL  = "https://api.etherscan.io/v2/api"
COINGECKO_PRICE_URL = "https://api.coingecko.com/api/v3/simple/price"
MAX_TRANSACTIONS    = 1000
ETH_ADDRESS_RE      = re.compile(r"^0x[0-9a-fA-F]{40}$")

# ── Logging ───────────────────────────────────────────────────────────────────
logging.basicConfig(
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
    level=logging.INFO,
)
logger = logging.getLogger(__name__)


# ══════════════════════════════════════════════════════════════════════════════
#  ETH PRICE (cached 60 s to avoid CoinGecko rate limits)
# ══════════════════════════════════════════════════════════════════════════════

_price_cache: dict = {"price": 0.0, "updated": 0.0}
_PRICE_TTL = 60  # seconds


def fetch_eth_price_usd() -> float:
    """Return live ETH/USD price, cached for 60 s."""
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
        _price_cache["price"]   = price
        _price_cache["updated"] = now
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


# ══════════════════════════════════════════════════════════════════════════════
#  DATA LAYER
# ══════════════════════════════════════════════════════════════════════════════

def fetch_transactions(wallet: str) -> list[dict]:
    params = {
        "chainid":    "1",
        "module":     "account",
        "action":     "txlist",
        "address":    wallet,
        "startblock": "0",
        "endblock":   "99999999",
        "page":       "1",
        "offset":     str(MAX_TRANSACTIONS),
        "sort":       "desc",
        "apikey":     ETHERSCAN_API_KEY,
    }

    logger.info("Fetching transactions for %s", wallet)
    try:
        response = requests.get(ETHERSCAN_BASE_URL, params=params, timeout=20)
        response.raise_for_status()
    except requests.exceptions.Timeout:
        raise RuntimeError("Etherscan API timed out. Please try again shortly.")
    except requests.exceptions.RequestException as exc:
        raise RuntimeError(f"Network error contacting Etherscan: {exc}")

    payload = response.json()

    if payload.get("status") == "0":
        msg    = payload.get("message", "")
        result = payload.get("result",  "")
        if "No transactions found" in (msg + str(result)):
            return []
        raise RuntimeError(f"Etherscan error: {payload.get('result', msg)}")

    if payload.get("status") != "1":
        raise RuntimeError(f"Unexpected Etherscan response: {payload}")

    return payload["result"]


def fetch_wallet_stats(address: str) -> dict:
    """
    Fetch for a wallet:
      - current_balance_eth  : latest on-chain balance
      - total_received_eth   : sum of incoming ETH (last MAX_TRANSACTIONS txns)
      - total_sent_eth       : sum of outgoing ETH (last MAX_TRANSACTIONS txns)
      - sample_size          : number of txns examined
    """
    # ── Current balance ───────────────────────────────────────────────────────
    balance_params = {
        "chainid": "1",
        "module":  "account",
        "action":  "balance",
        "address": address,
        "tag":     "latest",
        "apikey":  ETHERSCAN_API_KEY,
    }
    try:
        resp = requests.get(ETHERSCAN_BASE_URL, params=balance_params, timeout=20)
        resp.raise_for_status()
        bal_payload = resp.json()
        current_balance_eth = (
            int(bal_payload["result"]) / 1e18
            if bal_payload.get("status") == "1"
            else None
        )
    except Exception:
        current_balance_eth = None

    # ── Transaction list ──────────────────────────────────────────────────────
    txlist_params = {
        "chainid":    "1",
        "module":     "account",
        "action":     "txlist",
        "address":    address,
        "startblock": "0",
        "endblock":   "99999999",
        "page":       "1",
        "offset":     str(MAX_TRANSACTIONS),
        "sort":       "desc",
        "apikey":     ETHERSCAN_API_KEY,
    }
    try:
        resp = requests.get(ETHERSCAN_BASE_URL, params=txlist_params, timeout=20)
        resp.raise_for_status()
        tx_payload = resp.json()
        txns = tx_payload["result"] if tx_payload.get("status") == "1" else []
    except Exception:
        txns = []

    addr_lower = address.lower()

    total_received_wei = sum(
        int(tx.get("value", 0))
        for tx in txns
        if tx.get("to",   "").lower() == addr_lower
        and tx.get("isError") != "1"
    )
    total_sent_wei = sum(
        int(tx.get("value", 0))
        for tx in txns
        if tx.get("from", "").lower() == addr_lower
        and tx.get("isError") != "1"
    )

    return {
        "current_balance_eth": current_balance_eth,
        "total_received_eth":  total_received_wei / 1e18 if txns else None,
        "total_sent_eth":      total_sent_wei      / 1e18 if txns else None,
        "sample_size":         len(txns),
    }


# ══════════════════════════════════════════════════════════════════════════════
#  ANALYSIS LAYER
# ══════════════════════════════════════════════════════════════════════════════

def analyze_wallet(wallet: str, transactions: list[dict]) -> dict | None:
    wallet_lower = wallet.lower()
    recipient_txns: dict[str, list[dict]] = defaultdict(list)

    for tx in transactions:
        sender = tx.get("from", "").lower()
        to     = tx.get("to",   "").lower()

        if sender != wallet_lower:
            continue
        if not to:
            continue
        if tx.get("isError") == "1":
            continue

        recipient_txns[to].append(tx)

    if not recipient_txns:
        return None

    top_address = max(recipient_txns, key=lambda addr: len(recipient_txns[addr]))
    top_txns    = recipient_txns[top_address]

    total_wei = sum(int(tx.get("value", 0)) for tx in top_txns)
    total_eth = total_wei / 1e18

    timestamps = [
        datetime.fromtimestamp(int(tx["timeStamp"]), tz=timezone.utc)
        for tx in top_txns
        if tx.get("timeStamp")
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
#  FORMATTING LAYER
# ══════════════════════════════════════════════════════════════════════════════

def _fmt_month(dt: datetime | None) -> str:
    return dt.strftime("%b %Y") if dt else "N/A"


def esc(s: str) -> str:
    specials = r"\_*[]()~`>#+-=|{}.!"
    return "".join(f"\\{c}" if c in specials else c for c in str(s))


def fmt_eth(val: float) -> str:
    return f"{val:.4f}".rstrip("0").rstrip(".")


def eth_line(label: str, eth_val: float | None, price: float, suffix: str = "") -> str:
    """Return a formatted line showing both ETH and USD values."""
    if eth_val is None:
        return f"{label} N/A"
    usd_val  = eth_to_usd(eth_val, price)
    eth_str  = esc(fmt_eth(eth_val))
    usd_str  = esc(fmt_usd(usd_val)) if price else "USD N/A"
    suf      = esc(suffix)
    return f"{label} {eth_str} ETH \\(≈ {usd_str}\\){suf}"


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

    period = (
        first_str if first_str == last_str
        else f"{first_str} → {last_str}"
    ) if analysis["first_ts"] and analysis["last_ts"] else "Unknown"

    price_note = (
        f"_ETH price used: {esc(fmt_usd(eth_price))}_"
        if eth_price else "_\\(USD conversion unavailable\\)_"
    )

    lines = [
        "🔎 *Top Recipient Wallet*",
        "",
        price_note,
        "",
        f"📬 *Address:* `{addr}`",
        f"🔁 *Transactions to this address:* {esc(str(tx_count))}",
        eth_line("💸 *ETH sent to recipient:*", eth_val, eth_price),
        f"📅 *Activity Period:* {esc(period)}",
    ]

    # ── Sender wallet stats ───────────────────────────────────────────────────
    if sender_stats:
        sample = sender_stats.get("sample_size", 0)
        suf    = f" (last {sample:,} txns)" if sample else ""
        lines += [
            "",
            "─────────────────────────",
            f"👤 *Sender Wallet Stats* \\(`{esc(queried_wallet[:8])}…`\\)",
            eth_line("🏦 *Current Balance:*",  sender_stats.get("current_balance_eth"),  eth_price),
            eth_line("📤 *Total Sent:*",        sender_stats.get("total_sent_eth"),       eth_price, suf),
            eth_line("📥 *Total Received:*",    sender_stats.get("total_received_eth"),   eth_price, suf),
        ]

    # ── Recipient wallet stats ────────────────────────────────────────────────
    if recipient_stats:
        sample = recipient_stats.get("sample_size", 0)
        suf    = f" (last {sample:,} txns)" if sample else ""
        lines += [
            "",
            "─────────────────────────",
            f"🎯 *Recipient Wallet Stats* \\(`{esc(addr[:8])}…`\\)",
            eth_line("🏦 *Current Balance:*",  recipient_stats.get("current_balance_eth"),  eth_price),
            eth_line("📥 *Total Received:*",   recipient_stats.get("total_received_eth"),   eth_price, suf),
            eth_line("📤 *Total Sent:*",       recipient_stats.get("total_sent_eth"),       eth_price, suf),
        ]

    lines += [
        "",
        "─────────────────────────",
        f"📊 Scanned wallet sent *{esc(str(tx_count))}* txns to this address",
        f"   out of *{esc(str(total_out))}* total outgoing txns analysed\\.",
        "",
        f"🔗 [View on Etherscan](https://etherscan.io/address/{addr})",
    ]

    keyboard = InlineKeyboardMarkup([
        [InlineKeyboardButton("📋 Copy Address", callback_data=f"copy:{addr}")],
        [InlineKeyboardButton("🔗 View on Etherscan", url=f"https://etherscan.io/address/{addr}")],
    ])

    return "\n".join(lines), keyboard


# ══════════════════════════════════════════════════════════════════════════════
#  VALIDATION HELPERS
# ══════════════════════════════════════════════════════════════════════════════

def is_valid_eth_address(addr: str) -> bool:
    return bool(ETH_ADDRESS_RE.match(addr))


def _esc_v2(text: str) -> str:
    specials = r"\_*[]()~`>#+-=|{}.!"
    return "".join(f"\\{c}" if c in specials else c for c in text)


# ══════════════════════════════════════════════════════════════════════════════
#  TELEGRAM HANDLERS
# ══════════════════════════════════════════════════════════════════════════════

async def cmd_start(update: Update, _context: ContextTypes.DEFAULT_TYPE) -> None:
    text = (
        "👋 *Welcome to the Ethereum Wallet Analyzer Bot\\!*\n\n"
        "I trace Ethereum wallet activity and identify the address that "
        "receives the *most transactions* from a given wallet\\.\n\n"
        "━━━━━━━━━━━━━━━━━━━━━━\n"
        "🚀 *Usage*\n\n"
        "```\n/trace <wallet_address>\n```\n\n"
        "*Example:*\n"
        "```\n/trace 0xde0B295669a9FD93d5F28D9Ec85E40f4cb697BAe\n```\n\n"
        "━━━━━━━━━━━━━━━━━━━━━━\n"
        "📊 *What you'll get:*\n"
        "• Top recipient wallet address\n"
        "• Number of transactions sent\n"
        "• Total ETH transferred \\+ USD value\n"
        "• Current ETH balance \\+ USD value for both wallets\n"
        "• Total ETH sent/received \\+ USD for both wallets\n"
        "• Activity time period\n\n"
        "_Analysis covers up to the latest 1,000 transactions\\._"
    )
    await update.message.reply_text(text, parse_mode=ParseMode.MARKDOWN_V2)


async def cmd_trace(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not context.args:
        await update.message.reply_text(
            "⚠️ Please provide a wallet address\\.\n\n"
            "*Usage:* `/trace <wallet_address>`",
            parse_mode=ParseMode.MARKDOWN_V2,
        )
        return

    wallet = context.args[0].strip()

    if not is_valid_eth_address(wallet):
        await update.message.reply_text(
            "❌ *Invalid Ethereum address\\.*\n\n"
            "An Ethereum address must start with `0x` followed by 40 hex characters\\.\n\n"
            "*Example:* `0xde0B295669a9FD93d5F28D9Ec85E40f4cb697BAe`",
            parse_mode=ParseMode.MARKDOWN_V2,
        )
        return

    scanning_msg = await update.message.reply_text(
        f"🔄 *Scanning wallet…*\n\n`{wallet}`\n\n_Fetching up to {MAX_TRANSACTIONS:,} transactions\\.\\.\\._",
        parse_mode=ParseMode.MARKDOWN_V2,
    )

    try:
        # 1. Fetch ETH price first (used for all USD conversions)
        await scanning_msg.edit_text(
            f"💱 *Fetching ETH price…*\n\n`{wallet}`",
            parse_mode=ParseMode.MARKDOWN_V2,
        )
        eth_price = fetch_eth_price_usd()

        # 2. Fetch transactions for the queried wallet
        await scanning_msg.edit_text(
            f"🔄 *Scanning wallet…*\n\n`{wallet}`\n\n_Fetching up to {MAX_TRANSACTIONS:,} transactions\\.\\.\\._",
            parse_mode=ParseMode.MARKDOWN_V2,
        )
        transactions = fetch_transactions(wallet)

        # 3. Analyse
        await scanning_msg.edit_text(
            f"⚙️ *Analysing transactions…*\n\n`{wallet}`\n\n"
            f"_Found {len(transactions):,} transactions\\. Crunching numbers\\.\\.\\._",
            parse_mode=ParseMode.MARKDOWN_V2,
        )
        analysis = analyze_wallet(wallet, transactions)

        # 4. Fetch sender wallet stats (balance + sent/received)
        await scanning_msg.edit_text(
            f"📊 *Fetching sender wallet stats…*\n\n`{wallet}`",
            parse_mode=ParseMode.MARKDOWN_V2,
        )
        sender_stats = fetch_wallet_stats(wallet)

        # 5. Fetch recipient wallet stats
        recipient_stats: dict | None = None
        if analysis:
            await scanning_msg.edit_text(
                f"📥 *Fetching recipient wallet stats…*\n\n`{wallet}`\n\n"
                f"_Analysing top recipient address\\.\\.\\._",
                parse_mode=ParseMode.MARKDOWN_V2,
            )
            recipient_stats = fetch_wallet_stats(analysis["address"])

        # 6. Format and send
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
        logger.warning("RuntimeError for wallet %s: %s", wallet, exc)
        await scanning_msg.edit_text(
            f"❌ *Error during analysis*\n\n{_esc_v2(str(exc))}",
            parse_mode=ParseMode.MARKDOWN_V2,
        )
    except Exception:
        logger.exception("Unexpected error for wallet %s", wallet)
        await scanning_msg.edit_text(
            "❌ *An unexpected error occurred\\.*\n\n"
            "_Please try again in a few moments\\._",
            parse_mode=ParseMode.MARKDOWN_V2,
        )


async def handle_copy_callback(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
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
    logger.info("Starting Ethereum Wallet Analyzer Bot…")

    app = Application.builder().token(TELEGRAM_BOT_TOKEN).build()
    app.add_handler(CommandHandler("start", cmd_start))
    app.add_handler(CommandHandler("trace", cmd_trace))
    app.add_handler(CallbackQueryHandler(handle_copy_callback, pattern=r"^copy:"))

    logger.info("Bot is running. Press Ctrl+C to stop.")
    app.run_polling(allowed_updates=["message", "callback_query"])


if __name__ == "__main__":
    main()
