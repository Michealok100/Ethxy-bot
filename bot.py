"""
Ethereum Wallet Analyzer Telegram Bot
Traces wallet activity and identifies top transaction recipients via Etherscan API.
"""

import os
import re
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
ETHERSCAN_API_KEY: str = os.environ.get("ETHERSCAN_API_KEY", "")

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

ETHERSCAN_BASE_URL = "https://api.etherscan.io/v2/api"
MAX_TRANSACTIONS = 1000
ETH_ADDRESS_RE = re.compile(r"^0x[0-9a-fA-F]{40}$")

# ── Logging ───────────────────────────────────────────────────────────────────
logging.basicConfig(
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
    level=logging.INFO,
)
logger = logging.getLogger(__name__)


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
    logger.info("Etherscan raw response: %s", payload)

    if payload.get("status") == "0":
        msg = payload.get("message", "")
        result = payload.get("result", "")
        if "No transactions found" in (msg + str(result)):
            return []
        raise RuntimeError(f"Etherscan error: {payload.get('result', msg)}")

    if payload.get("status") != "1":
        raise RuntimeError(f"Unexpected Etherscan response: {payload}")

    return payload["result"]


def fetch_recipient_stats(address: str) -> dict:
    """Fetch total ETH received and current balance for a given address."""

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

    # ── Incoming transactions (up to 1 000) to sum received ETH ───────────────
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
        if tx.get("to", "").lower() == addr_lower
        and tx.get("isError") != "1"
    )
    total_received_eth = total_received_wei / 1e18 if txns else None

    return {
        "current_balance_eth": current_balance_eth,
        "total_received_eth":  total_received_eth,
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
    if dt is None:
        return "N/A"
    return dt.strftime("%b %Y")


def format_result(
    queried_wallet: str,
    analysis: dict | None,
    total_txns: int,
    recipient_stats: dict | None = None,
) -> tuple[str, InlineKeyboardMarkup | None]:
    if analysis is None:
        return (
            "🔍 *Wallet Analysis Complete*\n\n"
            f"Wallet `{queried_wallet}` has *no outgoing transactions* "
            f"in the latest {MAX_TRANSACTIONS} records\\.\n\n"
            "_This wallet may only receive funds or has no activity yet\\._",
            None
        )

    addr      = analysis["address"]
    tx_count  = analysis["tx_count"]
    eth_val   = analysis["total_eth"]
    first_str = _fmt_month(analysis["first_ts"])
    last_str  = _fmt_month(analysis["last_ts"])
    total_out = analysis["total_out_txns"]

    if analysis["first_ts"] and analysis["last_ts"]:
        if first_str == last_str:
            period = first_str
        else:
            period = f"{first_str} → {last_str}"
    else:
        period = "Unknown"

    def esc(s: str) -> str:
        specials = r"\_*[]()~`>#+-=|{}.!"
        return "".join(f"\\{c}" if c in specials else c for s in [s] for c in s)

    def fmt_eth(val: float) -> str:
        return f"{val:.4f}".rstrip("0").rstrip(".")

    eth_display = fmt_eth(eth_val)

    lines = [
        "🔎 *Top Recipient Wallet*",
        "",
        f"📬 *Address:* `{addr}`",
        f"🔁 *Transactions Received:* {esc(str(tx_count))}",
        f"💰 *ETH Sent by Traced Wallet:* {esc(eth_display)} ETH",
        f"📅 *Activity Period:* {esc(period)}",
    ]

    # ── Recipient address stats ───────────────────────────────────────────────
    if recipient_stats:
        lines.append("")
        lines.append("─────────────────────────")
        lines.append("📥 *Recipient Address Stats*")

        rcv = recipient_stats.get("total_received_eth")
        bal = recipient_stats.get("current_balance_eth")
        sample = recipient_stats.get("sample_size", 0)

        if rcv is not None:
            suffix = f" \\(from latest {esc(str(sample))} txns\\)" if sample else ""
            lines.append(f"⬇️ *Total ETH Received:* {esc(fmt_eth(rcv))} ETH{suffix}")
        if bal is not None:
            lines.append(f"🏦 *Current Balance:* {esc(fmt_eth(bal))} ETH")

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
        "• Total ETH transferred\n"
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
        transactions = fetch_transactions(wallet)

        await scanning_msg.edit_text(
            f"⚙️ *Analysing transactions…*\n\n`{wallet}`\n\n"
            f"_Found {len(transactions):,} transactions\\. Crunching numbers\\.\\.\\._",
            parse_mode=ParseMode.MARKDOWN_V2,
        )

        analysis = analyze_wallet(wallet, transactions)

        recipient_stats: dict | None = None
        if analysis:
            await scanning_msg.edit_text(
                f"📥 *Fetching recipient stats…*\n\n`{wallet}`\n\n"
                f"_Analysing top recipient address\\.\\.\\._",
                parse_mode=ParseMode.MARKDOWN_V2,
            )
            recipient_stats = fetch_recipient_stats(analysis["address"])

        result, keyboard = format_result(wallet, analysis, len(transactions), recipient_stats)

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
    except Exception as exc:
        logger.exception("Unexpected error for wallet %s", wallet)
        await scanning_msg.edit_text(
            "❌ *An unexpected error occurred\\.*\n\n"
            "_Please try again in a few moments\\._",
            parse_mode=ParseMode.MARKDOWN_V2,
        )


async def handle_copy_callback(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Handle the 📋 Copy Address button tap."""
    query = update.callback_query
    await query.answer()

    if query.data and query.data.startswith("copy:"):
        address = query.data.split("copy:", 1)[1]
        await query.message.reply_text(
            f"📋 *Tap and hold to copy:*\n\n`{address}`",
            parse_mode=ParseMode.MARKDOWN_V2,
        )


def _esc_v2(text: str) -> str:
    specials = r"\_*[]()~`>#+-=|{}.!"
    return "".join(f"\\{c}" if c in specials else c for c in text)


# ══════════════════════════════════════════════════════════════════════════════
#  ENTRY POINT
# ══════════════════════════════════════════════════════════════════════════════

def main() -> None:
    logger.info("Starting Ethereum Wallet Analyzer Bot…")

    app = (
        Application.builder()
        .token(TELEGRAM_BOT_TOKEN)
        .build()
    )

    app.add_handler(CommandHandler("start", cmd_start))
    app.add_handler(CommandHandler("trace", cmd_trace))
    app.add_handler(CallbackQueryHandler(handle_copy_callback, pattern=r"^copy:"))

    logger.info("Bot is running. Press Ctrl+C to stop.")
    app.run_polling(allowed_updates=["message", "callback_query"])


if __name__ == "__main__":
    main()
