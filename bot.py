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
from telegram import Update
from telegram.ext import (
    Application,
    CommandHandler,
    ContextTypes,
)
from telegram.constants import ParseMode

# ── Environment ──────────────────────────────────────────────────────────────
load_dotenv()

TELEGRAM_BOT_TOKEN: str = os.environ["TELEGRAM_BOT_TOKEN"]
ETHERSCAN_API_KEY: str = os.environ["ETHERSCAN_API_KEY"]

ETHERSCAN_BASE_URL = "https://api.etherscan.io/api"
MAX_TRANSACTIONS = 1000          # cap to avoid timeouts
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
    """
    Fetch the latest MAX_TRANSACTIONS normal (ETH) transactions for *wallet*
    from the Etherscan API.

    Returns a list of raw transaction dicts or raises RuntimeError on failure.
    """
    params = {
        "module":     "account",
        "action":     "txlist",
        "address":    wallet,
        "startblock": 0,
        "endblock":   99_999_999,
        "page":       1,
        "offset":     MAX_TRANSACTIONS,
        "sort":       "desc",          # newest first so we capture recent activity
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

    # Etherscan returns status "0" with message "No transactions found" for empty wallets
    if payload.get("status") == "0":
        msg = payload.get("message", "")
        result = payload.get("result", "")
        if "No transactions found" in (msg + str(result)):
            return []
        # Rate-limit or auth error
        raise RuntimeError(f"Etherscan error: {payload.get('result', msg)}")

    if payload.get("status") != "1":
        raise RuntimeError(f"Unexpected Etherscan response: {payload}")

    return payload["result"]


# ══════════════════════════════════════════════════════════════════════════════
#  ANALYSIS LAYER
# ══════════════════════════════════════════════════════════════════════════════

def analyze_wallet(wallet: str, transactions: list[dict]) -> dict | None:
    """
    Filter outgoing transactions and compute per-recipient statistics.

    Returns a dict with the top-recipient data, or None if there are no
    outgoing transactions.

    Schema returned:
    {
        "address":      str,          # top recipient address
        "tx_count":     int,          # number of outgoing txns to them
        "total_eth":    float,        # total ETH sent (in Ether)
        "first_ts":     datetime,     # earliest transaction timestamp (UTC)
        "last_ts":      datetime,     # latest  transaction timestamp (UTC)
        "total_out_txns": int,        # total outgoing txns across all recipients
    }
    """
    wallet_lower = wallet.lower()

    # Aggregate by recipient
    recipient_txns:  dict[str, list[dict]] = defaultdict(list)

    for tx in transactions:
        sender = tx.get("from", "").lower()
        to     = tx.get("to",   "").lower()

        # Only count outgoing, non-contract-creation, non-error transactions
        if sender != wallet_lower:
            continue
        if not to:                          # contract creation — skip
            continue
        if tx.get("isError") == "1":        # failed tx — skip
            continue

        recipient_txns[to].append(tx)

    if not recipient_txns:
        return None

    # Find the recipient with the most transactions
    top_address = max(recipient_txns, key=lambda addr: len(recipient_txns[addr]))
    top_txns    = recipient_txns[top_address]

    # Sum ETH sent (value is in Wei)
    total_wei = sum(int(tx.get("value", 0)) for tx in top_txns)
    total_eth = total_wei / 1e18

    # Determine activity window
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
    """Return 'Mon YYYY' string or 'N/A' for a datetime."""
    if dt is None:
        return "N/A"
    return dt.strftime("%b %Y")


def format_result(queried_wallet: str, analysis: dict | None, total_txns: int) -> str:
    """
    Build the Telegram-ready Markdown message for the analysis result.
    Uses MarkdownV2 escaping where required for special characters.
    """
    if analysis is None:
        return (
            "🔍 *Wallet Analysis Complete*\n\n"
            f"Wallet `{queried_wallet}` has *no outgoing transactions* "
            f"in the latest {MAX_TRANSACTIONS} records\\.\n\n"
            "_This wallet may only receive funds or has no activity yet\\._"
        )

    addr      = analysis["address"]
    tx_count  = analysis["tx_count"]
    eth_val   = analysis["total_eth"]
    first_str = _fmt_month(analysis["first_ts"])
    last_str  = _fmt_month(analysis["last_ts"])
    total_out = analysis["total_out_txns"]

    # Determine period label
    if analysis["first_ts"] and analysis["last_ts"]:
        if first_str == last_str:
            period = first_str
        else:
            period = f"{first_str} → {last_str}"
    else:
        period = "Unknown"

    # Escape MarkdownV2 special chars in dynamic strings
    def esc(s: str) -> str:
        """Escape special MarkdownV2 characters."""
        specials = r"\_*[]()~`>#+-=|{}.!"
        return "".join(f"\\{c}" if c in specials else c for s in [s] for c in s)

    eth_display = f"{eth_val:.4f}".rstrip("0").rstrip(".")

    lines = [
        "🔎 *Top Recipient Wallet*",
        "",
        f"📬 *Address:* `{addr}`",
        f"🔁 *Transactions Received:* {esc(str(tx_count))}",
        f"💰 *Total ETH Sent:* {esc(eth_display)} ETH",
        f"📅 *Activity Period:* {esc(period)}",
        "",
        "─────────────────────────",
        f"📊 Scanned wallet sent *{esc(str(tx_count))}* txns to this address",
        f"   out of *{esc(str(total_out))}* total outgoing txns analysed\\.",
        "",
        "📋 *Copy Address:*",
        f"`{addr}`",
        "",
        f"🔗 [View on Etherscan](https://etherscan.io/address/{addr})",
    ]
    return "\n".join(lines)


# ══════════════════════════════════════════════════════════════════════════════
#  VALIDATION HELPERS
# ══════════════════════════════════════════════════════════════════════════════

def is_valid_eth_address(addr: str) -> bool:
    """Return True if *addr* matches the Ethereum address pattern."""
    return bool(ETH_ADDRESS_RE.match(addr))


# ══════════════════════════════════════════════════════════════════════════════
#  TELEGRAM COMMAND HANDLERS
# ══════════════════════════════════════════════════════════════════════════════

async def cmd_start(update: Update, _context: ContextTypes.DEFAULT_TYPE) -> None:
    """Handle /start — send the welcome / usage message."""
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
    """
    Handle /trace <wallet_address>.

    Flow:
    1. Validate the supplied address.
    2. Send a "Scanning…" acknowledgement.
    3. Fetch transactions via Etherscan.
    4. Analyse outgoing transaction patterns.
    5. Format and reply with the result.
    """
    # ── Parse argument ────────────────────────────────────────────────────────
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

    # ── Acknowledge & scan ────────────────────────────────────────────────────
    scanning_msg = await update.message.reply_text(
        f"🔄 *Scanning wallet…*\n\n`{wallet}`\n\n_Fetching up to {MAX_TRANSACTIONS:,} transactions\\.\\.\\._",
        parse_mode=ParseMode.MARKDOWN_V2,
    )

    try:
        transactions = fetch_transactions(wallet)

        # Update progress
        await scanning_msg.edit_text(
            f"⚙️ *Analysing transactions…*\n\n`{wallet}`\n\n"
            f"_Found {len(transactions):,} transactions\\. Crunching numbers\\.\\.\\._",
            parse_mode=ParseMode.MARKDOWN_V2,
        )

        analysis = analyze_wallet(wallet, transactions)
        result   = format_result(wallet, analysis, len(transactions))

        # Deliver final result (delete progress msg, send clean reply)
        await scanning_msg.delete()
        await update.message.reply_text(result, parse_mode=ParseMode.MARKDOWN_V2)

    except RuntimeError as exc:
        logger.warning("RuntimeError for wallet %s: %s", wallet, exc)
        await scanning_msg.edit_text(
            f"❌ *Error during analysis*\n\n{_esc_v2(str(exc))}",
            parse_mode=ParseMode.MARKDOWN_V2,
        )
    except Exception as exc:  # noqa: BLE001
        logger.exception("Unexpected error for wallet %s", wallet)
        await scanning_msg.edit_text(
            "❌ *An unexpected error occurred\\.*\n\n"
            "_Please try again in a few moments\\._",
            parse_mode=ParseMode.MARKDOWN_V2,
        )


def _esc_v2(text: str) -> str:
    """Escape a plain string for safe MarkdownV2 rendering."""
    specials = r"\_*[]()~`>#+-=|{}.!"
    return "".join(f"\\{c}" if c in specials else c for c in text)


# ══════════════════════════════════════════════════════════════════════════════
#  ENTRY POINT
# ══════════════════════════════════════════════════════════════════════════════

def main() -> None:
    """Build and start the Telegram bot (long-polling mode)."""
    logger.info("Starting Ethereum Wallet Analyzer Bot…")

    app = (
        Application.builder()
        .token(TELEGRAM_BOT_TOKEN)
        .build()
    )

    app.add_handler(CommandHandler("start", cmd_start))
    app.add_handler(CommandHandler("trace", cmd_trace))

    logger.info("Bot is running. Press Ctrl+C to stop.")
    app.run_polling(allowed_updates=["message"])


if __name__ == "__main__":
    main()
