# 🔍 Ethereum Wallet Analyzer — Telegram Bot

Trace any Ethereum wallet and instantly identify its **top transaction recipient**, including total ETH sent and the activity period.

---

## ✨ Features

- `/start` — welcome message and usage guide
- `/trace <address>` — full wallet analysis
  - Identifies the address that received the **most outgoing transactions**
  - Shows total ETH sent, transaction count, and activity time period
  - Live "Scanning…" progress updates
  - Copyable address formatted for Telegram
- Validates Ethereum address format before querying
- Graceful handling of API rate limits and errors
- Capped at latest 1,000 transactions to prevent timeouts

---

## 🗂 Project Structure

```
eth-wallet-bot/
├── bot.py              # Main bot — all logic lives here
├── requirements.txt    # Python dependencies
├── .env.example        # Environment variable template
├── .gitignore
├── railway.json        # Railway deployment config
└── README.md
```

---

## 🚀 Quick Start (Local)

### 1. Clone the repo

```bash
git clone https://github.com/yourname/eth-wallet-bot.git
cd eth-wallet-bot
```

### 2. Create a virtual environment

```bash
python3 -m venv venv
source venv/bin/activate   # Windows: venv\Scripts\activate
```

### 3. Install dependencies

```bash
pip install -r requirements.txt
```

### 4. Configure environment variables

```bash
cp .env.example .env
```

Edit `.env` and fill in your keys:

```env
TELEGRAM_BOT_TOKEN=your_telegram_bot_token_here
ETHERSCAN_API_KEY=your_etherscan_api_key_here
```

### 5. Run the bot

```bash
python bot.py
```

---

## 🔑 Getting API Keys

### Telegram Bot Token
1. Open Telegram and search for **@BotFather**
2. Send `/newbot` and follow the prompts
3. Copy the token (looks like `123456:ABCdef...`)

### Etherscan API Key
1. Create a free account at [etherscan.io](https://etherscan.io)
2. Go to **My Profile → API Keys → Add**
3. Copy the generated key

---

## ☁️ Deploy on Railway

### Prerequisites
- [Railway account](https://railway.app) (free tier works)
- GitHub account (to push your code)

### Steps

#### 1. Push your code to GitHub
```bash
git init
git add .
git commit -m "initial commit"
git remote add origin https://github.com/yourname/eth-wallet-bot.git
git push -u origin main
```

#### 2. Create a new Railway project
1. Go to [railway.app](https://railway.app) → **New Project**
2. Select **Deploy from GitHub repo**
3. Choose your `eth-wallet-bot` repository

#### 3. Add Environment Variables
In the Railway dashboard:
1. Click your service → **Variables** tab
2. Add the following:

| Variable              | Value                        |
|-----------------------|------------------------------|
| `TELEGRAM_BOT_TOKEN`  | `your_telegram_bot_token`    |
| `ETHERSCAN_API_KEY`   | `your_etherscan_api_key`     |

#### 4. Deploy
Railway will automatically detect Python and run `python bot.py` as defined in `railway.json`.

Check the **Logs** tab to confirm: `Bot is running. Press Ctrl+C to stop.`

---

## 💬 Bot Usage

```
/start
```
> Shows welcome message and usage instructions.

```
/trace 0xde0B295669a9FD93d5F28D9Ec85E40f4cb697BAe
```

> **Example Output:**
>
> 🔎 **Top Recipient Wallet**
>
> 📬 **Address:** `0xabc123...`  
> 🔁 **Transactions Received:** 57  
> 💰 **Total ETH Sent:** 12.4 ETH  
> 📅 **Activity Period:** Jan 2024 → Mar 2024  
>
> 📋 **Copy Address:**  
> `0xabc123...`  
>
> 🔗 View on Etherscan

---

## ⚠️ Notes

- The free Etherscan API plan allows **5 requests/second** and **100,000 calls/day** — sufficient for normal bot usage.
- Analysis is capped at the **latest 1,000 transactions** per wallet to keep response times fast.
- Failed transactions (`isError=1`) and contract creation transactions are excluded from analysis.

---

## 🛡 Security

- API keys are loaded from environment variables only — never hardcoded.
- Add `.env` to `.gitignore` (already included) to prevent accidental exposure.
