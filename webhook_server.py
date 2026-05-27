import os
import json
import time
import threading
import requests
from flask import Flask, request, jsonify
import anthropic
import yfinance as yf
from datetime import datetime, timedelta
import pytz

app = Flask(__name__)

TELEGRAM_TOKEN    = os.environ.get("TELEGRAM_TOKEN", "")
ANTHROPIC_API_KEY = os.environ.get("ANTHROPIC_API_KEY", "")
TELEGRAM_CHAT_ID  = os.environ.get("TELEGRAM_CHAT_ID", "")
DATA_FILE         = "/tmp/bot_data.json"  # persistent across restarts on paid Render
                                           # on free tier use external DB (see README)

# ─────────────────────────────────────────
# PERSISTENT STORAGE
# Saves to disk so data survives restarts
# ─────────────────────────────────────────
def load_data():
    try:
        if os.path.exists(DATA_FILE):
            with open(DATA_FILE, "r") as f:
                return json.load(f)
    except:
        pass
    return {
        "signal_log": [],
        "circuit_breaker": {"consecutive_losses": 0},
        "last_update_id": 0
    }

def save_data(data):
    try:
        with open(DATA_FILE, "w") as f:
            json.dump(data, f, indent=2)
    except Exception as e:
        print(f"Save error: {e}")

# Load on startup
bot_data = load_data()
signal_log      = bot_data.get("signal_log", [])
circuit_breaker = bot_data.get("circuit_breaker", {"consecutive_losses": 0})
last_update_id  = bot_data.get("last_update_id", 0)

def persist():
    save_data({
        "signal_log":      signal_log,
        "circuit_breaker": circuit_breaker,
        "last_update_id":  last_update_id
    })

# ─────────────────────────────────────────
# TELEGRAM
# ─────────────────────────────────────────
def send_telegram(text, chat_id=None):
    try:
        requests.post(
            f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage",
            json={
                "chat_id":    chat_id or TELEGRAM_CHAT_ID,
                "text":       text,
                "parse_mode": "Markdown"
            },
            timeout=10
        )
    except Exception as e:
        print(f"Telegram error: {e}")

# ─────────────────────────────────────────
# CONTINUOUS TELEGRAM POLLING
# Runs in background thread — always on
# Commands work ANY time, not just on alerts
# ─────────────────────────────────────────
def telegram_polling_loop():
    global last_update_id
    print("Telegram polling loop started")
    while True:
        try:
            r = requests.get(
                f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/getUpdates",
                params={"offset": last_update_id + 1, "timeout": 30},
                timeout=35
            )
            updates = r.json().get("result", [])
            for update in updates:
                last_update_id = update["update_id"]
                message = update.get("message", {})
                text    = message.get("text", "").strip()
                chat_id = message.get("chat", {}).get("id")
                if text and chat_id:
                    handle_command(text, str(chat_id))
            persist()
        except Exception as e:
            print(f"Polling error: {e}")
            time.sleep(5)
        time.sleep(1)

# Start polling in background thread
polling_thread = threading.Thread(target=telegram_polling_loop, daemon=True)
polling_thread.start()

# ─────────────────────────────────────────
# AUTO-EXPIRE PENDING SIGNALS
# Runs every hour — marks old pending as expired
# ─────────────────────────────────────────
def auto_expire_loop():
    while True:
        try:
            now = datetime.now()
            for s in signal_log:
                if s["result"] == "pending":
                    signal_time = datetime.fromisoformat(s["time"])
                    hours_old   = (now - signal_time).total_seconds() / 3600
                    if hours_old > 48:
                        s["result"] = "expired"
                        print(f"Expired signal: {s['ticker']} {s['combo']}")
            persist()
        except Exception as e:
            print(f"Expire loop error: {e}")
        time.sleep(3600)  # run every hour

expire_thread = threading.Thread(target=auto_expire_loop, daemon=True)
expire_thread.start()

# ─────────────────────────────────────────
# AUTO PRICE CHECK
# 4hrs and 24hrs after signal — checks if
# target or stop was hit automatically
# ─────────────────────────────────────────
def check_signal_outcomes():
    while True:
        try:
            now = datetime.now()
            for s in signal_log:
                if s["result"] != "pending":
                    continue
                if "target1" not in s or "stop" not in s:
                    continue

                signal_time = datetime.fromisoformat(s["time"])
                hours_old   = (now - signal_time).total_seconds() / 3600

                # Check at 4hr and 24hr marks
                if hours_old < 4:
                    continue

                checked_4h  = s.get("checked_4h", False)
                checked_24h = s.get("checked_24h", False)

                if hours_old >= 4 and not checked_4h:
                    s["checked_4h"] = True
                    check_price_vs_levels(s, "4hr")

                if hours_old >= 24 and not checked_24h:
                    s["checked_24h"] = True
                    check_price_vs_levels(s, "24hr")

            persist()
        except Exception as e:
            print(f"Outcome check error: {e}")
        time.sleep(900)  # every 15 min

def check_price_vs_levels(signal, timeframe):
    try:
        ticker  = signal["ticker"]
        target1 = signal.get("target1", 0)
        target2 = signal.get("target2", 0)
        stop    = signal.get("stop", 0)
        entry   = signal.get("price", 0)

        if not any([target1, target2, stop, entry]):
            return

        df = yf.download(ticker, period="1d", interval="5m", progress=False)
        if df.empty:
            return

        current = float(df["Close"].values[-1])
        high    = float(df["High"].max())
        low     = float(df["Low"].min())

        # Determine if bullish or bearish signal
        is_bullish = signal.get("bias", "bullish").lower() == "bullish"

        result_msg = f"🔍 *AUTO CHECK — {ticker} ({timeframe})*\n"
        result_msg += f"Entry: ${entry:.2f} | Current: ${current:.2f}\n"

        if is_bullish:
            pnl = ((current - entry) / entry * 100) if entry > 0 else 0
            if target1 > 0 and high >= target1:
                result_msg += f"✅ *TARGET 1 HIT* (${target1:.2f})\n"
                signal["result"] = "win"
            elif target2 > 0 and high >= target2:
                result_msg += f"🎯 *TARGET 2 HIT* (${target2:.2f})\n"
                signal["result"] = "win"
            elif stop > 0 and low <= stop:
                result_msg += f"🛑 *STOP LOSS HIT* (${stop:.2f})\n"
                signal["result"] = "loss"
                circuit_breaker["consecutive_losses"] += 1
            else:
                result_msg += f"⏳ Still in play | P&L: {pnl:+.1f}%\n"
        else:
            pnl = ((entry - current) / entry * 100) if entry > 0 else 0
            if target1 > 0 and low <= target1:
                result_msg += f"✅ *TARGET 1 HIT* (${target1:.2f})\n"
                signal["result"] = "win"
            elif stop > 0 and high >= stop:
                result_msg += f"🛑 *STOP LOSS HIT* (${stop:.2f})\n"
                signal["result"] = "loss"
                circuit_breaker["consecutive_losses"] += 1
            else:
                result_msg += f"⏳ Still in play | P&L: {pnl:+.1f}%\n"

        result_msg += f"High: ${high:.2f} | Low: ${low:.2f}"
        send_telegram(result_msg)

    except Exception as e:
        print(f"Price check error: {e}")

outcome_thread = threading.Thread(target=check_signal_outcomes, daemon=True)
outcome_thread.start()

# ─────────────────────────────────────────
# COMMAND HANDLER
# ─────────────────────────────────────────
def handle_command(text, chat_id):
    parts  = text.strip().split()
    cmd    = parts[0].lower() if parts else ""
    ticker = parts[1].upper() if len(parts) > 1 else None

    if cmd in ["/win", "/loss", "/ignore"] and ticker:
        result = cmd.replace("/", "")
        mark_signal(ticker, result, chat_id)
    elif cmd == "/report":
        send_report(chat_id)
    elif cmd == "/status":
        send_status(chat_id)
    elif cmd in ["/info", "/start", "/help"]:
        send_info(chat_id)
    else:
        send_telegram(
            "❓ Unknown command. Type /info to see all commands.",
            chat_id
        )

def mark_signal(ticker, result, chat_id):
    for s in reversed(signal_log):
        if s["ticker"] == ticker and s["result"] == "pending":
            s["result"] = result
            if result == "win":
                circuit_breaker["consecutive_losses"] = 0
                emoji = "✅"
            elif result == "loss":
                circuit_breaker["consecutive_losses"] += 1
                emoji = "❌"
            else:
                emoji = "🚫"

            win_rate = get_win_rate()
            send_telegram(
                f"{emoji} *{result.upper()}* logged for *{ticker}*\n"
                f"Consecutive losses: {circuit_breaker['consecutive_losses']}\n"
                f"Win rate: {f'{win_rate:.0f}%' if win_rate else 'Building...'}",
                chat_id
            )
            persist()
            return

    send_telegram(
        f"⚠️ No pending signal found for *{ticker}*\n"
        f"Check /report to see pending signals.",
        chat_id
    )

def send_report(chat_id):
    win_rate = get_win_rate()
    total    = len(signal_log)
    pending  = len([s for s in signal_log if s["result"] == "pending"])
    wins     = len([s for s in signal_log if s["result"] == "win"])
    losses   = len([s for s in signal_log if s["result"] == "loss"])
    ignored  = len([s for s in signal_log if s["result"] == "ignore"])
    expired  = len([s for s in signal_log if s["result"] == "expired"])
    auto_wins = len([s for s in signal_log if s["result"] == "win" and s.get("checked_4h")])

    combos = {}
    for s in signal_log:
        c = s["combo"]
        if c not in combos:
            combos[c] = {"win": 0, "loss": 0}
        if s["result"] in ["win", "loss"]:
            combos[c][s["result"]] += 1

    report = f"""📊 *PERFORMANCE REPORT*
━━━━━━━━━━━━━━━━━━━━
📈 Total Signals: {total}
✅ Wins: {wins} ({auto_wins} auto-detected)
❌ Losses: {losses}
🚫 Ignored: {ignored}
⏳ Pending: {pending}
⌛ Expired: {expired}
🎯 Win Rate: {f"{win_rate:.0f}%" if win_rate else "Need more data (5+ trades)"}

🔧 *ADAPTIVE STATUS:*
{"🔴 Circuit breaker ACTIVE" if circuit_breaker["consecutive_losses"] >= 3 else "🟢 System running normally"}
{"📉 Filters TIGHTENED — win rate below 35%" if win_rate and win_rate < 35 else "📈 Filters NORMAL"}
Consecutive losses: {circuit_breaker["consecutive_losses"]}

📋 *COMBO BREAKDOWN:*"""

    for combo, stats in combos.items():
        total_c = stats["win"] + stats["loss"]
        wr      = f"{(stats['win']/total_c*100):.0f}%" if total_c > 0 else "N/A"
        report += f"\n• {combo}: {stats['win']}W / {stats['loss']}L ({wr})"

    report += "\n\n📋 *LAST 5 SIGNALS:*"
    for s in signal_log[-5:]:
        emoji  = "✅" if s["result"] == "win" else "❌" if s["result"] == "loss" else "🚫" if s["result"] == "ignore" else "⌛" if s["result"] == "expired" else "⏳"
        report += f"\n{emoji} {s['ticker']} | {s['combo']} | ${s['price']} | {s['time'][11:16]}"

    send_telegram(report, chat_id)

def send_status(chat_id):
    win_rate = get_win_rate()
    pending  = len([s for s in signal_log if s["result"] == "pending"])
    send_telegram(
        f"🤖 *BOT STATUS*\n"
        f"Signals logged: {len(signal_log)}\n"
        f"Pending: {pending}\n"
        f"Win rate: {f'{win_rate:.0f}%' if win_rate else 'Building...'}\n"
        f"Consecutive losses: {circuit_breaker['consecutive_losses']}\n"
        f"{'🔴 Circuit breaker ACTIVE' if circuit_breaker['consecutive_losses'] >= 3 else '🟢 Running normally'}\n"
        f"Polling: 🟢 Active",
        chat_id
    )

def send_info(chat_id):
    send_telegram(
        "🤖 *AI CHART BOT — COMMANDS*\n"
        "━━━━━━━━━━━━━━━━━━━━\n"
        "📈 *LOGGING TRADES:*\n"
        "`/win NVDA` — mark most recent NVDA alert as win\n"
        "`/loss NVDA` — mark most recent NVDA alert as loss\n"
        "`/ignore NVDA` — skip it, don't count in stats\n\n"
        "📊 *PERFORMANCE:*\n"
        "`/report` — full breakdown with win rate & combo stats\n"
        "`/status` — quick system health check\n\n"
        "ℹ️ *HELP:*\n"
        "`/info` — show this command list\n\n"
        "━━━━━━━━━━━━━━━━━━━━\n"
        "💡 *How it works:*\n"
        "• Bot auto-checks if target/stop hit at 4hr & 24hr\n"
        "• Pending signals expire after 48hrs automatically\n"
        "• Win rate below 35% = filters tighten automatically\n"
        "• 3 losses in a row = circuit breaker activates\n"
        "• Only log trades YOU actually took\n"
        "• /ignore keeps win rate clean for skipped alerts",
        chat_id
    )

# ─────────────────────────────────────────
# WIN RATE (excludes ignored + expired)
# ─────────────────────────────────────────
def get_win_rate():
    resolved = [s for s in signal_log if s["result"] in ["win", "loss"]]
    last10   = resolved[-10:]
    if len(last10) < 3:
        return None
    wins = len([s for s in last10 if s["result"] == "win"])
    return (wins / len(last10)) * 100

# ─────────────────────────────────────────
# REGIME DETECTION
# Uses 60 days for swing trade accuracy
# Higher timeframe uses EMA50 not VWAP
# ─────────────────────────────────────────
def detect_regime(ticker, interval_str=""):
    try:
        # Use longer lookback for swing timeframes
        is_swing = any(x in interval_str.upper() for x in ["1D", "1W", "D", "W", "DAY", "WEEK"])
        period   = "60d" if is_swing else "30d"

        df = yf.download(ticker, period=period, interval="1d", progress=False)
        if df.empty or len(df) < 10:
            return "unknown", 50.0, False

        closes = df["Close"].values.flatten()
        highs  = df["High"].values.flatten()
        lows   = df["Low"].values.flatten()
        vols   = df["Volume"].values.flatten()

        n      = len(closes)
        ema10  = sum(closes[-10:]) / 10
        ema20  = sum(closes[-min(20, n):]) / min(20, n)
        ema50  = sum(closes[-min(50, n):]) / min(50, n)
        atr    = sum([highs[i] - lows[i] for i in range(-10, 0)]) / 10
        atr_pct = (atr / closes[-1]) * 100

        # Volume trend
        vol_avg_recent = sum(vols[-5:]) / 5
        vol_avg_older  = sum(vols[-20:-5]) / 15
        vol_increasing = vol_avg_recent > vol_avg_older

        # RSI
        gains, losses = [], []
        for i in range(-14, 0):
            diff = float(closes[i]) - float(closes[i-1])
            if diff > 0: gains.append(diff); losses.append(0)
            else: gains.append(0); losses.append(abs(diff))

        avg_gain = sum(gains) / 14 if gains else 0.001
        avg_loss = sum(losses) / 14 if losses else 0.001
        rsi = 100 - (100 / (1 + avg_gain / avg_loss))

        # Pre-market awareness
        et_tz = pytz.timezone("America/New_York")
        now_et = datetime.now(et_tz)
        is_premarket = dtime(4, 0) <= now_et.time() < dtime(9, 30)
        is_afterhours = now_et.time() >= dtime(16, 0)

        score = 0
        if float(ema10) > float(ema20): score += 25
        if float(ema20) > float(ema50): score += 25
        if float(closes[-1]) > float(ema10): score += 15
        if atr_pct > 1.5: score += 20
        if 40 < rsi < 65: score += 15

        regime = "TRENDING 📈" if score >= 65 else "MIXED ↔️" if score >= 40 else "CHOPPY ⚠️"
        return regime, rsi, is_premarket or is_afterhours

    except Exception as e:
        print(f"Regime error: {e}")
        return "unknown", 50.0, False

# ─────────────────────────────────────────
# ADAPTIVE FILTER
# ─────────────────────────────────────────
def should_fire(ticker, combo, regime, rsi, is_extended_hours):
    skip = []

    if circuit_breaker["consecutive_losses"] >= 3:
        skip.append("⛔ Circuit breaker — 3 consecutive losses. Sitting out.")

    if regime == "CHOPPY ⚠️" and "Dip" in combo:
        skip.append("⚠️ Choppy market — Dip & Rip less reliable right now")

    win_rate = get_win_rate()
    if win_rate is not None and win_rate < 35:
        skip.append(f"📉 Win rate at {win_rate:.0f}% — filters tightened, skipping marginal setups")

    if rsi > 75 and "Momentum" in combo:
        skip.append(f"🔴 RSI {rsi:.0f} — overbought, momentum breakout high risk")

    if is_extended_hours:
        skip.append("🌙 Extended hours — lower volume, wider spreads. Trade with caution.")
        # Don't block — just warn

    return (False, skip) if [s for s in skip if not s.startswith("🌙")] else (True, skip)

# ─────────────────────────────────────────
# CANDLE DATA
# Intraday vs swing aware
# ─────────────────────────────────────────
def get_candle_data(ticker, interval_str=""):
    try:
        is_swing = any(x in interval_str.upper() for x in ["1D", "1W", "D", "W", "DAY", "WEEK"])
        period   = "30d" if is_swing else "5d"
        interval = "1d"  if is_swing else "15m"

        df = yf.download(ticker, period=period, interval=interval, progress=False)
        if df.empty:
            return "No data available"

        lines = []
        for ts, row in df.tail(10).iterrows():
            lines.append(
                f"{ts.strftime('%m/%d %H:%M')} | O:{float(row['Open']):.2f} "
                f"H:{float(row['High']):.2f} L:{float(row['Low']):.2f} "
                f"C:{float(row['Close']):.2f} V:{int(float(row['Volume']))}"
            )
        return "\n".join(lines)
    except Exception as e:
        return f"Data error: {e}"

# ─────────────────────────────────────────
# CLAUDE ANALYSIS
# Returns analysis + extracted price levels
# ─────────────────────────────────────────
def analyze_with_claude(alert_message, candle_data, ticker, regime, rsi, win_rate, is_extended):
    client       = anthropic.Anthropic(api_key=ANTHROPIC_API_KEY)
    win_rate_str = f"{win_rate:.0f}%" if win_rate else "Building history..."
    ext_note     = "⚠️ NOTE: Currently extended hours — lower liquidity." if is_extended else ""

    prompt = f"""You are an expert trading analyst. TradingView fired this confluence alert:
"{alert_message}"

MARKET CONTEXT:
- Regime: {regime}
- RSI(14): {rsi:.1f}
- Recent win rate: {win_rate_str}
{ext_note}

Last 10 candles for {ticker}:
{candle_data}

Give a concise high-probability trading analysis in this EXACT format:

📊 *SIGNAL* — What triggered and why it matters
🌍 *REGIME* — How market conditions affect this setup
🎯 *BIAS* — Bullish/Bearish/Neutral + conviction: Low/Medium/High
💰 *ENTRY* — Ideal entry zone (give exact price or range)
🎯 *TARGET 1* — First price target (give exact price)
🎯 *TARGET 2* — Second price target (give exact price)
🛑 *STOP LOSS* — Exact invalidation level (give exact price)
⏱ *HOLD TIME* — Expected duration
📐 *POSITION SIZE* — Suggested risk % of account (conservative)
⚠️ *RISK* — Key risks to this setup

Be specific with ALL price levels. This is a live trader."""

    msg = client.messages.create(
        model="claude-sonnet-4-20250514",
        max_tokens=600,
        messages=[{"role": "user", "content": prompt}]
    )
    return msg.content[0].text

def extract_price_levels(analysis_text, current_price):
    """Extract target and stop prices from Claude's analysis for auto-checking"""
    import re
    levels = {"target1": 0, "target2": 0, "stop": 0, "bias": "bullish"}

    try:
        # Extract TARGET 1
        t1_match = re.search(r"TARGET 1.*?\$?([\d.]+)", analysis_text)
        if t1_match:
            levels["target1"] = float(t1_match.group(1))

        # Extract TARGET 2
        t2_match = re.search(r"TARGET 2.*?\$?([\d.]+)", analysis_text)
        if t2_match:
            levels["target2"] = float(t2_match.group(1))

        # Extract STOP
        stop_match = re.search(r"STOP.*?\$?([\d.]+)", analysis_text)
        if stop_match:
            levels["stop"] = float(stop_match.group(1))

        # Extract bias
        if "bearish" in analysis_text.lower():
            levels["bias"] = "bearish"

    except Exception as e:
        print(f"Price extraction error: {e}")

    return levels

# ─────────────────────────────────────────
# LOG SIGNAL
# ─────────────────────────────────────────
def log_signal(ticker, combo, price, levels=None):
    entry = {
        "ticker":  ticker,
        "combo":   combo,
        "price":   price,
        "time":    str(datetime.now()),
        "result":  "pending",
        "checked_4h":  False,
        "checked_24h": False
    }
    if levels:
        entry.update(levels)

    signal_log.append(entry)
    if len(signal_log) > 100:
        signal_log.pop(0)
    persist()

# ─────────────────────────────────────────
# MAIN WEBHOOK
# ─────────────────────────────────────────
@app.route("/webhook", methods=["POST"])
def webhook():
    try:
        alert_text = request.get_data(as_text=True)
        if not alert_text:
            data       = request.get_json(silent=True) or {}
            alert_text = data.get("message", "Alert received")

        print(f"Alert: {alert_text}")

        # Extract ticker
        ticker = "SPY"
        parts  = alert_text.split("—")
        if len(parts) > 1:
            ticker = parts[1].strip().split("|")[0].strip()

        # Extract interval
        interval_str = ""
        if "|" in alert_text:
            alert_parts = alert_text.split("|")
            if len(alert_parts) > 1:
                interval_str = alert_parts[1].strip()

        # Extract combo
        combo = "Unknown"
        if "DIP" in alert_text.upper():        combo = "Dip & Rip"
        elif "MOMENTUM" in alert_text.upper():  combo = "Momentum Breakout"
        elif "BREAKDOWN" in alert_text.upper(): combo = "Breakdown Warning"

        # Extract price
        try:
            price_part = [p for p in alert_text.split("|") if "Price:" in p]
            price = float(price_part[0].replace("Price:", "").strip()) if price_part else 0.0
        except:
            price = 0.0

        # Analysis
        regime, rsi, is_extended = detect_regime(ticker, interval_str)
        win_rate                 = get_win_rate()
        fire, skip               = should_fire(ticker, combo, regime, rsi, is_extended)

        # Hard block
        hard_blocked = any(not s.startswith("🌙") for s in skip) if not fire else False

        if hard_blocked:
            warning  = f"⚠️ *SIGNAL FILTERED — {ticker}*\n"
            warning += f"Combo: {combo} | Regime: {regime} | RSI: {rsi:.0f}\n\n"
            warning += "🚫 *Reasons:*\n" + "\n".join(f"• {r}" for r in skip if not r.startswith("🌙"))
            warning += "\n\n_Type /info for commands_"
            send_telegram(warning)
            return jsonify({"status": "filtered"}), 200

        # Get data and analyze
        candle_data = get_candle_data(ticker, interval_str)
        analysis    = analyze_with_claude(
            alert_text, candle_data, ticker,
            regime, rsi, win_rate, is_extended
        )

        # Extract price levels for auto-checking
        levels = extract_price_levels(analysis, price)

        win_rate_str = f"{win_rate:.0f}%" if win_rate else "Building..."
        combo_emoji  = "🎣" if "Dip" in combo else "🚀" if "Momentum" in combo else "🚨"

        # Extended hours warning
        ext_warning = "\n⚠️ _Extended hours — lower liquidity_" if is_extended else ""

        message = (
            f"{combo_emoji} *{combo.upper()} — {ticker}*\n"
            f"🌍 Regime: {regime} | RSI: {rsi:.0f} | Win Rate: {win_rate_str}"
            f"{ext_warning}\n\n"
            f"{analysis}\n\n"
            f"_Signal #{len(signal_log)+1} | {datetime.now().strftime('%H:%M')} ET_\n"
            f"_Log: /win {ticker} • /loss {ticker} • /ignore {ticker}_"
        )

        send_telegram(message)
        log_signal(ticker, combo, price, levels)
        return jsonify({"status": "ok"}), 200

    except Exception as e:
        print(f"Error: {e}")
        send_telegram(f"❌ Bot error: {str(e)}")
        return jsonify({"error": str(e)}), 500

# ─────────────────────────────────────────
# HEALTH
# ─────────────────────────────────────────
@app.route("/", methods=["GET"])
def health():
    win_rate = get_win_rate()
    return (
        f"🤖 Chart Bot Level 2 ✅\n"
        f"Signals: {len(signal_log)}\n"
        f"Win rate: {f'{win_rate:.0f}%' if win_rate else 'Building...'}\n"
        f"Consecutive losses: {circuit_breaker['consecutive_losses']}\n"
        f"Polling: Active 🟢"
    ), 200

# ─────────────────────────────────────────
# IMPORT FOR EXTENDED HOURS CHECK
# ─────────────────────────────────────────
from datetime import time as dtime

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5000))
    app.run(host="0.0.0.0", port=port)
