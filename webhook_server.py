import os
import requests
from flask import Flask, request, jsonify
import anthropic
import yfinance as yf
from datetime import datetime

app = Flask(__name__)

TELEGRAM_TOKEN    = os.environ.get("TELEGRAM_TOKEN", "")
ANTHROPIC_API_KEY = os.environ.get("ANTHROPIC_API_KEY", "")
TELEGRAM_CHAT_ID  = os.environ.get("TELEGRAM_CHAT_ID", "")

# ─────────────────────────────────────────
# MEMORY
# ─────────────────────────────────────────
signal_log      = []
circuit_breaker = {"consecutive_losses": 0}

# ─────────────────────────────────────────
# TELEGRAM
# ─────────────────────────────────────────
def send_telegram(text):
    try:
        requests.post(
            f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage",
            json={"chat_id": TELEGRAM_CHAT_ID, "text": text, "parse_mode": "Markdown"},
            timeout=10
        )
    except Exception as e:
        print(f"Telegram error: {e}")

# ─────────────────────────────────────────
# TELEGRAM COMMAND LISTENER
# Polls Telegram for /commands from you
# ─────────────────────────────────────────
last_update_id = 0

def poll_telegram_commands():
    global last_update_id
    try:
        r = requests.get(
            f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/getUpdates",
            params={"offset": last_update_id + 1, "timeout": 1},
            timeout=5
        )
        updates = r.json().get("result", [])
        for update in updates:
            last_update_id = update["update_id"]
            message = update.get("message", {})
            text    = message.get("text", "").strip()
            if text:
                handle_command(text)
    except Exception as e:
        print(f"Poll error: {e}")

def handle_command(text):
    parts  = text.strip().split()
    cmd    = parts[0].lower() if parts else ""
    ticker = parts[1].upper() if len(parts) > 1 else None

    if cmd == "/win" and ticker:
        mark_signal(ticker, "win")

    elif cmd == "/loss" and ticker:
        mark_signal(ticker, "loss")

    elif cmd == "/ignore" and ticker:
        mark_signal(ticker, "ignore")

    elif cmd == "/report":
        send_report()

    elif cmd == "/status":
        send_status()

    elif cmd == "/info":
        send_info()

    elif cmd == "/start":
        send_info()

def mark_signal(ticker, result):
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
                f"Win rate: {f'{win_rate:.0f}%' if win_rate else 'Building...'}"
            )
            return

    send_telegram(f"⚠️ No pending signal found for *{ticker}*\nMake sure you have an active alert logged first.")

def send_report():
    win_rate = get_win_rate()
    total    = len(signal_log)
    pending  = len([s for s in signal_log if s["result"] == "pending"])
    wins     = len([s for s in signal_log if s["result"] == "win"])
    losses   = len([s for s in signal_log if s["result"] == "loss"])
    ignored  = len([s for s in signal_log if s["result"] == "ignore"])

    # Combo breakdown
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
✅ Wins: {wins}
❌ Losses: {losses}
🚫 Ignored: {ignored}
⏳ Pending: {pending}
🎯 Win Rate: {f"{win_rate:.0f}%" if win_rate else "Need more data"}

🔧 *ADAPTIVE STATUS:*
{"🔴 Circuit breaker ACTIVE — 3+ losses" if circuit_breaker["consecutive_losses"] >= 3 else "🟢 System running normally"}
{"📉 Filters TIGHTENED" if win_rate and win_rate < 35 else "📈 Filters NORMAL"}
Consecutive losses: {circuit_breaker["consecutive_losses"]}

📋 *COMBO BREAKDOWN:*"""

    for combo, stats in combos.items():
        total_c = stats["win"] + stats["loss"]
        wr = f"{(stats['win']/total_c*100):.0f}%" if total_c > 0 else "N/A"
        report += f"\n• {combo}: {stats['win']}W / {stats['loss']}L ({wr})"

    report += f"\n\n📋 *LAST 5 SIGNALS:*"
    for s in signal_log[-5:]:
        emoji = "✅" if s["result"] == "win" else "❌" if s["result"] == "loss" else "🚫" if s["result"] == "ignore" else "⏳"
        report += f"\n{emoji} {s['ticker']} | {s['combo']} | ${s['price']} | {s['time'][11:16]}"

    send_telegram(report)

def send_status():
    win_rate = get_win_rate()
    status = (
        f"🤖 *BOT STATUS*\n"
        f"Signals logged: {len(signal_log)}\n"
        f"Win rate: {f'{win_rate:.0f}%' if win_rate else 'Building...'}\n"
        f"Consecutive losses: {circuit_breaker['consecutive_losses']}\n"
        f"{'🔴 Circuit breaker ACTIVE' if circuit_breaker['consecutive_losses'] >= 3 else '🟢 Running normally'}"
    )
    send_telegram(status)

def send_info():
    info = """🤖 *AI CHART BOT — COMMANDS*
━━━━━━━━━━━━━━━━━━━━
📈 *LOGGING TRADES:*
`/win NVDA` — mark most recent NVDA alert as a win
`/loss NVDA` — mark most recent NVDA alert as a loss
`/ignore NVDA` — skip it, don't count in stats

📊 *PERFORMANCE:*
`/report` — full breakdown with win rate, combo stats, last 5 signals
`/status` — quick one-line system health check

ℹ️ *HELP:*
`/info` — show this command list

━━━━━━━━━━━━━━━━━━━━
💡 *Tips:*
• You don't have to log every alert
• Only log trades you actually took
• `/ignore` keeps your win rate clean
• After 10+ logged trades the bot starts adapting filters automatically"""
    send_telegram(info)

# ─────────────────────────────────────────
# WIN RATE
# ─────────────────────────────────────────
def get_win_rate():
    if len(signal_log) < 5:
        return None
    last10   = [s for s in signal_log[-20:] if s["result"] in ["win", "loss"]]
    if len(last10) < 3:
        return None
    wins = len([s for s in last10 if s["result"] == "win"])
    return (wins / len(last10)) * 100

# ─────────────────────────────────────────
# REGIME DETECTION
# ─────────────────────────────────────────
def detect_regime(ticker):
    try:
        df = yf.download(ticker, period="30d", interval="1d", progress=False)
        if df.empty or len(df) < 10:
            return "unknown", 50.0

        closes = df["Close"].values.flatten()
        highs  = df["High"].values.flatten()
        lows   = df["Low"].values.flatten()

        ema10   = sum(closes[-10:]) / 10
        ema20   = sum(closes[-20:]) / 20
        atr     = sum([highs[i] - lows[i] for i in range(-10, 0)]) / 10
        atr_pct = (atr / closes[-1]) * 100

        gains, losses = [], []
        for i in range(-14, 0):
            diff = float(closes[i]) - float(closes[i-1])
            if diff > 0:
                gains.append(diff); losses.append(0)
            else:
                gains.append(0); losses.append(abs(diff))

        avg_gain = sum(gains) / 14 if gains else 0.001
        avg_loss = sum(losses) / 14 if losses else 0.001
        rsi = 100 - (100 / (1 + avg_gain / avg_loss))

        score = 0
        if float(ema10) > float(ema20): score += 30
        if float(closes[-1]) > float(ema10): score += 20
        if atr_pct > 1.5: score += 25
        if 40 < rsi < 65: score += 25

        regime = "TRENDING 📈" if score >= 60 else "MIXED ↔️" if score >= 35 else "CHOPPY ⚠️"
        return regime, rsi

    except Exception as e:
        print(f"Regime error: {e}")
        return "unknown", 50.0

# ─────────────────────────────────────────
# ADAPTIVE FILTER
# ─────────────────────────────────────────
def should_fire(ticker, combo, regime, rsi):
    skip = []
    if circuit_breaker["consecutive_losses"] >= 3:
        skip.append("⛔ Circuit breaker — 3 consecutive losses")
    if regime == "CHOPPY ⚠️" and "Dip" in combo:
        skip.append("⚠️ Choppy market — Dip & Rip less reliable")
    win_rate = get_win_rate()
    if win_rate is not None and win_rate < 35:
        skip.append(f"📉 Win rate at {win_rate:.0f}% — filters tightened")
    if rsi > 75 and "Momentum" in combo:
        skip.append(f"🔴 RSI {rsi:.0f} — overbought, momentum risky")
    return (False, skip) if skip else (True, [])

# ─────────────────────────────────────────
# CANDLE DATA
# ─────────────────────────────────────────
def get_candle_data(ticker):
    try:
        df = yf.download(ticker, period="5d", interval="15m", progress=False)
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
# ─────────────────────────────────────────
def analyze_with_claude(alert_message, candle_data, ticker, regime, rsi, win_rate):
    client       = anthropic.Anthropic(api_key=ANTHROPIC_API_KEY)
    win_rate_str = f"{win_rate:.0f}%" if win_rate else "Building history..."

    prompt = f"""You are an expert trading analyst. TradingView fired this confluence alert:
"{alert_message}"

MARKET CONTEXT:
- Regime: {regime}
- RSI(14): {rsi:.1f}
- Recent win rate: {win_rate_str}

Last 10 candles for {ticker}:
{candle_data}

Give a concise high-probability trading analysis:

📊 *SIGNAL* — What triggered and why it matters
🌍 *REGIME* — How market conditions affect this setup
🎯 *BIAS* — Bullish/Bearish/Neutral + conviction Low/Medium/High
💰 *ENTRY* — Ideal entry zone
🎯 *TARGET* — Price target 1 and target 2
🛑 *STOP LOSS* — Exact invalidation level
⏱ *HOLD TIME* — Expected duration
⚠️ *RISK* — Key risks

Be specific with price levels. This is a live trader."""

    msg = client.messages.create(
        model="claude-sonnet-4-20250514",
        max_tokens=500,
        messages=[{"role": "user", "content": prompt}]
    )
    return msg.content[0].text

# ─────────────────────────────────────────
# LOG
# ─────────────────────────────────────────
def log_signal(ticker, combo, price):
    signal_log.append({
        "ticker": ticker,
        "combo":  combo,
        "price":  price,
        "time":   str(datetime.now()),
        "result": "pending"
    })
    if len(signal_log) > 100:
        signal_log.pop(0)

# ─────────────────────────────────────────
# MAIN WEBHOOK
# ─────────────────────────────────────────
@app.route("/webhook", methods=["POST"])
def webhook():
    # Check for Telegram commands first
    poll_telegram_commands()

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

        # Extract combo
        combo = "Unknown"
        if "DIP" in alert_text.upper():       combo = "Dip & Rip"
        elif "MOMENTUM" in alert_text.upper(): combo = "Momentum Breakout"
        elif "BREAKDOWN" in alert_text.upper(): combo = "Breakdown Warning"

        # Extract price
        try:
            price_part = [p for p in alert_text.split("|") if "Price:" in p]
            price = float(price_part[0].replace("Price:", "").strip()) if price_part else 0.0
        except:
            price = 0.0

        regime, rsi = detect_regime(ticker)
        win_rate    = get_win_rate()
        fire, skip  = should_fire(ticker, combo, regime, rsi)

        if not fire:
            warning  = f"⚠️ *SIGNAL FILTERED — {ticker}*\n"
            warning += f"Combo: {combo} | Regime: {regime} | RSI: {rsi:.0f}\n\n"
            warning += "🚫 *Reasons:*\n" + "\n".join(f"• {r}" for r in skip)
            warning += "\n\n_Use /info to see commands_"
            send_telegram(warning)
            return jsonify({"status": "filtered"}), 200

        candle_data  = get_candle_data(ticker)
        analysis     = analyze_with_claude(alert_text, candle_data, ticker, regime, rsi, win_rate)
        win_rate_str = f"{win_rate:.0f}%" if win_rate else "Building..."
        combo_emoji  = "🎣" if "Dip" in combo else "🚀" if "Momentum" in combo else "🚨"

        message = f"""{combo_emoji} *{combo.upper()} — {ticker}*
🌍 Regime: {regime} | RSI: {rsi:.0f} | Win Rate: {win_rate_str}

{analysis}

_Signal #{len(signal_log)+1} | {datetime.now().strftime('%H:%M')} ET_
_Log it: /win {ticker} or /loss {ticker} or /ignore {ticker}_"""

        send_telegram(message)
        log_signal(ticker, combo, price)
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
    poll_telegram_commands()
    win_rate = get_win_rate()
    return (
        f"🤖 Chart Bot Level 2 ✅\n"
        f"Signals: {len(signal_log)}\n"
        f"Win rate: {f'{win_rate:.0f}%' if win_rate else 'Building...'}\n"
        f"Consecutive losses: {circuit_breaker['consecutive_losses']}"
    ), 200

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5000))
    app.run(host="0.0.0.0", port=port)
