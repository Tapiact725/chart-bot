import os
import json
import time
import threading
import requests
import re
from flask import Flask, request, jsonify
import anthropic
import yfinance as yf
from datetime import datetime, timedelta
from datetime import time as dtime
import pytz

app = Flask(__name__)

TELEGRAM_TOKEN    = os.environ.get("TELEGRAM_TOKEN", "")
ANTHROPIC_API_KEY = os.environ.get("ANTHROPIC_API_KEY", "")
TELEGRAM_CHAT_ID  = os.environ.get("TELEGRAM_CHAT_ID", "")
NEWS_API_KEY      = os.environ.get("NEWS_API_KEY", "")
ALPHA_VANTAGE_KEY = os.environ.get("ALPHA_VANTAGE_KEY", "")  # free at alphavantage.co
SHEETS_WEBHOOK    = os.environ.get("SHEETS_WEBHOOK", "")
DATA_FILE         = "/tmp/bot_data.json"

WATCHLIST = ["NVDA", "TSLA", "AMD", "SMH", "ARM", "AAPL", "META", "MSFT", "SPY", "QQQ"]

SECTOR_MAP = {
    "NVDA": "SMH", "AMD": "SMH", "ARM": "SMH", "INTC": "SMH",
    "TSLA": "QQQ", "AAPL": "QQQ", "META": "QQQ", "MSFT": "QQQ",
    "SPY":  "SPY", "QQQ":  "QQQ"
}

CORRELATED_GROUPS = [
    ["NVDA", "AMD", "ARM", "SMH", "INTC"],
    ["AAPL", "MSFT", "META", "QQQ"],
    ["TSLA"]
]

# ─────────────────────────────────────────
# PERSISTENT STORAGE
# ─────────────────────────────────────────
def load_data():
    try:
        if os.path.exists(DATA_FILE):
            with open(DATA_FILE, "r") as f:
                return json.load(f)
    except:
        pass
    return {
        "signal_log":      [],
        "circuit_breaker": {"consecutive_losses": 0},
        "last_update_id":  0,
        "open_trades":     {}
    }

def save_data():
    try:
        with open(DATA_FILE, "w") as f:
            json.dump({
                "signal_log":      signal_log,
                "circuit_breaker": circuit_breaker,
                "last_update_id":  last_update_id,
                "open_trades":     open_trades
            }, f, indent=2, default=str)
    except Exception as e:
        print(f"Save error: {e}")

bot_data        = load_data()
signal_log      = bot_data.get("signal_log", [])
circuit_breaker = bot_data.get("circuit_breaker", {"consecutive_losses": 0})
last_update_id  = bot_data.get("last_update_id", 0)
open_trades     = bot_data.get("open_trades", {})

# ─────────────────────────────────────────
# TELEGRAM
# ─────────────────────────────────────────
def send_telegram(text, chat_id=None, reply_markup=None):
    try:
        payload = {
            "chat_id":    chat_id or TELEGRAM_CHAT_ID,
            "text":       text[:4000],
            "parse_mode": "Markdown"
        }
        if reply_markup:
            payload["reply_markup"] = json.dumps(reply_markup)
        requests.post(
            f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage",
            json=payload, timeout=10
        )
    except Exception as e:
        print(f"Telegram error: {e}")

def send_with_buttons(text, ticker, chat_id=None):
    markup = {
        "inline_keyboard": [[
            {"text": "✅ Win",    "callback_data": f"win_{ticker}"},
            {"text": "❌ Loss",   "callback_data": f"loss_{ticker}"},
            {"text": "🚫 Ignore", "callback_data": f"ignore_{ticker}"}
        ]]
    }
    send_telegram(text, chat_id, reply_markup=markup)

def answer_callback(cq_id):
    try:
        requests.post(
            f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/answerCallbackQuery",
            json={"callback_query_id": cq_id}, timeout=5
        )
    except:
        pass

# ─────────────────────────────────────────
# CONTINUOUS TELEGRAM POLLING
# ─────────────────────────────────────────
def telegram_polling_loop():
    global last_update_id
    print("Telegram polling started")
    while True:
        try:
            r = requests.get(
                f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/getUpdates",
                params={"offset": last_update_id + 1, "timeout": 30},
                timeout=35
            )
            for update in r.json().get("result", []):
                last_update_id = update["update_id"]

                if "callback_query" in update:
                    cq      = update["callback_query"]
                    data    = cq.get("data", "")
                    chat_id = str(cq["message"]["chat"]["id"])
                    answer_callback(cq["id"])
                    if "_" in data:
                        action, ticker = data.split("_", 1)
                        mark_signal(ticker.upper(), action, chat_id)

                elif "message" in update:
                    msg     = update["message"]
                    text    = msg.get("text", "").strip()
                    chat_id = str(msg.get("chat", {}).get("id", ""))
                    if text and chat_id:
                        handle_command(text, chat_id)

            save_data()
        except Exception as e:
            print(f"Polling error: {e}")
            time.sleep(5)
        time.sleep(1)

threading.Thread(target=telegram_polling_loop, daemon=True).start()

# ─────────────────────────────────────────
# AUTO EXPIRE (48hrs)
# ─────────────────────────────────────────
def auto_expire_loop():
    while True:
        try:
            now = datetime.now()
            for s in signal_log:
                if s["result"] == "pending":
                    try:
                        if (now - datetime.fromisoformat(str(s["time"]))).total_seconds() / 3600 > 48:
                            s["result"] = "expired"
                    except:
                        pass
            save_data()
        except Exception as e:
            print(f"Expire error: {e}")
        time.sleep(3600)

threading.Thread(target=auto_expire_loop, daemon=True).start()

# ─────────────────────────────────────────
# AUTO PRICE CHECK (4hr + 24hr)
# ─────────────────────────────────────────
def check_outcomes_loop():
    while True:
        try:
            now = datetime.now()
            for s in signal_log:
                if s["result"] != "pending":
                    continue
                if not all(k in s for k in ["target1", "stop"]):
                    continue
                try:
                    hours_old = (now - datetime.fromisoformat(str(s["time"]))).total_seconds() / 3600
                    if hours_old >= 4 and not s.get("checked_4h"):
                        s["checked_4h"] = True
                        check_price_vs_levels(s, "4hr")
                    if hours_old >= 24 and not s.get("checked_24h"):
                        s["checked_24h"] = True
                        check_price_vs_levels(s, "24hr")
                except:
                    pass
            save_data()
        except Exception as e:
            print(f"Outcome check error: {e}")
        time.sleep(900)

def check_price_vs_levels(signal, timeframe):
    try:
        ticker  = signal["ticker"]
        target1 = float(signal.get("target1", 0))
        target2 = float(signal.get("target2", 0))
        stop    = float(signal.get("stop", 0))
        entry   = float(signal.get("price", 0))
        bias    = signal.get("bias", "bullish")

        df = yf.download(ticker, period="1d", interval="1m", progress=False)
        if df.empty:
            return

        current = float(df["Close"].values[-1])
        high    = float(df["High"].max())
        low     = float(df["Low"].min())
        pnl     = ((current - entry) / entry * 100) if entry > 0 else 0

        msg = f"🔍 *AUTO CHECK — {ticker} ({timeframe})*\n"
        msg += f"Entry: ${entry:.2f} → Now: ${current:.2f} ({pnl:+.1f}%)\n"

        if bias == "bullish":
            if target2 > 0 and high >= target2:
                msg += f"🎯 *TARGET 2 HIT* (${target2:.2f}) 🔥"
                signal["result"] = "win"
                circuit_breaker["consecutive_losses"] = 0
            elif target1 > 0 and high >= target1:
                msg += f"✅ *TARGET 1 HIT* (${target1:.2f})"
                signal["result"] = "win"
                circuit_breaker["consecutive_losses"] = 0
            elif stop > 0 and low <= stop:
                msg += f"🛑 *STOP HIT* (${stop:.2f})"
                signal["result"] = "loss"
                circuit_breaker["consecutive_losses"] += 1
            else:
                msg += f"⏳ Still in play | H:${high:.2f} L:${low:.2f}"
        else:
            if target1 > 0 and low <= target1:
                msg += f"✅ *TARGET 1 HIT* (${target1:.2f})"
                signal["result"] = "win"
                circuit_breaker["consecutive_losses"] = 0
            elif stop > 0 and high >= stop:
                msg += f"🛑 *STOP HIT* (${stop:.2f})"
                signal["result"] = "loss"
                circuit_breaker["consecutive_losses"] += 1
            else:
                msg += f"⏳ Still in play | H:${high:.2f} L:${low:.2f}"

        send_telegram(msg)
        save_data()
    except Exception as e:
        print(f"Price check error: {e}")

threading.Thread(target=check_outcomes_loop, daemon=True).start()

# ─────────────────────────────────────────
# MORNING BRIEF (9am ET weekdays)
# WEEKLY SUMMARY (Sunday 8pm ET)
# ─────────────────────────────────────────
def scheduler_loop():
    while True:
        try:
            et_tz  = pytz.timezone("America/New_York")
            now_et = datetime.now(et_tz)
            if now_et.weekday() < 5 and now_et.hour == 9 and now_et.minute == 0:
                send_morning_brief()
                time.sleep(61)
            if now_et.weekday() == 6 and now_et.hour == 20 and now_et.minute == 0:
                send_weekly_summary()
                time.sleep(61)
        except Exception as e:
            print(f"Scheduler error: {e}")
        time.sleep(30)

threading.Thread(target=scheduler_loop, daemon=True).start()

def send_morning_brief():
    try:
        et_tz  = pytz.timezone("America/New_York")
        now_et = datetime.now(et_tz)
        brief  = f"🌅 *MORNING BRIEF — {now_et.strftime('%A %b %d')}*\n━━━━━━━━━━━━━━━━━━━━\n\n"

        # Market regime
        spy_regime, spy_rsi, _ = get_regime_info("SPY")
        qqq_regime, qqq_rsi, _ = get_regime_info("QQQ")
        brief += f"📊 *MARKET:*\nSPY: {spy_regime} | RSI: {spy_rsi:.0f}\n"
        brief += f"QQQ: {qqq_regime} | RSI: {qqq_rsi:.0f}\n\n"

        # Pre-market movers
        brief += "📈 *PRE-MARKET MOVERS:*\n"
        movers = []
        for ticker in WATCHLIST[:8]:
            try:
                df = yf.download(ticker, period="2d", interval="1d", progress=False)
                if len(df) >= 2:
                    prev = float(df["Close"].values[-2])
                    curr = float(df["Close"].values[-1])
                    chg  = ((curr - prev) / prev) * 100
                    movers.append((ticker, chg, curr))
            except:
                pass
        movers.sort(key=lambda x: abs(x[1]), reverse=True)
        for ticker, chg, price in movers[:5]:
            emoji = "🟢" if chg > 0 else "🔴"
            brief += f"{emoji} {ticker}: ${price:.2f} ({chg:+.1f}%)\n"

        # Setups
        brief += "\n🎯 *TOP SETUPS:*\n"
        setups = scan_watchlist_setups()
        for s in (setups[:3] if setups else ["No high-conviction setups yet"]):
            brief += f"• {s}\n"

        # Earnings
        brief += "\n📅 *EARNINGS THIS WEEK:*\n"
        earnings = check_earnings_week()
        for e in (earnings if earnings else ["No major earnings in watchlist"]):
            brief += f"⚠️ {e}\n"

        send_telegram(brief)
    except Exception as e:
        print(f"Morning brief error: {e}")

def send_weekly_summary():
    try:
        win_rate    = get_win_rate()
        week_ago    = datetime.now() - timedelta(days=7)
        week_sigs   = [s for s in signal_log if datetime.fromisoformat(str(s["time"])) > week_ago]
        combo_stats = {}

        for s in week_sigs:
            c = s["combo"]
            if c not in combo_stats:
                combo_stats[c] = {"win": 0, "loss": 0}
            if s["result"] in ["win", "loss"]:
                combo_stats[c][s["result"]] += 1

        summary  = f"📊 *WEEKLY SUMMARY*\n━━━━━━━━━━━━━━━━━━━━\n"
        summary += f"Signals this week: {len(week_sigs)}\n"
        summary += f"Win rate: {f'{win_rate:.0f}%' if win_rate else 'Building...'}\n\n"
        summary += "*COMBO PERFORMANCE:*\n"

        for combo, stats in combo_stats.items():
            total = stats["win"] + stats["loss"]
            wr    = f"{(stats['win']/total*100):.0f}%" if total > 0 else "N/A"
            summary += f"• {combo}: {stats['win']}W/{stats['loss']}L ({wr})\n"

        send_telegram(summary)
    except Exception as e:
        print(f"Weekly summary error: {e}")

# ─────────────────────────────────────────
# REAL TIME DATA
# Uses Alpha Vantage if key available
# Falls back to yfinance 1min otherwise
# ─────────────────────────────────────────
def get_candle_data(ticker, interval_str=""):
    # Try Alpha Vantage first (most accurate)
    if ALPHA_VANTAGE_KEY:
        try:
            is_swing  = any(x in str(interval_str).upper() for x in ["1D","1W","D","W","DAY","WEEK"])
            av_interval = "daily" if is_swing else "5min"
            r = requests.get(
                "https://www.alphavantage.co/query",
                params={
                    "function":   "TIME_SERIES_INTRADAY" if not is_swing else "TIME_SERIES_DAILY",
                    "symbol":     ticker,
                    "interval":   av_interval if not is_swing else None,
                    "outputsize": "compact",
                    "apikey":     ALPHA_VANTAGE_KEY
                },
                timeout=10
            )
            data = r.json()
            key  = [k for k in data.keys() if "Time Series" in k]
            if key:
                series = data[key[0]]
                lines  = []
                for ts, vals in list(series.items())[:10]:
                    lines.append(
                        f"{ts} | O:{float(vals['1. open']):.2f} "
                        f"H:{float(vals['2. high']):.2f} "
                        f"L:{float(vals['3. low']):.2f} "
                        f"C:{float(vals['4. close']):.2f} "
                        f"V:{int(float(vals['5. volume']))}"
                    )
                if lines:
                    return "\n".join(lines) + "\n_(Alpha Vantage — 5min data)_"
        except Exception as e:
            print(f"Alpha Vantage error: {e}")

    # Fall back to yfinance 1min
    try:
        is_swing = any(x in str(interval_str).upper() for x in ["1D","1W","D","W","DAY","WEEK"])
        df = yf.download(
            ticker,
            period="30d" if is_swing else "1d",
            interval="1d" if is_swing else "1m",
            progress=False
        )
        if df.empty:
            return "No data available"
        lines = []
        for ts, row in df.tail(10).iterrows():
            lines.append(
                f"{ts.strftime('%m/%d %H:%M')} | O:{float(row['Open']):.2f} "
                f"H:{float(row['High']):.2f} L:{float(row['Low']):.2f} "
                f"C:{float(row['Close']):.2f} V:{int(float(row['Volume']))}"
            )
        return "\n".join(lines) + "\n_(yfinance 1min)_"
    except Exception as e:
        return f"Data error: {e}"

# ─────────────────────────────────────────
# REGIME INFO (for morning brief + context)
# ─────────────────────────────────────────
def get_regime_info(ticker, interval_str=""):
    try:
        is_swing = any(x in str(interval_str).upper() for x in ["1D","1W","D","W","DAY","WEEK"])
        df = yf.download(ticker, period="60d" if is_swing else "30d", interval="1d", progress=False)
        if df.empty or len(df) < 10:
            return "unknown", 50.0, False

        closes = df["Close"].values.flatten()
        highs  = df["High"].values.flatten()
        lows   = df["Low"].values.flatten()
        vols   = df["Volume"].values.flatten()
        n      = len(closes)

        ema10  = sum(closes[-10:]) / 10
        ema20  = sum(closes[-min(20,n):]) / min(20,n)
        ema50  = sum(closes[-min(50,n):]) / min(50,n)
        atr    = sum([highs[i] - lows[i] for i in range(-10, 0)]) / 10
        atr_pct = (atr / closes[-1]) * 100

        vol_recent = sum(vols[-5:]) / 5
        vol_older  = sum(vols[-20:-5]) / 15 if n >= 20 else vol_recent

        gains, losses = [], []
        for i in range(-14, 0):
            diff = float(closes[i]) - float(closes[i-1])
            if diff > 0: gains.append(diff); losses.append(0)
            else: gains.append(0); losses.append(abs(diff))

        avg_gain = sum(gains) / 14 if gains else 0.001
        avg_loss = sum(losses) / 14 if losses else 0.001
        rsi = 100 - (100 / (1 + avg_gain / avg_loss))

        et_tz      = pytz.timezone("America/New_York")
        now_et     = datetime.now(et_tz)
        is_extended = now_et.time() < dtime(9, 30) or now_et.time() >= dtime(16, 0)

        score = 0
        if float(ema10) > float(ema20): score += 20
        if float(ema20) > float(ema50): score += 20
        if float(closes[-1]) > float(ema10): score += 15
        if atr_pct > 1.5: score += 20
        if 40 < rsi < 65: score += 15
        if vol_recent > vol_older: score += 10

        regime = "TRENDING 📈" if score >= 65 else "MIXED ↔️" if score >= 40 else "CHOPPY ⚠️"
        return regime, rsi, is_extended
    except:
        return "unknown", 50.0, False

# ─────────────────────────────────────────
# ENRICHMENT CHECKS
# These ADD info — they never block signals
# Only exception: circuit breaker + earnings
# ─────────────────────────────────────────

def check_news(ticker):
    if not NEWS_API_KEY:
        return None, []
    try:
        r = requests.get(
            "https://newsapi.org/v2/everything",
            params={"q": ticker, "sortBy": "publishedAt",
                    "pageSize": 5, "apiKey": NEWS_API_KEY, "language": "en"},
            timeout=5
        )
        articles = r.json().get("articles", [])
        if not articles:
            return "neutral", []

        neg_words = ["lawsuit","investigation","fraud","miss","loss","decline","crash",
                     "ban","recall","downgrade","warning","bankruptcy","sec","fine","hack"]
        pos_words = ["beat","upgrade","record","growth","profit","deal","partnership",
                     "launch","breakthrough","strong","surge","buy"]

        neg, pos, headlines = 0, 0, []
        for a in articles[:3]:
            title = a.get("title", "").lower()
            headlines.append(a.get("title", ""))
            neg += sum(1 for w in neg_words if w in title)
            pos += sum(1 for w in pos_words if w in title)

        sentiment = "negative" if neg > pos + 1 else "positive" if pos > neg else "neutral"
        return sentiment, headlines
    except:
        return None, []

def check_earnings(ticker):
    try:
        cal = yf.Ticker(ticker).calendar
        if cal is None or (hasattr(cal, 'empty') and cal.empty):
            return None
        if hasattr(cal, 'columns') and 'Earnings Date' in cal.columns:
            dates = cal['Earnings Date']
            if len(dates) > 0:
                ed = dates.iloc[0]
                if hasattr(ed, 'date'):
                    days = (ed.date() - datetime.now().date()).days
                    return days if 0 <= days <= 14 else None
        return None
    except:
        return None

def check_earnings_week():
    warnings = []
    for ticker in WATCHLIST:
        days = check_earnings(ticker)
        if days is not None:
            warnings.append(f"{ticker} earnings in {days} day{'s' if days != 1 else ''}")
    return warnings

def check_sector(ticker):
    try:
        etf = SECTOR_MAP.get(ticker, "SPY")
        if etf == ticker:
            return None
        df = yf.download(etf, period="5d", interval="1d", progress=False)
        if df.empty or len(df) < 2:
            return None
        chg = ((float(df["Close"].values[-1]) - float(df["Close"].values[-2]))
               / float(df["Close"].values[-2]) * 100)
        if chg < -1.5:
            return f"⚠️ Sector ({etf}) down {chg:.1f}% today — weakens bullish signal"
        elif chg > 1.5:
            return f"✅ Sector ({etf}) up {chg:.1f}% today — strengthens bullish signal"
        return f"↔️ Sector ({etf}) flat ({chg:+.1f}%)"
    except:
        return None

def check_options(ticker):
    try:
        stock = yf.Ticker(ticker)
        opts  = stock.options
        if not opts:
            return None
        chain   = stock.option_chain(opts[0])
        call_vol = chain.calls["volume"].sum()
        put_vol  = chain.puts["volume"].sum()
        if call_vol + put_vol == 0:
            return None
        pcr = put_vol / (call_vol + 1)
        if pcr > 1.5:
            return f"🐋 High PUT activity (P/C: {pcr:.1f}) — smart money hedging"
        elif pcr < 0.5:
            return f"🐋 High CALL activity (P/C: {pcr:.1f}) — bullish flow detected"
        return None
    except:
        return None

def check_mtf(ticker, interval_str):
    try:
        is_intraday = any(x in str(interval_str) for x in ["1","5","15","30","60"])
        if not is_intraday:
            return None
        df = yf.download(ticker, period="30d", interval="1d", progress=False)
        if df.empty or len(df) < 20:
            return None
        closes = df["Close"].values.flatten()
        ema10  = sum(closes[-10:]) / 10
        ema20  = sum(closes[-20:]) / 20
        last   = float(closes[-1])
        if last > ema10 > ema20:
            return "✅ Daily uptrend confirmed — HIGH conviction"
        elif last < ema10 < ema20:
            return "⚠️ Daily downtrend — consider reducing size"
        return "↔️ Daily mixed — MEDIUM conviction"
    except:
        return None

def check_correlation(ticker):
    warnings = []
    for group in CORRELATED_GROUPS:
        if ticker in group:
            for ot in open_trades:
                if ot in group and ot != ticker:
                    warnings.append(f"⚠️ Open {ot} trade — {ticker} & {ot} highly correlated (double sector risk)")
    return warnings

def calculate_rr(entry, target1, stop):
    try:
        if not all([entry, target1, stop]):
            return None
        reward = abs(target1 - entry)
        risk   = abs(entry - stop)
        if risk == 0:
            return None
        rr    = reward / risk
        emoji = "✅" if rr >= 2 else "⚠️" if rr >= 1.5 else "❌ Low"
        return f"{emoji} R:R = {rr:.1f}:1 | Risk: ${risk:.2f} | Reward: ${reward:.2f}"
    except:
        return None

# ─────────────────────────────────────────
# WIN RATE
# ─────────────────────────────────────────
def get_win_rate():
    resolved = [s for s in signal_log if s["result"] in ["win","loss"]]
    last10   = resolved[-10:]
    if len(last10) < 3:
        return None
    return (len([s for s in last10 if s["result"] == "win"]) / len(last10)) * 100

# ─────────────────────────────────────────
# ONLY 2 HARD BLOCKS
# Everything else is info only
# ─────────────────────────────────────────
def hard_blocks_only(earnings_days):
    blocks = []
    if circuit_breaker["consecutive_losses"] >= 3:
        blocks.append("⛔ Circuit breaker active — 3 consecutive losses. Reset with /reset")
    if earnings_days is not None and earnings_days <= 2:
        blocks.append(f"📅 Earnings in {earnings_days} days — too risky to trade")
    return blocks

# ─────────────────────────────────────────
# WATCHLIST SCANNER
# ─────────────────────────────────────────
def scan_watchlist_setups():
    setups = []
    for ticker in WATCHLIST:
        try:
            df = yf.download(ticker, period="30d", interval="1d", progress=False)
            if df.empty or len(df) < 20:
                continue
            closes = df["Close"].values.flatten()
            highs  = df["High"].values.flatten()
            lows   = df["Low"].values.flatten()
            vols   = df["Volume"].values.flatten()
            ema10  = sum(closes[-10:]) / 10
            ema20  = sum(closes[-20:]) / 20
            last   = float(closes[-1])
            vol_avg = sum(vols[-20:]) / 20

            gains, losses = [], []
            for i in range(-14, 0):
                diff = float(closes[i]) - float(closes[i-1])
                if diff > 0: gains.append(diff); losses.append(0)
                else: gains.append(0); losses.append(abs(diff))
            avg_gain = sum(gains) / 14 if gains else 0.001
            avg_loss = sum(losses) / 14 if losses else 0.001
            rsi = 100 - (100 / (1 + avg_gain / avg_loss))

            if rsi < 35 and last < ema20 and float(vols[-1]) > vol_avg:
                setups.append(f"🎣 {ticker} — Oversold dip (RSI:{rsi:.0f}) + volume")
            if last > float(highs[-2]) and float(vols[-1]) > vol_avg * 1.3:
                setups.append(f"🚀 {ticker} — Breakout above recent high + volume")
            if float(ema10) > float(ema20) and last < float(ema10) * 1.005 and rsi < 50:
                setups.append(f"📈 {ticker} — Pullback to EMA10 in uptrend (RSI:{rsi:.0f})")
        except:
            pass
    return setups[:5]

# ─────────────────────────────────────────
# CLAUDE ANALYSIS
# ─────────────────────────────────────────
def analyze_with_claude(alert_msg, candle_data, ticker, regime, rsi,
                        win_rate, is_extended, enrichment_context):
    client       = anthropic.Anthropic(api_key=ANTHROPIC_API_KEY)
    win_rate_str = f"{win_rate:.0f}%" if win_rate else "Building..."

    prompt = f"""You are an expert trading analyst. TradingView fired this 3-way confluence alert:
"{alert_msg}"

MARKET CONTEXT (use this to inform your analysis):
- Regime: {regime}
- RSI(14): {rsi:.1f}
- Win rate: {win_rate_str}
- Extended hours: {is_extended}
{enrichment_context}

Last 10 candles for {ticker}:
{candle_data}

Respond in this EXACT format with specific prices:

📊 *SIGNAL* — What triggered and confluence significance
🌍 *REGIME* — How ALL context above affects this setup
🎯 *BIAS* — Bullish/Bearish/Neutral | Conviction: Low/Medium/High
💰 *ENTRY* — $XX.XX to $XX.XX
🎯 *TARGET 1* — $XX.XX (conservative)
🎯 *TARGET 2* — $XX.XX (extended)
🛑 *STOP LOSS* — $XX.XX
⏱ *HOLD TIME* — X hours/days
📐 *POSITION SIZE* — Risk X% of account (be conservative)
⚠️ *RISK* — Top 2 risks specific to this setup

Use ALL the context provided. Be specific with every price level."""

    msg = client.messages.create(
        model="claude-sonnet-4-20250514",
        max_tokens=600,
        messages=[{"role": "user", "content": prompt}]
    )
    return msg.content[0].text

def extract_levels(text, current_price):
    levels = {"target1": 0, "target2": 0, "stop": 0, "bias": "bullish"}
    try:
        t1 = re.search(r"TARGET 1.*?\$([\d.]+)", text)
        t2 = re.search(r"TARGET 2.*?\$([\d.]+)", text)
        sl = re.search(r"STOP.*?\$([\d.]+)", text)
        if t1: levels["target1"] = float(t1.group(1))
        if t2: levels["target2"] = float(t2.group(1))
        if sl: levels["stop"]    = float(sl.group(1))
        if "bearish" in text.lower(): levels["bias"] = "bearish"
    except:
        pass
    return levels

# ─────────────────────────────────────────
# LOG SIGNAL
# ─────────────────────────────────────────
def log_signal(ticker, combo, price, levels=None):
    entry = {
        "ticker":      ticker,
        "combo":       combo,
        "price":       price,
        "time":        str(datetime.now()),
        "result":      "pending",
        "checked_4h":  False,
        "checked_24h": False
    }
    if levels:
        entry.update(levels)
    signal_log.append(entry)
    if len(signal_log) > 100:
        signal_log.pop(0)
    if SHEETS_WEBHOOK:
        try:
            requests.post(SHEETS_WEBHOOK, json=entry, timeout=5)
        except:
            pass
    save_data()

# ─────────────────────────────────────────
# COMMAND HANDLER
# ─────────────────────────────────────────
def handle_command(text, chat_id):
    parts  = text.strip().split()
    cmd    = parts[0].lower()
    ticker = parts[1].upper() if len(parts) > 1 else None

    commands = {
        "/win":     lambda: mark_signal(ticker, "win", chat_id) if ticker else send_telegram("Usage: /win TICKER", chat_id),
        "/loss":    lambda: mark_signal(ticker, "loss", chat_id) if ticker else send_telegram("Usage: /loss TICKER", chat_id),
        "/ignore":  lambda: mark_signal(ticker, "ignore", chat_id) if ticker else send_telegram("Usage: /ignore TICKER", chat_id),
        "/report":  lambda: send_report(chat_id),
        "/status":  lambda: send_status(chat_id),
        "/scan":    lambda: send_scan(chat_id),
        "/brief":   lambda: send_morning_brief(),
        "/weekly":  lambda: send_weekly_summary(),
        "/reset":   lambda: reset_circuit_breaker(chat_id),
        "/info":    lambda: send_info(chat_id),
        "/start":   lambda: send_info(chat_id),
        "/help":    lambda: send_info(chat_id),
    }

    action = commands.get(cmd)
    if action:
        action()
    else:
        send_telegram("❓ Unknown command. Type /info for help.", chat_id)

def mark_signal(ticker, result, chat_id):
    for s in reversed(signal_log):
        if s["ticker"] == ticker and s["result"] == "pending":
            s["result"] = result
            if result == "win":
                circuit_breaker["consecutive_losses"] = 0
                emoji = "✅"
                open_trades.pop(ticker, None)
            elif result == "loss":
                circuit_breaker["consecutive_losses"] += 1
                emoji = "❌"
                open_trades.pop(ticker, None)
            else:
                emoji = "🚫"
            win_rate = get_win_rate()
            send_telegram(
                f"{emoji} *{result.upper()}* — *{ticker}*\n"
                f"Streak: {circuit_breaker['consecutive_losses']} losses\n"
                f"Win rate: {f'{win_rate:.0f}%' if win_rate else 'Building...'}",
                chat_id
            )
            save_data()
            return
    send_telegram(f"⚠️ No pending signal for *{ticker}*. Check /report.", chat_id)

def reset_circuit_breaker(chat_id):
    circuit_breaker["consecutive_losses"] = 0
    save_data()
    send_telegram("✅ Circuit breaker reset. Bot will fire alerts again.", chat_id)

def send_scan(chat_id):
    send_telegram("🔍 Scanning watchlist...", chat_id)
    setups = scan_watchlist_setups()
    if setups:
        send_telegram("📊 *CURRENT SETUPS:*\n" + "\n".join(f"• {s}" for s in setups), chat_id)
    else:
        send_telegram("No high-conviction setups right now. Wait for better conditions.", chat_id)

def send_report(chat_id):
    win_rate = get_win_rate()
    total    = len(signal_log)
    wins     = len([s for s in signal_log if s["result"] == "win"])
    losses   = len([s for s in signal_log if s["result"] == "loss"])
    ignored  = len([s for s in signal_log if s["result"] == "ignore"])
    pending  = len([s for s in signal_log if s["result"] == "pending"])
    expired  = len([s for s in signal_log if s["result"] == "expired"])

    combo_stats = {}
    for s in signal_log:
        c = s["combo"]
        if c not in combo_stats:
            combo_stats[c] = {"win": 0, "loss": 0}
        if s["result"] in ["win", "loss"]:
            combo_stats[c][s["result"]] += 1

    report  = f"📊 *PERFORMANCE REPORT*\n━━━━━━━━━━━━━━━━━━━━\n"
    report += f"Total: {total} | ✅{wins} | ❌{losses} | 🚫{ignored} | ⏳{pending} | ⌛{expired}\n"
    report += f"🎯 Win Rate: {f'{win_rate:.0f}%' if win_rate else 'Need 5+ trades'}\n\n"
    report += f"*STATUS:* {'🔴 Circuit breaker ACTIVE' if circuit_breaker['consecutive_losses'] >= 3 else '🟢 Normal'}\n"
    report += f"Consecutive losses: {circuit_breaker['consecutive_losses']}\n\n"
    report += "*COMBO BREAKDOWN:*\n"
    for combo, stats in combo_stats.items():
        total_c = stats["win"] + stats["loss"]
        wr = f"{(stats['win']/total_c*100):.0f}%" if total_c > 0 else "N/A"
        report += f"• {combo}: {stats['win']}W/{stats['loss']}L ({wr})\n"

    report += "\n*LAST 5 SIGNALS:*\n"
    for s in signal_log[-5:]:
        e = "✅" if s["result"]=="win" else "❌" if s["result"]=="loss" else "🚫" if s["result"]=="ignore" else "⌛" if s["result"]=="expired" else "⏳"
        report += f"{e} {s['ticker']} | {s['combo']} | ${s['price']} | {str(s['time'])[11:16]}\n"

    send_telegram(report, chat_id)

def send_status(chat_id):
    win_rate = get_win_rate()
    pending  = len([s for s in signal_log if s["result"] == "pending"])
    send_telegram(
        f"🤖 *BOT STATUS*\n"
        f"Signals: {len(signal_log)} | Pending: {pending}\n"
        f"Win rate: {f'{win_rate:.0f}%' if win_rate else 'Building...'}\n"
        f"Consecutive losses: {circuit_breaker['consecutive_losses']}\n"
        f"{'🔴 Circuit breaker ACTIVE — type /reset to clear' if circuit_breaker['consecutive_losses'] >= 3 else '🟢 Running normally'}\n"
        f"Open trades: {list(open_trades.keys()) or 'None'}\n"
        f"Polling: 🟢 | Auto-check: 🟢 | Scheduler: 🟢",
        chat_id
    )

def send_info(chat_id):
    send_telegram(
        "🤖 *AI CHART BOT — COMMANDS*\n"
        "━━━━━━━━━━━━━━━━━━━━\n"
        "📈 *TRADE LOGGING:*\n"
        "`/win NVDA` — mark most recent NVDA as win\n"
        "`/loss NVDA` — mark most recent NVDA as loss\n"
        "`/ignore NVDA` — skip, don't count in stats\n\n"
        "📊 *PERFORMANCE:*\n"
        "`/report` — full stats & combo breakdown\n"
        "`/status` — quick system health check\n\n"
        "🔍 *MARKET:*\n"
        "`/scan` — scan watchlist for setups now\n"
        "`/brief` — get morning brief on demand\n"
        "`/weekly` — weekly performance summary\n\n"
        "⚙️ *SYSTEM:*\n"
        "`/reset` — reset circuit breaker\n"
        "`/info` — show this list\n\n"
        "━━━━━━━━━━━━━━━━━━━━\n"
        "💡 *How TradingView + Bot work together:*\n"
        "• TradingView finds ALL signals (never blocked)\n"
        "• Bot adds news, earnings, sector, options info\n"
        "• Only 2 hard blocks: circuit breaker + earnings ≤2 days\n"
        "• Everything else shown as info — YOU decide\n"
        "• Tap ✅❌🚫 buttons on alerts to log instantly\n"
        "• Auto price checks at 4hr & 24hr\n"
        "• Morning brief 9am ET | Weekly recap Sundays",
        chat_id
    )

# ─────────────────────────────────────────
# MAIN WEBHOOK
# TradingView signal → enrich → send
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
            iparts = alert_text.split("|")
            if len(iparts) > 1:
                interval_str = iparts[1].strip()

        # Extract combo
        combo = "Unknown"
        if "DIP" in alert_text.upper():         combo = "Dip & Rip"
        elif "MOMENTUM" in alert_text.upper():   combo = "Momentum Breakout"
        elif "BREAKDOWN" in alert_text.upper():  combo = "Breakdown Warning"

        # Extract price
        try:
            pp    = [p for p in alert_text.split("|") if "Price:" in p]
            price = float(pp[0].replace("Price:", "").strip()) if pp else 0.0
        except:
            price = 0.0

        # ── ENRICHMENT (info only — never blocks) ──
        regime, rsi, is_extended = get_regime_info(ticker, interval_str)
        win_rate                  = get_win_rate()
        news_sentiment, headlines = check_news(ticker)
        sector_note               = check_sector(ticker)
        options_note              = check_options(ticker)
        earnings_days             = check_earnings(ticker)
        mtf_note                  = check_mtf(ticker, interval_str)
        corr_warns                = check_correlation(ticker)
        rr_note                   = None  # filled after Claude analysis

        # ── ONLY 2 HARD BLOCKS ──
        blocks = hard_blocks_only(earnings_days)
        if blocks:
            msg = f"🚫 *SIGNAL BLOCKED — {ticker}*\n"
            msg += f"TradingView fired: {combo}\n\n"
            msg += "⛔ *Hard block reasons:*\n"
            for b in blocks:
                msg += f"• {b}\n"
            msg += "\n_All other signals still fire normally_"
            send_telegram(msg)
            return jsonify({"status": "blocked"}), 200

        # ── GET DATA ──
        candle_data = get_candle_data(ticker, interval_str)

        # ── BUILD ENRICHMENT CONTEXT FOR CLAUDE ──
        ctx_lines = []
        if news_sentiment == "negative" and headlines:
            ctx_lines.append(f"⚠️ NEGATIVE NEWS: {headlines[0][:80]}")
        elif news_sentiment == "positive" and headlines:
            ctx_lines.append(f"✅ POSITIVE NEWS: {headlines[0][:80]}")
        if sector_note:
            ctx_lines.append(f"SECTOR: {sector_note}")
        if options_note:
            ctx_lines.append(f"OPTIONS: {options_note}")
        if mtf_note:
            ctx_lines.append(f"DAILY CHART: {mtf_note}")
        if earnings_days is not None:
            ctx_lines.append(f"⚠️ EARNINGS IN {earnings_days} DAYS — factor into hold time")
        if is_extended:
            ctx_lines.append("🌙 EXTENDED HOURS — lower liquidity")
        enrichment_context = "\n".join(ctx_lines)

        # ── CLAUDE ANALYSIS ──
        analysis = analyze_with_claude(
            alert_text, candle_data, ticker,
            regime, rsi, win_rate,
            is_extended, enrichment_context
        )

        # ── EXTRACT PRICE LEVELS ──
        levels = extract_levels(analysis, price)
        rr_note = calculate_rr(price, levels.get("target1", 0), levels.get("stop", 0))

        # ── BUILD TELEGRAM MESSAGE ──
        win_rate_str = f"{win_rate:.0f}%" if win_rate else "Building..."
        combo_emoji  = "🎣" if "Dip" in combo else "🚀" if "Momentum" in combo else "🚨"

        message  = f"{combo_emoji} *{combo.upper()} — {ticker}*\n"
        message += f"🌍 Regime: {regime} | RSI: {rsi:.0f} | Win Rate: {win_rate_str}\n"
        message += "━━━━━━━━━━━━━━━━━━━━\n"

        # Context info block
        if is_extended:
            message += "🌙 Extended hours — wider spreads\n"
        if earnings_days is not None:
            message += f"📅 Earnings in {earnings_days} days — adjust hold time\n"
        if news_sentiment == "negative":
            message += f"📰 Negative news detected — trade carefully\n"
        elif news_sentiment == "positive":
            message += f"📰 Positive news — adds confidence\n"
        if sector_note:
            message += f"{sector_note}\n"
        if options_note:
            message += f"{options_note}\n"
        if mtf_note:
            message += f"📊 {mtf_note}\n"
        for cw in corr_warns:
            message += f"{cw}\n"
        if rr_note:
            message += f"{rr_note}\n"

        message += "━━━━━━━━━━━━━━━━━━━━\n"
        message += f"{analysis}\n\n"
        message += f"_Signal #{len(signal_log)+1} | {datetime.now().strftime('%H:%M')} ET_"

        # ── SEND WITH BUTTONS ──
        send_with_buttons(message, ticker)

        # ── TRACK + LOG ──
        open_trades[ticker] = {"combo": combo, "price": price, "time": str(datetime.now())}
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
        f"🤖 Chart Bot — Final Version ✅\n"
        f"Architecture: TradingView finds → Bot enriches\n"
        f"Hard blocks: Circuit breaker + Earnings only\n"
        f"Data: {'Alpha Vantage' if ALPHA_VANTAGE_KEY else 'yfinance 1min'}\n"
        f"Signals: {len(signal_log)}\n"
        f"Win rate: {f'{win_rate:.0f}%' if win_rate else 'Building...'}\n"
        f"Losses streak: {circuit_breaker['consecutive_losses']}\n"
        f"All systems: 🟢"
    ), 200

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5000))
    app.run(host="0.0.0.0", port=port)
