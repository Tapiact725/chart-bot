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
NEWS_API_KEY      = os.environ.get("NEWS_API_KEY", "")      # free at newsapi.org
SHEETS_WEBHOOK    = os.environ.get("SHEETS_WEBHOOK", "")    # optional Google Sheets
DATA_FILE         = "/tmp/bot_data.json"

# ─────────────────────────────────────────
# WATCHLIST — add your tickers here
# ─────────────────────────────────────────
WATCHLIST = ["NVDA", "TSLA", "AMD", "SMH", "ARM", "AAPL", "META", "MSFT", "SPY", "QQQ"]

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
        "signal_log": [],
        "circuit_breaker": {"consecutive_losses": 0},
        "last_update_id": 0,
        "open_trades": {}
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

def send_alert_with_buttons(text, ticker, chat_id=None):
    """Send alert with inline Win/Loss/Ignore buttons"""
    markup = {
        "inline_keyboard": [[
            {"text": "✅ Win",    "callback_data": f"win_{ticker}"},
            {"text": "❌ Loss",   "callback_data": f"loss_{ticker}"},
            {"text": "🚫 Ignore", "callback_data": f"ignore_{ticker}"}
        ]]
    }
    send_telegram(text, chat_id, reply_markup=markup)

def answer_callback(callback_query_id):
    try:
        requests.post(
            f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/answerCallbackQuery",
            json={"callback_query_id": callback_query_id}, timeout=5
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
            updates = r.json().get("result", [])
            for update in updates:
                last_update_id = update["update_id"]

                # Handle button taps
                if "callback_query" in update:
                    cq      = update["callback_query"]
                    data    = cq.get("data", "")
                    chat_id = str(cq["message"]["chat"]["id"])
                    answer_callback(cq["id"])
                    if "_" in data:
                        action, ticker = data.split("_", 1)
                        mark_signal(ticker.upper(), action, chat_id)

                # Handle text commands
                elif "message" in update:
                    message = update["message"]
                    text    = message.get("text", "").strip()
                    chat_id = str(message.get("chat", {}).get("id", ""))
                    if text and chat_id:
                        handle_command(text, chat_id)

            save_data()
        except Exception as e:
            print(f"Polling error: {e}")
            time.sleep(5)
        time.sleep(1)

polling_thread = threading.Thread(target=telegram_polling_loop, daemon=True)
polling_thread.start()

# ─────────────────────────────────────────
# AUTO EXPIRE SIGNALS (48hrs)
# ─────────────────────────────────────────
def auto_expire_loop():
    while True:
        try:
            now = datetime.now()
            for s in signal_log:
                if s["result"] == "pending":
                    try:
                        signal_time = datetime.fromisoformat(str(s["time"]))
                        if (now - signal_time).total_seconds() / 3600 > 48:
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
                    signal_time = datetime.fromisoformat(str(s["time"]))
                    hours_old   = (now - signal_time).total_seconds() / 3600

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

        df = yf.download(ticker, period="1d", interval="5m", progress=False)
        if df.empty:
            return

        current = float(df["Close"].values[-1])
        high    = float(df["High"].max())
        low     = float(df["Low"].min())
        pnl     = ((current - entry) / entry * 100) if entry > 0 else 0

        msg = f"🔍 *AUTO CHECK — {ticker} ({timeframe})*\n"
        msg += f"Entry: ${entry:.2f} → Current: ${current:.2f} ({pnl:+.1f}%)\n"

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
                msg += f"⏳ Still in play | H: ${high:.2f} L: ${low:.2f}"
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
                msg += f"⏳ Still in play | H: ${high:.2f} L: ${low:.2f}"

        send_telegram(msg)
        save_data()
    except Exception as e:
        print(f"Price check error: {e}")

threading.Thread(target=check_outcomes_loop, daemon=True).start()

# ─────────────────────────────────────────
# DAILY MORNING BRIEF (9am ET)
# ─────────────────────────────────────────
def morning_brief_loop():
    while True:
        try:
            et_tz  = pytz.timezone("America/New_York")
            now_et = datetime.now(et_tz)
            # Fire at 9:00 AM ET on weekdays
            if now_et.weekday() < 5 and now_et.hour == 9 and now_et.minute == 0:
                send_morning_brief()
                time.sleep(61)  # prevent double fire
            # Weekly summary Sunday 8pm ET
            if now_et.weekday() == 6 and now_et.hour == 20 and now_et.minute == 0:
                send_weekly_summary()
                time.sleep(61)
        except Exception as e:
            print(f"Brief loop error: {e}")
        time.sleep(30)

def send_morning_brief():
    try:
        et_tz  = pytz.timezone("America/New_York")
        now_et = datetime.now(et_tz)

        brief = f"🌅 *MORNING BRIEF — {now_et.strftime('%A %b %d')}*\n"
        brief += "━━━━━━━━━━━━━━━━━━━━\n\n"

        # Market regime for SPY + QQQ
        spy_regime, spy_rsi, _ = detect_regime("SPY")
        qqq_regime, qqq_rsi, _ = detect_regime("QQQ")
        brief += f"📊 *MARKET REGIME:*\n"
        brief += f"SPY: {spy_regime} | RSI: {spy_rsi:.0f}\n"
        brief += f"QQQ: {qqq_regime} | RSI: {qqq_rsi:.0f}\n\n"

        # Pre-market movers from watchlist
        brief += f"📈 *PRE-MARKET MOVERS:*\n"
        movers = []
        for ticker in WATCHLIST[:8]:
            try:
                df = yf.download(ticker, period="2d", interval="1d", progress=False)
                if len(df) >= 2:
                    prev  = float(df["Close"].values[-2])
                    curr  = float(df["Close"].values[-1])
                    chg   = ((curr - prev) / prev) * 100
                    movers.append((ticker, chg, curr))
            except:
                pass

        movers.sort(key=lambda x: abs(x[1]), reverse=True)
        for ticker, chg, price in movers[:5]:
            emoji = "🟢" if chg > 0 else "🔴"
            brief += f"{emoji} {ticker}: ${price:.2f} ({chg:+.1f}%)\n"

        # Top setups
        brief += f"\n🎯 *TOP SETUPS TO WATCH:*\n"
        setups = scan_watchlist_setups()
        if setups:
            for s in setups[:3]:
                brief += f"• {s}\n"
        else:
            brief += "• No high-conviction setups yet — wait for open\n"

        # Earnings warnings
        brief += f"\n📅 *EARNINGS THIS WEEK:*\n"
        earnings = check_earnings_week()
        if earnings:
            for e in earnings:
                brief += f"⚠️ {e}\n"
        else:
            brief += "No major earnings in your watchlist\n"

        brief += f"\n_Market opens in {max(0, 30 - datetime.now(et_tz).minute)} min_"
        send_telegram(brief)

    except Exception as e:
        print(f"Morning brief error: {e}")
        send_telegram(f"⚠️ Morning brief error: {str(e)}")

def send_weekly_summary():
    try:
        win_rate = get_win_rate()
        wins     = len([s for s in signal_log if s["result"] == "win"])
        losses   = len([s for s in signal_log if s["result"] == "loss"])

        # Best combo this week
        week_ago = datetime.now() - timedelta(days=7)
        week_signals = [
            s for s in signal_log
            if datetime.fromisoformat(str(s["time"])) > week_ago
        ]

        combo_stats = {}
        for s in week_signals:
            c = s["combo"]
            if c not in combo_stats:
                combo_stats[c] = {"win": 0, "loss": 0}
            if s["result"] in ["win", "loss"]:
                combo_stats[c][s["result"]] += 1

        summary  = f"📊 *WEEKLY SUMMARY*\n━━━━━━━━━━━━━━━━━━━━\n"
        summary += f"Signals this week: {len(week_signals)}\n"
        summary += f"Overall win rate: {f'{win_rate:.0f}%' if win_rate else 'Building...'}\n\n"
        summary += f"*COMBO PERFORMANCE:*\n"

        for combo, stats in combo_stats.items():
            total = stats["win"] + stats["loss"]
            wr    = f"{(stats['win']/total*100):.0f}%" if total > 0 else "N/A"
            summary += f"• {combo}: {stats['win']}W/{stats['loss']}L ({wr})\n"

        summary += f"\n*BEST PERFORMING TICKER:*\n"
        ticker_stats = {}
        for s in week_signals:
            t = s["ticker"]
            if t not in ticker_stats:
                ticker_stats[t] = {"win": 0, "loss": 0}
            if s["result"] in ["win", "loss"]:
                ticker_stats[t][s["result"]] += 1

        best = sorted(
            ticker_stats.items(),
            key=lambda x: x[1]["win"] - x[1]["loss"],
            reverse=True
        )
        for ticker, stats in best[:3]:
            summary += f"• {ticker}: {stats['win']}W/{stats['loss']}L\n"

        send_telegram(summary)
    except Exception as e:
        print(f"Weekly summary error: {e}")

threading.Thread(target=morning_brief_loop, daemon=True).start()

# ─────────────────────────────────────────
# NEWS SENTIMENT CHECK
# ─────────────────────────────────────────
def check_news_sentiment(ticker):
    if not NEWS_API_KEY:
        return None, []

    try:
        r = requests.get(
            "https://newsapi.org/v2/everything",
            params={
                "q":        ticker,
                "sortBy":   "publishedAt",
                "pageSize": 5,
                "apiKey":   NEWS_API_KEY,
                "language": "en"
            },
            timeout=5
        )
        articles = r.json().get("articles", [])
        if not articles:
            return "neutral", []

        negative_words = [
            "lawsuit", "investigation", "fraud", "miss", "loss", "decline",
            "crash", "ban", "recall", "downgrade", "warning", "risk",
            "bankruptcy", "sec", "fine", "penalty", "hack", "breach"
        ]
        positive_words = [
            "beat", "upgrade", "record", "growth", "profit", "deal",
            "partnership", "launch", "breakthrough", "strong", "surge"
        ]

        neg_count = 0
        pos_count = 0
        headlines = []

        for article in articles[:3]:
            title = article.get("title", "").lower()
            headlines.append(article.get("title", ""))
            neg_count += sum(1 for w in negative_words if w in title)
            pos_count += sum(1 for w in positive_words if w in title)

        if neg_count > pos_count + 1:
            return "negative", headlines
        elif pos_count > neg_count:
            return "positive", headlines
        else:
            return "neutral", headlines

    except Exception as e:
        print(f"News error: {e}")
        return None, []

# ─────────────────────────────────────────
# EARNINGS CHECK
# ─────────────────────────────────────────
def check_earnings(ticker):
    try:
        stock = yf.Ticker(ticker)
        cal   = stock.calendar
        if cal is None or cal.empty:
            return None

        if hasattr(cal, 'columns'):
            if 'Earnings Date' in cal.columns:
                dates = cal['Earnings Date']
                if len(dates) > 0:
                    earnings_date = dates.iloc[0]
                    if hasattr(earnings_date, 'date'):
                        days_until = (earnings_date.date() - datetime.now().date()).days
                        if 0 <= days_until <= 7:
                            return days_until
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

# ─────────────────────────────────────────
# OPTIONS FLOW CHECK
# Uses unusual volume as proxy
# ─────────────────────────────────────────
def check_options_flow(ticker):
    try:
        stock = yf.Ticker(ticker)
        opts  = stock.options
        if not opts:
            return None

        nearest_exp = opts[0]
        chain       = stock.option_chain(nearest_exp)
        calls       = chain.calls
        puts        = chain.puts

        if calls.empty or puts.empty:
            return None

        call_vol = calls["volume"].sum()
        put_vol  = puts["volume"].sum()

        if call_vol + put_vol == 0:
            return None

        put_call_ratio = put_vol / (call_vol + 1)

        if put_call_ratio > 1.5:
            return f"🐋 High PUT activity on {ticker} (P/C ratio: {put_call_ratio:.1f}) — smart money hedging?"
        elif put_call_ratio < 0.5:
            return f"🐋 High CALL activity on {ticker} (P/C ratio: {put_call_ratio:.1f}) — bullish flow"
        return None
    except:
        return None

# ─────────────────────────────────────────
# SECTOR HEALTH CHECK
# ─────────────────────────────────────────
SECTOR_MAP = {
    "NVDA": "SMH", "AMD": "SMH", "ARM": "SMH", "INTC": "SMH",
    "TSLA": "QQQ", "AAPL": "QQQ", "META": "QQQ", "MSFT": "QQQ",
    "SPY": "SPY",  "QQQ": "QQQ"
}

def check_sector_health(ticker):
    try:
        sector_etf = SECTOR_MAP.get(ticker, "SPY")
        if sector_etf == ticker:
            return None

        df = yf.download(sector_etf, period="5d", interval="1d", progress=False)
        if df.empty or len(df) < 2:
            return None

        today_chg = ((float(df["Close"].values[-1]) - float(df["Close"].values[-2]))
                     / float(df["Close"].values[-2]) * 100)

        if today_chg < -1.5:
            return f"⚠️ Sector ({sector_etf}) down {today_chg:.1f}% today — weakens bullish signals"
        elif today_chg > 1.5:
            return f"✅ Sector ({sector_etf}) up {today_chg:.1f}% today — strengthens bullish signals"
        return None
    except:
        return None

# ─────────────────────────────────────────
# CORRELATION WARNING
# ─────────────────────────────────────────
CORRELATED_GROUPS = [
    ["NVDA", "AMD", "ARM", "SMH", "INTC"],
    ["AAPL", "MSFT", "META", "QQQ"],
    ["TSLA"]
]

def check_correlation_risk(ticker):
    warnings = []
    for group in CORRELATED_GROUPS:
        if ticker in group:
            for open_ticker in open_trades:
                if open_ticker in group and open_ticker != ticker:
                    warnings.append(
                        f"📊 You have open {open_ticker} trade — {ticker} & {open_ticker} "
                        f"are highly correlated. Double sector risk."
                    )
    return warnings

# ─────────────────────────────────────────
# MULTI-TIMEFRAME CONFIRMATION
# ─────────────────────────────────────────
def check_higher_timeframe(ticker, current_interval):
    try:
        is_intraday = any(x in current_interval for x in ["1", "5", "15", "30", "60"])
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
            return "✅ Daily chart agrees — uptrend confirmed (HIGH conviction)"
        elif last < ema10 < ema20:
            return "⚠️ Daily chart disagrees — daily downtrend. Reduce size."
        else:
            return "↔️ Daily chart mixed — medium conviction"
    except:
        return None

# ─────────────────────────────────────────
# RISK/REWARD CALCULATOR
# ─────────────────────────────────────────
def calculate_rr(entry, target1, stop):
    try:
        if not all([entry, target1, stop]):
            return None
        reward = abs(target1 - entry)
        risk   = abs(entry - stop)
        if risk == 0:
            return None
        rr = reward / risk
        emoji = "✅" if rr >= 2 else "⚠️" if rr >= 1.5 else "❌"
        return f"{emoji} R:R = {rr:.1f}:1 | Risk: ${risk:.2f} | Reward: ${reward:.2f}"
    except:
        return None

# ─────────────────────────────────────────
# WATCHLIST SCANNER (for morning brief)
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

            ema10    = sum(closes[-10:]) / 10
            ema20    = sum(closes[-20:]) / 20
            last     = float(closes[-1])
            vol_avg  = sum(vols[-20:]) / 20
            vol_now  = float(vols[-1])

            # RSI
            gains, losses = [], []
            for i in range(-14, 0):
                diff = float(closes[i]) - float(closes[i-1])
                if diff > 0: gains.append(diff); losses.append(0)
                else: gains.append(0); losses.append(abs(diff))
            avg_gain = sum(gains) / 14 if gains else 0.001
            avg_loss = sum(losses) / 14 if losses else 0.001
            rsi = 100 - (100 / (1 + avg_gain / avg_loss))

            # Dip setup
            if rsi < 35 and last < ema20 and vol_now > vol_avg:
                setups.append(f"🎣 {ticker} — Dip setup (RSI: {rsi:.0f}, oversold + volume)")

            # Breakout setup
            if last > float(highs[-2]) and vol_now > vol_avg * 1.3:
                setups.append(f"🚀 {ticker} — Breakout above recent high with volume")

            # Trend pullback
            if float(ema10) > float(ema20) and last < float(ema10) * 1.005 and rsi < 50:
                setups.append(f"📈 {ticker} — Trend pullback to EMA10 in uptrend")

        except:
            pass
    return setups[:5]

# ─────────────────────────────────────────
# GOOGLE SHEETS LOGGER
# ─────────────────────────────────────────
def log_to_sheets(signal_data):
    if not SHEETS_WEBHOOK:
        return
    try:
        requests.post(SHEETS_WEBHOOK, json=signal_data, timeout=5)
    except Exception as e:
        print(f"Sheets error: {e}")

# ─────────────────────────────────────────
# WIN RATE
# ─────────────────────────────────────────
def get_win_rate():
    resolved = [s for s in signal_log if s["result"] in ["win", "loss"]]
    last10   = resolved[-10:]
    if len(last10) < 3:
        return None
    return (len([s for s in last10 if s["result"] == "win"]) / len(last10)) * 100

# ─────────────────────────────────────────
# REGIME DETECTION
# ─────────────────────────────────────────
def detect_regime(ticker, interval_str=""):
    try:
        is_swing = any(x in str(interval_str).upper() for x in ["1D","1W","D","W","DAY","WEEK"])
        period   = "60d" if is_swing else "30d"
        df       = yf.download(ticker, period=period, interval="1d", progress=False)

        if df.empty or len(df) < 10:
            return "unknown", 50.0, False

        closes  = df["Close"].values.flatten()
        highs   = df["High"].values.flatten()
        lows    = df["Low"].values.flatten()
        vols    = df["Volume"].values.flatten()
        n       = len(closes)

        ema10   = sum(closes[-10:]) / 10
        ema20   = sum(closes[-min(20,n):]) / min(20,n)
        ema50   = sum(closes[-min(50,n):]) / min(50,n)
        atr     = sum([highs[i] - lows[i] for i in range(-10, 0)]) / 10
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

        et_tz       = pytz.timezone("America/New_York")
        now_et      = datetime.now(et_tz)
        is_extended = (now_et.time() < dtime(9, 30) or now_et.time() >= dtime(16, 0))

        score = 0
        if float(ema10) > float(ema20): score += 20
        if float(ema20) > float(ema50): score += 20
        if float(closes[-1]) > float(ema10): score += 15
        if atr_pct > 1.5: score += 20
        if 40 < rsi < 65: score += 15
        if vol_recent > vol_older: score += 10

        regime = "TRENDING 📈" if score >= 65 else "MIXED ↔️" if score >= 40 else "CHOPPY ⚠️"
        return regime, rsi, is_extended

    except Exception as e:
        print(f"Regime error: {e}")
        return "unknown", 50.0, False

# ─────────────────────────────────────────
# ADAPTIVE FILTER
# ─────────────────────────────────────────
def should_fire(ticker, combo, regime, rsi, is_extended):
    hard_skip = []
    soft_warn = []

    if circuit_breaker["consecutive_losses"] >= 3:
        hard_skip.append("⛔ Circuit breaker — 3 consecutive losses")
    if regime == "CHOPPY ⚠️" and "Dip" in combo:
        hard_skip.append("⚠️ Choppy market — Dip & Rip unreliable")
    win_rate = get_win_rate()
    if win_rate and win_rate < 35:
        hard_skip.append(f"📉 Win rate {win_rate:.0f}% — filters tightened")
    if rsi > 75 and "Momentum" in combo:
        hard_skip.append(f"🔴 RSI {rsi:.0f} — overbought for momentum")
    if is_extended:
        soft_warn.append("🌙 Extended hours — lower liquidity, wider spreads")

    return (False, hard_skip + soft_warn) if hard_skip else (True, soft_warn)

# ─────────────────────────────────────────
# CANDLE DATA
# ─────────────────────────────────────────
def get_candle_data(ticker, interval_str=""):
    try:
        is_swing = any(x in str(interval_str).upper() for x in ["1D","1W","D","W","DAY","WEEK"])
        df = yf.download(
            ticker,
            period="30d" if is_swing else "5d",
            interval="1d" if is_swing else "15m",
            progress=False
        )
        if df.empty:
            return "No data"
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
def analyze_with_claude(alert_msg, candle_data, ticker, regime, rsi, win_rate, is_extended, context=""):
    client       = anthropic.Anthropic(api_key=ANTHROPIC_API_KEY)
    win_rate_str = f"{win_rate:.0f}%" if win_rate else "Building..."

    prompt = f"""You are an expert trading analyst. TradingView fired this confluence alert:
"{alert_msg}"

MARKET CONTEXT:
- Regime: {regime}
- RSI(14): {rsi:.1f}
- Win rate: {win_rate_str}
- Extended hours: {is_extended}
{context}

Last 10 candles for {ticker}:
{candle_data}

Respond in this EXACT format:

📊 *SIGNAL* — What triggered and why
🌍 *REGIME* — How conditions affect this setup
🎯 *BIAS* — Bullish/Bearish/Neutral | Conviction: Low/Medium/High
💰 *ENTRY* — $XX.XX - $XX.XX
🎯 *TARGET 1* — $XX.XX
🎯 *TARGET 2* — $XX.XX
🛑 *STOP LOSS* — $XX.XX
⏱ *HOLD TIME* — X hours/days
📐 *POSITION SIZE* — Risk X% of account
⚠️ *RISK* — Key risks

Always give exact dollar amounts for entry, targets, stop."""

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

    # Log to Google Sheets
    log_to_sheets({
        "ticker": ticker, "combo": combo,
        "price":  price,  "time":  entry["time"],
        "target1": entry.get("target1", 0),
        "target2": entry.get("target2", 0),
        "stop":    entry.get("stop", 0)
    })
    save_data()

# ─────────────────────────────────────────
# COMMAND HANDLER
# ─────────────────────────────────────────
def handle_command(text, chat_id):
    parts  = text.strip().split()
    cmd    = parts[0].lower()
    ticker = parts[1].upper() if len(parts) > 1 else None

    if cmd in ["/win", "/loss", "/ignore"] and ticker:
        mark_signal(ticker, cmd.replace("/",""), chat_id)
    elif cmd == "/report":
        send_report(chat_id)
    elif cmd == "/status":
        send_status(chat_id)
    elif cmd in ["/info", "/start", "/help"]:
        send_info(chat_id)
    elif cmd == "/scan":
        send_telegram("🔍 Scanning watchlist...", chat_id)
        setups = scan_watchlist_setups()
        if setups:
            send_telegram("📊 *CURRENT SETUPS:*\n" + "\n".join(f"• {s}" for s in setups), chat_id)
        else:
            send_telegram("No high-conviction setups right now.", chat_id)
    elif cmd == "/brief":
        send_morning_brief()
    elif cmd == "/weekly":
        send_weekly_summary()
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
                f"Consecutive losses: {circuit_breaker['consecutive_losses']}\n"
                f"Win rate: {f'{win_rate:.0f}%' if win_rate else 'Building...'}",
                chat_id
            )
            save_data()
            return

    send_telegram(f"⚠️ No pending signal for *{ticker}*. Check /report.", chat_id)

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
    report += f"Total: {total} | ✅ {wins} | ❌ {losses} | 🚫 {ignored} | ⏳ {pending} | ⌛ {expired}\n"
    report += f"🎯 Win Rate: {f'{win_rate:.0f}%' if win_rate else 'Need 5+ trades'}\n\n"
    report += f"🔧 *STATUS:* {'🔴 Circuit breaker ACTIVE' if circuit_breaker['consecutive_losses'] >= 3 else '🟢 Normal'}\n"
    report += f"{'📉 Filters TIGHTENED' if win_rate and win_rate < 35 else '📈 Filters NORMAL'}\n\n"
    report += f"*COMBO BREAKDOWN:*\n"

    for combo, stats in combo_stats.items():
        total_c = stats["win"] + stats["loss"]
        wr = f"{(stats['win']/total_c*100):.0f}%" if total_c > 0 else "N/A"
        report += f"• {combo}: {stats['win']}W/{stats['loss']}L ({wr})\n"

    report += f"\n*LAST 5:*\n"
    for s in signal_log[-5:]:
        e = "✅" if s["result"]=="win" else "❌" if s["result"]=="loss" else "🚫" if s["result"]=="ignore" else "⌛" if s["result"]=="expired" else "⏳"
        report += f"{e} {s['ticker']} | {s['combo']} | ${s['price']} | {str(s['time'])[11:16]}\n"

    send_telegram(report, chat_id)

def send_status(chat_id):
    win_rate = get_win_rate()
    send_telegram(
        f"🤖 *BOT STATUS*\n"
        f"Signals: {len(signal_log)} | Pending: {len([s for s in signal_log if s['result']=='pending'])}\n"
        f"Win rate: {f'{win_rate:.0f}%' if win_rate else 'Building...'}\n"
        f"Consecutive losses: {circuit_breaker['consecutive_losses']}\n"
        f"{'🔴 Circuit breaker ACTIVE' if circuit_breaker['consecutive_losses'] >= 3 else '🟢 Running normally'}\n"
        f"Polling: 🟢 | Auto-check: 🟢 | Morning brief: 🟢",
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
        "`/report` — full stats, combo breakdown, last 5\n"
        "`/status` — quick system health check\n\n"
        "🔍 *MARKET:*\n"
        "`/scan` — scan watchlist for setups right now\n"
        "`/brief` — get today's morning brief on demand\n"
        "`/weekly` — get weekly performance summary\n\n"
        "ℹ️ *HELP:*\n"
        "`/info` — show this list\n\n"
        "━━━━━━━━━━━━━━━━━━━━\n"
        "💡 *Auto features:*\n"
        "• ✅❌ buttons on every alert — tap to log\n"
        "• Auto price check at 4hr & 24hr\n"
        "• Pending signals expire after 48hrs\n"
        "• Morning brief at 9am ET weekdays\n"
        "• Weekly summary every Sunday 8pm ET\n"
        "• News, earnings, sector & options checked on every alert",
        chat_id
    )

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

        # All checks in parallel-ish
        regime, rsi, is_extended     = detect_regime(ticker, interval_str)
        win_rate                      = get_win_rate()
        fire, warnings                = should_fire(ticker, combo, regime, rsi, is_extended)
        news_sentiment, news_headlines = check_news_sentiment(ticker)
        sector_note                   = check_sector_health(ticker)
        options_note                  = check_options_flow(ticker)
        earnings_days                 = check_earnings(ticker)
        mtf_note                      = check_higher_timeframe(ticker, interval_str)
        correlation_warns             = check_correlation_risk(ticker)

        # Hard block
        if not fire:
            hard = [w for w in warnings if not w.startswith("🌙")]
            warn = f"⚠️ *SIGNAL FILTERED — {ticker}*\n"
            warn += f"Combo: {combo} | Regime: {regime} | RSI: {rsi:.0f}\n\n"
            warn += "🚫 *Reasons:*\n" + "\n".join(f"• {r}" for r in hard)
            send_telegram(warn)
            return jsonify({"status": "filtered"}), 200

        # Build context for Claude
        context_lines = []
        if news_sentiment == "negative" and news_headlines:
            context_lines.append(f"⚠️ NEGATIVE NEWS: {news_headlines[0][:80]}")
        elif news_sentiment == "positive" and news_headlines:
            context_lines.append(f"✅ POSITIVE NEWS: {news_headlines[0][:80]}")
        if sector_note:
            context_lines.append(sector_note)
        if options_note:
            context_lines.append(options_note)
        if mtf_note:
            context_lines.append(f"MTF: {mtf_note}")
        if earnings_days is not None:
            context_lines.append(f"⚠️ EARNINGS IN {earnings_days} DAYS")

        context = "\n".join(context_lines)

        # Get candles and analyze
        candle_data = get_candle_data(ticker, interval_str)
        analysis    = analyze_with_claude(
            alert_text, candle_data, ticker,
            regime, rsi, win_rate, is_extended, context
        )

        # Extract price levels
        levels = extract_levels(analysis, price)

        # Calculate R:R
        rr_note = calculate_rr(price, levels.get("target1", 0), levels.get("stop", 0))

        # Build message
        win_rate_str = f"{win_rate:.0f}%" if win_rate else "Building..."
        combo_emoji  = "🎣" if "Dip" in combo else "🚀" if "Momentum" in combo else "🚨"

        message = f"{combo_emoji} *{combo.upper()} — {ticker}*\n"
        message += f"🌍 Regime: {regime} | RSI: {rsi:.0f} | Win Rate: {win_rate_str}\n"

        # Add all context warnings
        if warnings:
            message += "\n".join(f"• {w}" for w in warnings) + "\n"
        if earnings_days is not None:
            message += f"📅 *EARNINGS IN {earnings_days} DAYS — elevated risk*\n"
        if news_sentiment == "negative":
            message += f"📰 *NEGATIVE NEWS DETECTED — trade carefully*\n"
        if sector_note:
            message += f"{sector_note}\n"
        if options_note:
            message += f"{options_note}\n"
        if mtf_note:
            message += f"📊 MTF: {mtf_note}\n"
        for cw in correlation_warns:
            message += f"⚠️ {cw}\n"
        if rr_note:
            message += f"\n{rr_note}\n"

        message += f"\n{analysis}\n\n"
        message += f"_Signal #{len(signal_log)+1} | {datetime.now().strftime('%H:%M')} ET_"

        # Send with inline buttons
        send_alert_with_buttons(message, ticker)

        # Track as open trade
        open_trades[ticker] = {
            "combo": combo, "price": price,
            "time":  str(datetime.now())
        }

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
        f"🤖 Chart Bot Level 2 Full ✅\n"
        f"Signals: {len(signal_log)}\n"
        f"Win rate: {f'{win_rate:.0f}%' if win_rate else 'Building...'}\n"
        f"Losses streak: {circuit_breaker['consecutive_losses']}\n"
        f"Open trades: {list(open_trades.keys())}\n"
        f"All systems: 🟢"
    ), 200

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5000))
    app.run(host="0.0.0.0", port=port)
