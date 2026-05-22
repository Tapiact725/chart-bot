import os
import requests
from flask import Flask, request, jsonify
import anthropic
import yfinance as yf

app = Flask(__name__)

TELEGRAM_TOKEN   = os.environ.get("TELEGRAM_TOKEN", "8446395290:AAFuBqv_epL4FhGLBHy8LjZPnOXg9oSQz0E")
ANTHROPIC_API_KEY = os.environ.get("ANTHROPIC_API_KEY", "")
TELEGRAM_CHAT_ID  = os.environ.get("TELEGRAM_CHAT_ID", "")  # your personal chat ID

def send_telegram(text):
    requests.post(
        f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage",
        json={"chat_id": TELEGRAM_CHAT_ID, "text": text, "parse_mode": "Markdown"}
    )

def get_candle_data(ticker, period="5d", interval="15m"):
    try:
        df = yf.download(ticker, period=period, interval=interval, progress=False)
        if df.empty:
            return "No data available"
        last10 = df.tail(10)
        lines = []
        for ts, row in last10.iterrows():
            lines.append(
                f"{ts.strftime('%m/%d %H:%M')} | O:{float(row['Open']):.2f} H:{float(row['High']):.2f} "
                f"L:{float(row['Low']):.2f} C:{float(row['Close']):.2f} V:{int(float(row['Volume']))}"
            )
        return "\n".join(lines)
    except Exception as e:
        return f"Data error: {e}"

def analyze_with_claude(alert_message, candle_data, ticker):
    client = anthropic.Anthropic(api_key=ANTHROPIC_API_KEY)
    prompt = f"""TradingView just fired this alert:
"{alert_message}"

Here are the last 10 candles for {ticker}:
{candle_data}

Give a concise trading analysis:
📊 SIGNAL: What triggered and why it matters
📈 TREND: Current momentum
🎯 BIAS: Bullish / Bearish / Neutral
💰 WATCH: Key price levels to watch
⚠️ RISK: What invalidates this setup

Keep it short and actionable. This is for a live trader."""

    message = client.messages.create(
        model="claude-sonnet-4-20250514",
        max_tokens=400,
        messages=[{"role": "user", "content": prompt}]
    )
    return message.content[0].text

@app.route("/webhook", methods=["POST"])
def webhook():
    try:
        # TradingView sends plain text
        alert_text = request.get_data(as_text=True)
        if not alert_text:
            data = request.get_json(silent=True) or {}
            alert_text = data.get("message", "Alert received")

        print(f"Alert received: {alert_text}")

        # Extract ticker from alert message
        ticker = "SPY"  # default
        parts = alert_text.split("—")
        if len(parts) > 1:
            detail = parts[1].strip()
            ticker = detail.split("|")[0].strip()

        # Get candle data
        candle_data = get_candle_data(ticker)

        # Analyze with Claude
        analysis = analyze_with_claude(alert_text, candle_data, ticker)

        # Send to Telegram
        message = f"🚨 *ALERT: {ticker}*\n\n{alert_text}\n\n{analysis}"
        send_telegram(message)

        return jsonify({"status": "ok"}), 200

    except Exception as e:
        print(f"Error: {e}")
        send_telegram(f"❌ Webhook error: {str(e)}")
        return jsonify({"error": str(e)}), 500

@app.route("/", methods=["GET"])
def health():
    return "Chart Bot is running ✅", 200

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5000))
    app.run(host="0.0.0.0", port=port)
