"""
DERIV PHONE DASHBOARD
Live Deriv digit statistics streamed to your phone.
"""

import asyncio
import json
import math
import os
import threading
from collections import deque, Counter
from datetime import datetime

from flask import Flask, render_template, send_from_directory, jsonify
from flask_socketio import SocketIO
import websockets

SYMBOL = "R_100"
HISTORY_SIZE = 2000
APP_ID = 1089

app = Flask(__name__)
app.config["SECRET_KEY"] = "deriv-phone"
socketio = SocketIO(app, cors_allowed_origins="*", async_mode="eventlet")


class DigitEngine:
    def __init__(self, symbol=SYMBOL, history=HISTORY_SIZE):
        self.symbol = symbol
        self.digits = deque(maxlen=history)
        self.transitions = [[0] * 10 for _ in range(10)]
        self.tick_count = 0
        self.start_time = datetime.now()
        self.last_quote = 0.0
        self.lock = threading.Lock()

    @staticmethod
    def extract_digit(quote):
        return int(f"{quote:.2f}"[-1])

    def record(self, quote):
        digit = self.extract_digit(quote)
        with self.lock:
            if self.digits:
                self.transitions[self.digits[-1]][digit] += 1
            self.digits.append(digit)
            self.tick_count += 1
            self.last_quote = quote
        return digit

    def frequency(self):
        n = len(self.digits)
        if n == 0:
            return {d: 0.0 for d in range(10)}
        c = Counter(self.digits)
        return {d: (c.get(d, 0) / n) * 100 for d in range(10)}

    def z_score(self, digit):
        n = len(self.digits)
        if n < 30:
            return 0.0
        obs = self.digits.count(digit)
        exp = n * 0.10
        std = math.sqrt(n * 0.10 * 0.90)
        return (obs - exp) / std if std else 0.0

    def markov_next(self, min_samples=10):
        if not self.digits:
            return None
        last = self.digits[-1]
        row = self.transitions[last]
        total = sum(row)
        if total < min_samples:
            return None
        return {d: (row[d] / total) * 100 for d in range(10)}

    def current_streak(self):
        if not self.digits:
            return None, 0
        last = self.digits[-1]
        c = 0
        for d in reversed(self.digits):
            if d == last:
                c += 1
            else:
                break
        return last, c

    def chi_square(self):
        n = len(self.digits)
        if n < 100:
            return None
        c = Counter(self.digits)
        exp = n / 10
        return sum(((c.get(d, 0) - exp) ** 2) / exp for d in range(10))

    def even_odd(self):
        n = len(self.digits)
        if n == 0:
            return 50.0, 50.0
        even = sum(1 for d in self.digits if d % 2 == 0)
        return (even / n) * 100, ((n - even) / n) * 100

    def under_over(self):
        n = len(self.digits)
        if n == 0:
            return 50.0, 50.0
        u = sum(1 for d in self.digits if d < 5)
        return (u / n) * 100, ((n - u) / n) * 100

    def snapshot(self):
        with self.lock:
            freq = self.frequency()
            zs = {d: self.z_score(d) for d in range(10)}
            sd, sl = self.current_streak()
            mk = self.markov_next()
            chi = self.chi_square()
            even, odd = self.even_odd()
            under, over = self.under_over()
            recent = list(self.digits)[-30:]
            elapsed = (datetime.now() - self.start_time).total_seconds()
            rate = self.tick_count / elapsed if elapsed > 0 else 0
            return {
                "symbol": self.symbol,
                "tick_count": self.tick_count,
                "last_quote": self.last_quote,
                "rate": round(rate, 2),
                "frequency": {str(k): round(v, 2) for k, v in freq.items()},
                "zscore": {str(k): round(v, 2) for k, v in zs.items()},
                "streak": {"digit": sd, "length": sl},
                "markov": {str(k): round(v, 2) for k, v in mk.items()} if mk else None,
                "markov_last_digit": self.digits[-1] if self.digits else None,
                "chi_square": round(chi, 2) if chi else None,
                "even": round(even, 2),
                "odd": round(odd, 2),
                "under": round(under, 2),
                "over": round(over, 2),
                "recent": recent,
                "timestamp": datetime.now().strftime("%H:%M:%S"),
            }


engine = DigitEngine(SYMBOL)


async def deriv_stream():
    url = f"wss://ws.derivws.com/websockets/v3?app_id={APP_ID}"
    while True:
        try:
            print(f"Connecting to Deriv ({SYMBOL})...")
            async with websockets.connect(url, ping_interval=20) as ws:
                await ws.send(json.dumps({"ticks": SYMBOL, "subscribe": 1}))
                print("Connected.")
                async for msg in ws:
                    data = json.loads(msg)
                    if "tick" in data:
                        quote = float(data["tick"]["quote"])
                        engine.record(quote)
                        socketio.emit("update", engine.snapshot())
                    elif "error" in data:
                        print(f"Deriv error: {data['error']['message']}")
                        break
        except Exception as e:
            print(f"Connection lost: {e}. Reconnecting...")
            await asyncio.sleep(3)


def start_background_loop():
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    loop.run_until_complete(deriv_stream())


@app.route("/")
def index():
    return render_template("index.html", symbol=SYMBOL)


@app.route("/manifest.json")
def manifest():
    return send_from_directory("templetes/static", "manifest.json")


@app.route("/sw.js")
def sw():
    return send_from_directory("templates/static", "sw.js")


@app.route("/health")
def health():
    return jsonify({"status": "ok", "ticks": engine.tick_count})


@socketio.on("connect")
def on_connect():
    print("Browser connected")
    socketio.emit("update", engine.snapshot())


if __name__ == "__main__":
    t = threading.Thread(target=start_background_loop, daemon=True)
    t.start()
    port = int(os.environ.get("PORT", 5000))
    socketio.run(app, host="0.0.0.0", port=port, debug=False)
