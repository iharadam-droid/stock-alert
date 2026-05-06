import os
import sys
import traceback
import requests
import yfinance as yf
import pandas as pd
from datetime import datetime, timedelta

# 固定ティッカー（35銘柄）
TICKERS = [
    "AAPL", "MSFT", "NVDA", "AMZN", "GOOGL", "META", "TSLA",
    "AVGO", "LLY", "V", "MA", "COST", "NFLX", "ADBE",
    "CRM", "AMD", "INTC", "QCOM", "TXN",
    "UNH", "JNJ", "PFE", "MRK",
    "HD", "LOW", "NKE", "SBUX",
    "XOM", "CVX",
    "BA", "CAT",
    "GS", "JPM",
    "PLTR", "SNOW"
]

def fetch_data(ticker):
    try:
        end = datetime.today()
        start = end - timedelta(days=550)  # 約1年半

        df = yf.download(ticker, start=start, end=end, progress=False)

        if df.empty:
            print(f"[WARN] No data for {ticker}")
            return None

        return df

    except Exception as e:
        print(f"[ERROR] fetch_data failed for {ticker}: {e}")
        return None


def calculate_indicators(df):
    df["MA50"] = df["Close"].rolling(50).mean()
    df["MA150"] = df["Close"].rolling(150).mean()
    df["MA200"] = df["Close"].rolling(200).mean()
    return df


def check_trend_template(df):
    try:
        latest = df.iloc[-1]

        price = latest["Close"]
        ma50 = latest["MA50"]
        ma150 = latest["MA150"]
        ma200 = latest["MA200"]

        # 52週安値
        low_52w = df["Close"].tail(252).min()

        # 条件4：200MAが20日上昇
        ma200_20d_ago = df["MA200"].iloc[-21]

        conds = [
            price > ma150,
            price > ma200,
            ma150 > ma200,
            ma200 > ma200_20d_ago,
            ma50 > ma150,
            ma50 > ma200,
            price > ma50,
            price >= low_52w * 1.25
        ]

        return all(conds), {
            "price": price,
            "ma50": ma50,
            "ma150": ma150,
            "ma200": ma200,
            "low_52w": low_52w
        }

    except Exception as e:
        print(f"[ERROR] check_trend_template failed: {e}")
        return False, None


def format_result(ticker, data):
    price = data["price"]
    ma50 = data["ma50"]
    ma150 = data["ma150"]
    ma200 = data["ma200"]
    low_52w = data["low_52w"]

    ratio = (price / low_52w - 1) * 100

    return (
        f"{ticker}\n"
        f"Price: {price:.2f}\n"
        f"MA50: {ma50:.2f} / MA150: {ma150:.2f} / MA200: {ma200:.2f}\n"
        f"52W Low Diff: +{ratio:.1f}%\n"
    )


def send_to_slack(message):
    webhook_url = os.getenv("SLACK_WEBHOOK_URL")

    if not webhook_url:
        print("[ERROR] SLACK_WEBHOOK_URL is not set")
        return

    try:
        response = requests.post(webhook_url, json={"text": message})

        if response.status_code != 200:
            print(f"[ERROR] Slack send failed: {response.status_code}, {response.text}")

    except Exception as e:
        print(f"[ERROR] send_to_slack failed: {e}")


def main():
    results = []

    for ticker in TICKERS:
        print(f"[INFO] Processing {ticker}")

        df = fetch_data(ticker)
        if df is None:
            continue

        df = calculate_indicators(df)

        ok, data = check_trend_template(df)
        if ok:
            results.append(format_result(ticker, data))

    if results:
        message = "*Minervini Trend Template Matches*\n\n" + "\n".join(results)
    else:
        message = "No stocks matched the Minervini Trend Template today."

    print(message)
    send_to_slack(message)


if __name__ == "__main__":
    try:
        main()
    except Exception:
        print("[FATAL ERROR]")
        traceback.print_exc()
        sys.exit(1)
      
