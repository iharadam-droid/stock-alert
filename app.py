import os
import sys
import traceback
import requests
import yfinance as yf
import pandas as pd
from datetime import datetime, timedelta

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
        start = end - timedelta(days=550)

        df = yf.download(ticker, start=start, end=end, progress=False)

        if df.empty:
            print(f"[WARN] No data for {ticker}")
            return None

        # 🔥 ここが今回の核心
        if isinstance(df.columns, pd.MultiIndex):
            df.columns = df.columns.get_level_values(0)

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

        # NaNチェック
        if pd.isna(price) or pd.isna(ma50) or pd.isna(ma150) or pd.isna(ma200):
            return False, None

        low_52w = df["Close"].tail(252).min()
        ma200_20d_ago = df["MA200"].iloc[-21]

        if pd.isna(low_52w) or pd.isna(ma200_20d_ago):
            return False, None

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
            "price": float(price),
            "ma50": float(ma50),
            "ma150": float(ma150),
            "ma200": float(ma200),
            "low_52w": float(low_52w)
        }

    except Exception as e:
        print(f"[ERROR] check_trend_template failed: {e}")
        return False, None


def format_result(ticker, data):
    ratio = (data["price"] / data["low_52w"] - 1) * 100

    return (
        f"{ticker}\n"
        f"Price: {data['price']:.2f}\n"
        f"MA50: {data['ma50']:.2f} / MA150: {data['ma150']:.2f} / MA200: {data['ma200']:.2f}\n"
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
