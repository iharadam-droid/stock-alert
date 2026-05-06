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
    df["VolAvg50"] = df["Volume"].rolling(50).mean()
    df["High50"] = df["High"].rolling(50).max()
    return df


def check_trend_template(df):
    try:
        latest = df.iloc[-1]

        price = latest["Close"]
        ma50 = latest["MA50"]
        ma150 = latest["MA150"]
        ma200 = latest["MA200"]

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


def check_volume(df):
    try:
        latest = df.iloc[-1]

        volume = latest["Volume"]
        vol_avg = latest["VolAvg50"]

        if pd.isna(volume) or pd.isna(vol_avg) or vol_avg == 0:
            return False, None

        ratio = volume / vol_avg

        return ratio >= 1.1, float(ratio)

    except Exception as e:
        print(f"[ERROR] check_volume failed: {e}")
        return False, None


def check_breakout(df):
    try:
        latest = df.iloc[-1]

        price = latest["Close"]
        high50 = latest["High50"]

        if pd.isna(price) or pd.isna(high50):
            return False, None

        return price >= high50, float(high50)

    except Exception as e:
        print(f"[ERROR] check_breakout failed: {e}")
        return False, None


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
    trend_matches = []
    volume_matches = []
    final_matches = []

    for ticker in TICKERS:
        print(f"[INFO] Processing {ticker}")

        df = fetch_data(ticker)
        if df is None:
            continue

        df = calculate_indicators(df)

        ok, data = check_trend_template(df)
        if ok:
            trend_matches.append((ticker, data))

            vol_ok, vol_ratio = check_volume(df)
            if vol_ok:
                volume_matches.append((ticker, data, vol_ratio))

                brk_ok, high50 = check_breakout(df)
                if brk_ok:
                    final_matches.append((ticker, data, vol_ratio, high50))

    # 🔥 Slackメッセージ改善
    today_str = datetime.utcnow().strftime("%Y-%m-%d")
    header = f"📊 Trend Template + Vol×1.1 + 50D High ｜ {today_str}\n\n"

    message = header + "```"

    # 最終通過銘柄（1行フォーマット）
    if final_matches:
        for ticker, data, vol_ratio, high50 in final_matches:
            message += (
                f"{ticker}｜${data['price']:.2f}｜Vol ×{vol_ratio:.1f}｜50D High ${high50:.2f}\n"
            )
    else:
        message += "該当なし\n"

    message += "```\n\n"

    # トレンド通過
    message += "*Trend Template Passed*\n"
    if trend_matches:
        for ticker, data in trend_matches:
            message += f"{ticker} ${data['price']:.2f}\n"
    else:
        message += "該当なし\n"

    message += "\n*Volume Filter Passed*\n"
    if volume_matches:
        for ticker, data, vol_ratio in volume_matches:
            message += f"{ticker} ${data['price']:.2f} (Vol ×{vol_ratio:.1f})\n"
    else:
        message += "該当なし\n"

    print(message)
    send_to_slack(message)


if __name__ == "__main__":
    try:
        main()
    except Exception:
        print("[FATAL ERROR]")
        traceback.print_exc()
        sys.exit(1)
