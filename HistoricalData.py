import os

from breeze_connect import BreezeConnect
from datetime import datetime, timedelta
import pandas as pd
import time
import localConfiguration


# === Setup BreezeConnect ===
breeze = BreezeConnect(api_key=localConfiguration.API_KEY)
breeze.generate_session(api_secret=localConfiguration.API_SECRET, session_token=localConfiguration.SESSION_TOKEN)

# === Load Holiday Calendar ===
holiday_df = pd.read_csv("data/config/HOLIDAY.CSV")
holiday_df["TradingDate"] = pd.to_datetime(holiday_df["TradingDate"], format="%d-%b-%y")
holiday_dates = set(holiday_df["TradingDate"])

# === Load Expiry Data ===
expiry_df = pd.read_csv("data/config/EXPIRY_DATA.csv")
expiry_df = expiry_df[expiry_df["expiry_type"] == 'MONTHLY']
expiry_df["expiry_date"] = pd.to_datetime(expiry_df["expiry_date"])

# === Config ===
start_day = datetime(2025, 5, 1)  # adjust this as needed
api_call_count = 0


def is_trading_day(date):
    return date.weekday() < 5 and date not in holiday_dates

def get_nifty_ohl(date):
    global api_call_count
    from_date = date - timedelta(days=1)
    from_date = from_date.strftime("%Y-%m-%dT09:15:00.000Z")
    to_date = date.strftime("%Y-%m-%dT15:30:00.000Z")
    try:
        data = breeze.get_historical_data_v2(
            interval="1day",
            from_date=from_date,
            to_date=to_date,
            stock_code="NIFTY",
            exchange_code="NSE",
            product_type="cash"
        )
        api_call_count += 1
        if "Success" in data and isinstance(data["Success"], list) and len(data["Success"]) > 0:
            record = data["Success"][0]
            return float(record["open"]), float(record["high"]), float(record["low"])
        else:
            print(f"⚠️ No data returned for {date.date()}")
            return None, None, None

    except Exception as e:
        print(f"⚠️ Error fetching NIFTY OHLC for {date.date()}: {e}")
        return None, None, None

def get_nifty_open_price(date):
    global api_call_count
    from_date = date - timedelta(days=1)
    from_date = from_date.strftime("%Y-%m-%dT09:15:00.000Z")
    to_date = date.strftime("%Y-%m-%dT15:30:00.000Z")

    try:
        data = breeze.get_historical_data_v2(
            interval="1day",
            from_date=from_date,
            to_date=to_date,
            stock_code="NIFTY",
            exchange_code="NSE",
            product_type="cash"
        )
        api_call_count += 1
        if "Success" in data and isinstance(data["Success"], list) and len(data["Success"]) > 0:
            return float(data["Success"][0]["open"])
        else:
            print(f"⚠️ No data returned for {symbol}: {from_date} to {to_date}")
            return None
    except Exception as e:
        print(f"⚠️ Error fetching NIFTY open price for {symbol}: {from_date} to {to_date}: {e}")
        return None


def get_option_data(date, expiry_date, strike_price, right):
    global api_call_count
    from_date = date.strftime("%Y-%m-%dT09:15:00.000Z")
    to_date = date.strftime("%Y-%m-%dT15:30:00.000Z")
    try:
        data = breeze.get_historical_data_v2(
            interval="1minute",
            from_date=from_date,
            to_date=to_date,
            stock_code="NIFTY",
            exchange_code="NFO",
            product_type="options",
            strike_price=str(strike_price),
            right=right,
            expiry_date=expiry_date.strftime("%Y-%m-%dT07:00:00.000Z")
        )
        api_call_count += 1
        return data.get("Success", [])
    except Exception as e:
        print(f"⚠️ Error fetching data for {strike_price}{right} on {date.date()}: {e}")
        return []


# === Main Loop ===
for _, row in expiry_df.iterrows():
    expiry_date = row["expiry_date"]
    symbol = row["symbol"]

    current_day = start_day
    while current_day <= expiry_date:
        if not is_trading_day(current_day):
            current_day += timedelta(days=1)
            continue

        #nifty_open = get_nifty_open_price(current_day)
        nifty_open,nifty_high, nifty_low = get_nifty_ohl(current_day)
        if not nifty_open or not nifty_high or not nifty_low:
            print(f"Skipping {current_day.date()} due to missing price data.")
            current_day += timedelta(days=1)
            continue

        strike_low = round((nifty_low - 300) / 50) * 50
        strike_high = round((nifty_high + 300) / 50) * 50
        strike_range = range(strike_low, strike_high + 50, 50)
        # ✅ Prepare date-wise directory
        folder_name = current_day.strftime("%d-%b-%Y").lstrip("0")  # e.g., 2-May-2025
        day_dir = os.path.join("data", "options", folder_name)
        os.makedirs(day_dir, exist_ok=True)
        # Fetch & Save Options Data
        for strike in strike_range:
            for right in ["call", "put"]:
                option_data = get_option_data(current_day, expiry_date, strike, right)
                if option_data:
                    df = pd.DataFrame(option_data)
                    filename = f"NIFTY_{strike}_{right.upper()}.csv"
                    filepath = os.path.join(day_dir, filename)
                    df.to_csv(filepath, index=False)
                    print(f"✅ Saved: {filepath}")
                time.sleep(0.5)  # Prevent rate limiting

        # ✅ Print daily API usage
        print(f"📊 {current_day.date()} → API calls used so far: {api_call_count}")
        current_day += timedelta(days=1)
