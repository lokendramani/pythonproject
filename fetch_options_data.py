import pandas as pd
import os
import time
from datetime import datetime, timedelta
from breeze_connect import BreezeConnect
from utils import round_to_nearest_50, generate_strike_range, get_monthly_expiry_dates
from config import API_KEY, API_SECRET, SESSION_TOKEN

# Init Breeze
breeze = BreezeConnect(api_key=API_KEY)
breeze.generate_session(api_secret=API_SECRET, session_token=SESSION_TOKEN)

OUTPUT_DIR = "data/options"
os.makedirs(OUTPUT_DIR, exist_ok=True)


def get_nifty_open_price(trading_day):
    """Fetch NIFTY open price for the given day"""
    from_iso = f"{trading_day}T09:15:00.000Z"
    to_iso = f"{trading_day}T09:16:00.000Z"
    data = breeze.get_historical_data_v2(
        stock_code="NIFTY",
        exchange_code="NSE",
        interval="1minute",
        from_date=from_iso,
        to_date=to_iso,
        product_type="cash"
    )
    if data["Success"]:
        return float(data["Success"][0]["open"])
    else:
        raise Exception(f"Unable to get NIFTY open price on {trading_day}")


def fetch_option_data(trading_day, expiry_day, strike, option_type):
    """Fetch 1-minute data for a single option"""
    symbol = f"NIFTY{expiry_day.strftime('%y%m%d')}{strike}{option_type}"
    from_iso = f"{trading_day}T09:15:00.000Z"
    to_iso = f"{trading_day}T15:30:00.000Z"

    try:
        data = breeze.get_historical_data_v2(
            stock_code=symbol,
            exchange_code="NFO",
            interval="1minute",
            from_date=from_iso,
            to_date=to_iso,
            product_type="options"
        )
        if data["Success"]:
            df = pd.DataFrame(data["Success"])
            df["datetime"] = pd.to_datetime(df["datetime"])
            df["symbol"] = symbol
            df["strike"] = strike
            df["type"] = option_type
            df["expiry"] = expiry_day
            return df
    except Exception as e:
        print(f"Error fetching {symbol}: {e}")
    return None


def run_for_expiry_day(trading_day, expiry_day):
    """Fetch and store options data for all strikes"""
    try:
        nifty_open = get_nifty_open_price(trading_day)
    except Exception as e:
        print(e)
        return

    atm = round_to_nearest_50(nifty_open)
    strikes = generate_strike_range(atm)

    output_file = os.path.join(OUTPUT_DIR, f"{trading_day}.csv")
    first_write = True

    for strike in strikes:
        for opt_type in ["CE", "PE"]:
            df = fetch_option_data(trading_day, expiry_day, strike, opt_type)
            if df is not None:
                df.to_csv(output_file, mode='a', index=False, header=first_write)
                first_write = False
            time.sleep(1)  # To avoid rate limiting


if __name__ == "__main__":
    expiry_dates = get_monthly_expiry_dates("2025-01-01", "2025-12-31")
    for expiry in expiry_dates:
        trading_day = expiry.strftime("%Y-%m-%d")
        print(f"🔁 Processing: {trading_day} (Expiry: {expiry})")
        run_for_expiry_day(trading_day, expiry)
