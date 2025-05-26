from breeze_connect import BreezeConnect
import pandas as pd
from datetime import datetime, timedelta
import time
import os

API_KEY = ""
API_SECRET = ""
SESSION_TOKEN = ""


# Breeze login
breeze = BreezeConnect(api_key=API_KEY)
breeze.generate_session(api_secret=API_SECRET, session_token=SESSION_TOKEN)

# Set start and end dates
start_date = datetime(2024, 1, 1)
end_date = datetime(2025, 1, 31)

# Output CSV file
output_file = "nifty_spot_1min_jan2025.csv"

# Delete file if it already exists to start fresh
if os.path.exists(output_file):
    os.remove(output_file)

# Loop over each day
first_write = True

while start_date <= end_date:
    from_iso = start_date.strftime("%Y-%m-%dT09:15:00.000Z")
    to_iso = start_date.strftime("%Y-%m-%dT15:30:00.000Z")

    print(f"📅 Fetching data for: {start_date.strftime('%Y-%m-%d')}")

    try:
        response = breeze.get_historical_data_v2(
            stock_code="NIFTY",
            exchange_code="NSE",
            interval="1minute",
            from_date=from_iso,
            to_date=to_iso,
            product_type="cash"
        )

        if response["Success"]:
            df = pd.DataFrame(response["Success"])
            df["datetime"] = pd.to_datetime(df["datetime"])
            df.to_csv(output_file, mode='a', index=False, header=first_write)
            first_write = False  # Only write header for the first day
        else:
            print("⚠️ No data for this date (maybe holiday or API issue)")

    except Exception as e:
        print(f"❌ Error on {start_date.date()}: {e}")

    # Move to next day
    start_date += timedelta(days=1)

    # Be kind to the API
    time.sleep(1)

print("✅ Done! NIFTY 1-minute data saved to:", output_file)
