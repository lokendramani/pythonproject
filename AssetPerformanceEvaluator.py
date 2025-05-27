from breeze_connect import BreezeConnect
import localConfiguration
import pandas as pd
from datetime import datetime



breeze = BreezeConnect(api_key=localConfiguration.API_KEY)
breeze.generate_session(api_secret=localConfiguration.API_SECRET, session_token=localConfiguration.SESSION_TOKEN)

# Set start and end dates
start_date = datetime(2025, 5, 23)
end_date = datetime(2025, 5, 27)

from_iso = start_date.strftime("%Y-%m-%dT09:15:00.000Z")
to_iso = end_date.strftime("%Y-%m-%dT15:30:00.000Z")
try:
    response = breeze.get_historical_data_v2(
        stock_code="NIFTY",
        exchange_code="NSE",
        interval="1day",
        from_date=from_iso,
        to_date=to_iso,
        product_type="cash"
    )

    if response["Success"]:
        df = pd.DataFrame(response["Success"])
        df["datetime"] = pd.to_datetime(df["datetime"])
        print(df)
    else:
        print("⚠️ No data for this date (maybe holiday or API issue)")

except Exception as e:
    print(f"❌ Error on {start_date.date()}: {e}")