import json

from breeze_connect import BreezeConnect
from datetime import datetime, timedelta

from dateutil.relativedelta import relativedelta

import localConfiguration  # Contains your Breeze credentials

# === Step 1: Initialize BreezeConnect ===
breeze = BreezeConnect(api_key=localConfiguration.API_KEY)
breeze.generate_session(api_secret=localConfiguration.API_SECRET, session_token=localConfiguration.SESSION_TOKEN)
def get_historical_option_data():
    date = datetime.today()
    stock_code = 'RELIND'
    fut_date = date + relativedelta(days=1)
    from_date = date.strftime("%Y-%m-%dT09:15:00.000Z")
    to_date = fut_date.strftime("%Y-%m-%dT15:30:00.000Z")
    try:
        data = breeze.get_historical_data_v2(
            interval="1minute",
            from_date="2025-05-28T09:00:00.000Z",
            to_date="2025-05-28T15:30:00.000Z",
            stock_code="NIFTY",
            exchange_code="NFO",
            product_type="options",
            strike_price="24800",
            right = "call",
            expiry_date="2025-05-29T07:00:00.000Z"
        )
        if "Success" in data and data["Success"]:
            print( json.dumps(data))
    except Exception as e:
        print(f"⚠️ Error fetching price for {stock_code} on {date.date()}: {e}")



data = breeze.get_historical_data_v2(stock_code="NIFTY",
                    exchange_code="NSE",
                    interval="1day",
                    from_date="2025-05-27T09:00:00.000Z",
                    to_date="2025-05-28T15:30:00.000Z",
                    product_type="cash",
                    )
if "Success" in data and data["Success"]:
    print(json.dumps(data))