from datetime import datetime
import math

def round_to_nearest_50(value):
    return int(round(value / 50) * 50)

def generate_strike_range(atm, spread=1000, step=50):
    return list(range(atm - spread, atm + spread + 1, step))

def get_monthly_expiry_date(symbol, year, month ):
    # Returns list of last Thursday of each month between dates
    import pandas as pd
    df = pd.read_csv("data/expiry_data")
    expiry_row = df[
        (df['symbol'].str.upper() == symbol.upper()) &
        (df['year'] == year) &
        (df['month'] == month) &
        (df['expiry_type'].str.lower() == 'monthly')
    ]
    if not expiry_row.empty:
        return expiry_row["expiry_date"].iloc[0]  # return the first matching expiry date
    else:
        return None
