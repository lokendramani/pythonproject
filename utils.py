from datetime import datetime
import math

def round_to_nearest_50(value):
    return int(round(value / 50) * 50)

def generate_strike_range(atm, spread=1000, step=50):
    return list(range(atm - spread, atm + spread + 1, step))

def get_monthly_expiry_dates(start_date, end_date):
    # Returns list of last Thursday of each month between dates
    import pandas as pd
    expiry_dates = []
    d = pd.date_range(start=start_date, end=end_date, freq='B')  # business days
    for month, group in d.groupby(d.month):
        thursdays = group[group.weekday == 3]  # 3 = Thursday
        if not thursdays.empty:
            expiry_dates.append(thursdays[-1].date())
    return expiry_dates
