from breeze_connect import BreezeConnect
from datetime import datetime, timedelta
import pandas as pd
import time

import localConfiguration

breeze = BreezeConnect(api_key=localConfiguration.API_KEY)
breeze.generate_session(api_secret=localConfiguration.API_SECRET, session_token=localConfiguration.SESSION_TOKEN)

# ETF symbol to name map
etf_map = {
    "ICIGOL": "ICICI PRUDENTIAL GOLD ETF",
    "ICIPIT": "ICICI PRUDENTIAL NIFTY IT ETF",
    "ICIPRI": "ICICI PRU NIFTY PVT BANK ETF",
    "MOTDEF": "MOTILAL OSWAL NFTY IND DEF ETF",
    "ICINIF": "ICICI PRUDENTIAL NIFTY 50 ETF",
    "ICICON":"ICICI PRUD CONSUMPTION ETF",
    "MOTNIF": "MOTILAL OSWAL NIFTY 500 ETF",
    "ICIFMC": "ICICI PRU NIFTY FMCG ETF",
    "ICIOIL": "ICICI PRUD NIFTY OIL & GAS ETF",
    "MIRFIN": "MIRAE ASSET NIFTY FIN SERV ETF",
    "MONET1": "MOTILAL OSWAL NIFTY REALTY ETF",
    "CPSETF":"CPSE ETF",
    "ICIPBE":"ICICI PRU NIFTY BANK ETF",
    "NIPPHA":"NIPPON INDIA NIFTY PHARMA ETF",
    "ICIMET":"ICICI PRUD NIFTY METAL ETF",
"ICIPSE":"ICICI PRUDENTIAL SILVER ETF",
"ICIAUT":"ICICI PRU NIFTY AUTO ETF",
"ICIPSU":"ICICI PRUDEN NIF PSU BANK ETF",
"ICIINF":"ICICI Prudent Nifty Infra ETF",
"TATNID":"TATA NIFTY INDIA DIGITAL ETF",
    "MIRNYS": "MOTILAL OSWAL FANG ETF",
"MOTQ50": "MOTILAL OSWAL NASDAQ Q 50 ETF",
    "MOTNAS":"MOTILAL OSWAL MS NASDAQ 100ETF",
"HANBEE":"NIPPON INDIA ETF HANGSENG BEES"
}
etf_sector_map = {
"ICICI PRUDENTIAL GOLD ETF": "GOLD",
"ICICI PRUDENTIAL NIFTY IT ETF": "NIFTY IT",
"ICICI PRU NIFTY PVT BANK ETF": "NIFTY Private Bank",
"MOTILAL OSWAL NFTY IND DEF ETF": "NIFTY INDIA DEFENCE",
"ICICI PRUDENTIAL NIFTY 50 ETF": "NIFTY 50",
"ICICI PRUD CONSUMPTION ETF": "NIFTY CONSUMPTION",
"MOTILAL OSWAL NIFTY 500 ETF": "NIFTY 500",
"ICICI PRU NIFTY FMCG ETF": "NIFTY FMCG",
"ICICI PRUD NIFTY OIL & GAS ETF": "NIFTY OIL & GAS",
"MIRAE ASSET NIFTY FIN SERV ETF": "NIFTY FINANCIAL SERVICES",
"MOTILAL OSWAL NIFTY REALTY ETF": "NIFTY REALTY",
"CPSE ETF": "CPSE",
"ICICI PRU NIFTY BANK ETF": "NIFTY BANK",
"NIPPON INDIA NIFTY PHARMA ETF": "NIFTY PHARMA",
"ICICI PRUD NIFTY METAL ETF": "NIFTY METAL",
"ICICI PRUDENTIAL SILVER ETF": "SILVER",
"ICICI PRU NIFTY AUTO ETF": "NIFTY AUTO",
"ICICI PRUDEN NIF PSU BANK ETF": "NIFTY PSU BANK",
"ICICI Prudent Nifty Infra ETF": "NIFTY INFRA",
"TATA NIFTY INDIA DIGITAL ETF": "NIFTY INDIA DIGITAL",
"MOTILAL OSWAL FANG ETF": "FANG",
"MOTILAL OSWAL NASDAQ Q 50 ETF": "NASDAQ Q 50",
"MOTILAL OSWAL MS NASDAQ 100ETF": "NASDAQ 100",
"NIPPON INDIA ETF HANGSENG BEES":"HANGSENG"
}
symbols = list(etf_map.keys())

# -------------------- Date Setup --------------------
end_date = datetime.today() #- timedelta(days=1)
start_date = end_date - timedelta(days=30)

def format_date(dt):
    return dt.strftime('%Y-%m-%dT09:15:00.000Z')  # Breeze API format

start_date_str = format_date(start_date)
end_date_str = format_date(end_date)

# For display
start_label = start_date.strftime('%d-%b-%Y')
end_label = end_date.strftime('%d-%b-%Y')

# -------------------- Get Return --------------------
def get_etf_return(symbol):
    try:
        data = breeze.get_historical_data_v2(
            interval="1day",
            from_date=start_date_str,
            to_date=end_date_str,
            stock_code=symbol,
            exchange_code="NSE",
            product_type="cash"
        )
        if data and "Success" in data and len(data["Success"]) >= 2:
            prices = data["Success"]
            first_close = round(prices[0]["close"], 2)
            last_close = round(prices[-1]["close"], 2)
            diff = round(last_close - first_close, 2)
            pct = round((diff / first_close) * 100, 2)
            return (etf_map[symbol], first_close, last_close, diff, pct)
    except Exception as e:
        print(f"Error fetching {symbol}: {e}")
    return None

# -------------------- Run Loop --------------------
results = []

for symbol in symbols:
    result = get_etf_return(symbol)
    if result:
        results.append(result)
    time.sleep(1.2)

# -------------------- Output --------------------
df = pd.DataFrame(results, columns=[
    "ETF Name", start_label, end_label, "Difference", "% Return"
])
df = df.sort_values(by="% Return", ascending=False).reset_index(drop=True)

print(df.to_string(index=False))
df["ETF Name"] = df["ETF Name"].replace(etf_sector_map)
print(df.to_string(index=False))