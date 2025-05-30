import pandas as pd

data = pd.read_csv("data/EXPIRY_DATA")
year = 2024
month = 1
symbol = "NIFTY"
expiry_type = "MONTHLY"
selRow = data[(data['year']==year) & (data['month']==month) & (data['symbol']==symbol) & (data['expiry_type'] == expiry_type)]
print(selRow[["expiry_date"]])