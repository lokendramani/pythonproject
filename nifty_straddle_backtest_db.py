import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import os
from sqlalchemy import create_engine
from datetime import datetime, date

# ======================= CONFIG =========================
engine = create_engine("postgresql+psycopg2://postgres:kaushik9@localhost:5432/Trading")
log_dir = "D:/Manidata/trading/backtest/trade_logs/"
plot_dir = "D:/Manidata/trading/backtest/trade_plots/"
os.makedirs(log_dir, exist_ok=True)
os.makedirs(plot_dir, exist_ok=True)

# ================== STRATEGY LOGIC ======================
def round_to_nearest_50(x):
    return int(round(x / 50.0)) * 50

def run_straddle_strategy(trade_date):
    try:
        entry_time = f"{trade_date} 09:50:00"
        open_time = f"{trade_date} 09:15:00"
        end_time = f"{trade_date} 15:01:00"

        # --- Get NIFTY spot open ---
        spot_df = pd.read_sql(f"""
            SELECT * FROM trading.stock_data 
            WHERE stock_code = 'NIFTY' AND date_time = '{open_time}'
        """, engine)
        if spot_df.empty:
            print(f"{trade_date}: No NIFTY open")
            return

        open_price = spot_df.iloc[0]['open_price']
        atm_strike = round_to_nearest_50(open_price)

        # --- Get CE/PE at 9:50 ---
        ce_df = pd.read_sql(f"""
            SELECT * FROM trading.stock_data 
            WHERE strike_price = {atm_strike} 
              AND right_type = 'Call'
              AND date_time = '{entry_time}'
        """, engine)
        pe_df = pd.read_sql(f"""
            SELECT * FROM trading.stock_data 
            WHERE strike_price = {atm_strike} 
              AND right_type = 'Put'
              AND date_time = '{entry_time}'
        """, engine)

        if ce_df.empty or pe_df.empty:
            print(f"{trade_date}: Missing CE/PE data")
            return

        ce_symbol = ce_df.iloc[0]['stock_code']
        pe_symbol = pe_df.iloc[0]['stock_code']
        ce_entry = ce_df.iloc[0]['close_price']
        pe_entry = pe_df.iloc[0]['close_price']
        entry_total = ce_entry + pe_entry

        # --- Fetch full day CE/PE + NIFTY spot ---
        monitor_df = pd.read_sql(f"""
            SELECT stock_code, exchange_code, right_type, strike_price, date_time, close_price 
            FROM trading.stock_data 
            WHERE date_time > '{entry_time}' 
              AND date_time <= '{end_time}'
              AND (
                    (stock_code = 'NIFTY' and exchange_code = 'NSE') or
                    (stock_code = '{ce_symbol}' and exchange_code = 'NFO' and right_type = 'Call' and strike_price = {atm_strike}) or
                    (stock_code = '{pe_symbol}' and exchange_code = 'NFO' and right_type = 'Put' and strike_price = {atm_strike})
                )
        """, engine)

        if monitor_df.empty:
            print(f"{trade_date}: No monitoring data")
            return

        # ✅ Create uniquely identifying label using full key
        monitor_df['symbol_label'] = monitor_df.apply(
            lambda row: (
                'NIFTY' if (row['stock_code'] == 'NIFTY' and row['exchange_code'] == 'NSE') else
                f"{row['stock_code']}_{row['exchange_code']}_{row['right_type'].upper()}_{int(row['strike_price'])}"
            ), axis=1
        )

        pivot_df = monitor_df.pivot_table(
            index='date_time',
            columns='symbol_label',
            values='close_price',
            aggfunc='last'
        ).dropna()

        ce_col = f"{ce_symbol}_NFO_CALL_{atm_strike}"
        pe_col = f"{pe_symbol}_NFO_PUT_{atm_strike}"
        pivot_df['total_pnl'] = (entry_total - pivot_df[ce_col] - pivot_df[pe_col]) * 75
        pivot_df['sl_hit'] = pivot_df['total_pnl'] <= -2000

        # --- Exit logic ---
        sl_row = pivot_df[pivot_df['sl_hit']]
        exit_time = sl_row.index[0] if not sl_row.empty else pivot_df.index[-1]

        ce_exit = pivot_df.loc[exit_time][ce_col]
        pe_exit = pivot_df.loc[exit_time][pe_col]
        final_pnl = pivot_df.loc[exit_time]['total_pnl']
        sl_hit_flag = not sl_row.empty
        max_drawdown = pivot_df['total_pnl'].min()
        max_profit = pivot_df['total_pnl'].max()

        # --- Save CSV ---
        log_df = pd.DataFrame([{
            'date': trade_date,
            'nifty_open': open_price,
            'strike': atm_strike,
            'ce_symbol': ce_symbol,
            'pe_symbol': pe_symbol,
            'ce_entry': ce_entry,
            'pe_entry': pe_entry,
            'ce_exit': ce_exit,
            'pe_exit': pe_exit,
            'entry_time': entry_time,
            'exit_time': exit_time,
            'final_pnl': final_pnl,
            'sl_hit': sl_hit_flag,
            'max_drawdown': max_drawdown,
            'max_profit': max_profit
        }])
        log_df.to_csv(master_log_path, mode = 'a', index=False, header=not os.path.exists(master_log_path))

        # --- Plot MTM + NIFTY ---
        plt.figure(figsize=(12, 6))
        ax1 = plt.gca()
        ax1.plot(pivot_df.index, pivot_df['total_pnl'], label='Straddle P&L', color='blue')
        ax1.axhline(-2000, color='red', linestyle='--', label='SL (-2000)')
        exit_label = exit_time.strftime('%H:%M:%S')
        ax1.axvline(exit_time, color='orange', linestyle='--', label=f'Exit @ {exit_label}')
        ax1.set_ylabel('P&L (₹)', color='blue')
        ax1.tick_params(axis='y', labelcolor='blue')

        ax2 = ax1.twinx()
        ax2.plot(pivot_df.index, pivot_df['NIFTY'], label='NIFTY Spot', color='green', alpha=0.6)
        ax2.set_ylabel('NIFTY Spot', color='green')
        ax2.tick_params(axis='y', labelcolor='green')

        plt.title(f"{trade_date} - {atm_strike} Straddle P&L + NIFTY")
        fig = plt.gcf()
        fig.autofmt_xdate()
        ax1.legend(loc='upper left')
        ax2.legend(loc='upper right')
        plt.tight_layout()
        plt.savefig(os.path.join(plot_dir, f"{trade_date}_mtm_plot.png"))
        plt.close()

        print(f"{trade_date} ✅ Done | PnL: ₹{final_pnl:.2f} | SL hit: {sl_hit_flag}")

    except Exception as e:
        print(f"❌ Error on {trade_date}: {e}")

# ============= EXECUTE FOR DATE RANGE ===================
from datetime import timedelta

start_date = date(2024, 8, 1)
end_date = date(2025, 5, 31)
backtest_start_date = start_date.strftime('%Y-%m-%d')
master_log_path = os.path.join(log_dir, f"{backtest_start_date}_backtest_log.csv")
# Remove old file if exists (optional for clean rerun)
if os.path.exists(master_log_path):
    os.remove(master_log_path)

for d in pd.date_range(start=start_date, end=end_date):
    run_straddle_strategy(d.strftime('%Y-%m-%d'))
