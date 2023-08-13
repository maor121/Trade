import glob
import os
import trace
import traceback
from pathlib import Path

import yfinance as yf
import pandas as pd


def scrape_historical_stock():

    ticker_done = [
        "^DUX", "^DVS"
        "^DUX", "^DRG", "^DJX", "^DJTTR", "^DJR", "^DJINET", "^DJI", "^DJCISI",
        "^DJCISB", "^DJCIKC", "^DJCIIK", "^DJCIGR", "^DJCIGC", "^DJCICC", "DJCIEN", "DJCICC", "^DJCIAGC", "DAX", "^CXU",
        "^CLL", "^CEX", "^BXR", "^BXN", "^BXM", "^BXD", "^BKX", "^IXIC", "MSFT", "AEX"]

    txt = Path('data_raw/raw').read_text()
    for t in txt.split("\n"):
        ticker_symbol = "^" + t.split(" ")[0]
        try:
            stock = yf.Ticker(ticker_symbol)
            stock.history(period="max", interval="1d").to_csv('%s_%s.csv' % (
                ticker_symbol, stock.info.get('shortName', 'UNK').replace("/", '.')))
        except Exception:
            traceback.print_exc()


def flatten_stocks_df():
    csv_files = glob.glob("data_raw/*.csv")

    csv_columns = ['Date', 'Open', 'High', 'Low', 'Close', 'Volume', 'Dividends', 'Stock Splits']

    ticker_df_dict = {'ticker_symbol': [], 'shortName': [], 'id': []}
    stock_df_dict = {'ticker_id': [], 'id': []}
    for col in csv_columns:
        stock_df_dict[col] = []

    for csv_idx, csv_file in enumerate(csv_files):
        print("CSV: %d/%d" % (csv_idx, len(csv_files)))

        ticker_symbol, stock_short_name = Path(csv_file).name.split("_")
        stock_short_name = stock_short_name.replace(".csv", "")

        ticker_id = len(ticker_df_dict['id'])
        ticker_df_dict['id'].append(ticker_id)
        ticker_df_dict['shortName'].append(stock_short_name)
        ticker_df_dict['ticker_symbol'].append(ticker_symbol)

        csv_df = pd.read_csv(csv_file)
        for __, row in csv_df.iterrows():
            stock_df_dict['id'].append(len(stock_df_dict['id']))
            stock_df_dict['ticker_id'].append(ticker_id)
            for col in csv_columns:
                stock_df_dict[col].append(row[col])

    ticker_df = pd.DataFrame.from_dict(ticker_df_dict)
    stock_df = pd.DataFrame.from_dict(stock_df_dict)

    ticker_df.to_csv("data/tickers_2.csv", index_label="id")
    stock_df.to_csv("data/stocks_2.csv", index_label="id")


flatten_stocks_df()