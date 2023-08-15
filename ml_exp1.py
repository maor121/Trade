import datetime

import pandas as pd

def close_monthly():
    tickers_df = pd.read_csv("data/investing_com/tickers.csv")
    stocks_df = pd.read_csv("data/investing_com/stocks.csv")

    stocks_merged = pd.merge(stocks_df, tickers_df, on="ticker", suffixes=("_stocks", "_ticker"))
                     # .join(tickers_df, on="ticker", how="inner",
                     #               lsuffix='_stocks', rsuffix='_ticker'))
    stocks_merged["date"] = pd.to_datetime(stocks_merged["date"])
    assert len(stocks_merged) == len(stocks_df)

    print(stocks_merged.head())
    print(stocks_merged.info())
    print(len(stocks_merged))
    # print(stocks_merged)


    pd.set_option('display.max_columns', None)
    pd.set_option('display.max_rows', None)
    # stocks_merged.index = stocks_merged["date"]
    # stocks_merged_g = stocks_merged.groupby(pd.Grouper(freq='M'))
    stocks_merged_g = (stocks_merged.groupby(["ticker", "symbol", "description",
                                             pd.Grouper(key='date', freq='M')])['close']
                       .agg(['median', 'mean']).add_prefix("close_").add_suffix('_month').reset_index())

    # stocks_merged_g = (stocks_merged.groupby(["ticker", "symbol", "description",
    #                                          pd.Grouper(key='date', freq='M')]).agg({'close': 'mean'}).
    #                    rename({"close": "close_m_avg"}))
    stocks_merged_g.to_csv("close_monthly.csv", index=False)
    print(stocks_merged_g)


def filter_stocks(start_date: datetime, end_date: datetime):
    stocks_monthly = pd.read_csv("close_monthly.csv")
    stocks_monthly['date'] = pd.to_datetime(stocks_monthly['date'])

    stocks_monthly = stocks_monthly[stocks_monthly["date"] >= start_date]
    stocks_monthly = stocks_monthly[stocks_monthly["date"] <= end_date]

    first_date = stocks_monthly["date"].min()
    last_date = stocks_monthly["date"].max()
    stocks_in_first_date = set(stocks_monthly[(stocks_monthly.date.dt.month == first_date.month)
                               & (stocks_monthly.date.dt.year == first_date.year)]["ticker"])
    stocks_in_last_date = set(stocks_monthly[(stocks_monthly.date.dt.month == last_date.month)
                              & (stocks_monthly.date.dt.year == last_date.year)]["ticker"])

    print("Number of months: %d" % len(stocks_monthly.groupby("date")))
    print("start stocks: %d, End stocks: %d" %(len(stocks_in_first_date), len(stocks_in_last_date)))
    print("Stocks at start_date, not in end: %s" % (stocks_in_first_date - stocks_in_last_date))
    print("Stocks at last_date, not in first: %s" % (stocks_in_last_date - stocks_in_first_date))

    common_stocks = stocks_in_first_date & stocks_in_last_date
    print("Common stocks: %d" % len(common_stocks))
    common_stocks = stocks_monthly[stocks_monthly["ticker"].isin(common_stocks)].groupby(["ticker", "symbol",
                                                                                          "description"])["close_mean_month"].count()
    # print(common_stocks)
    print("Filter stocks with fewer values then max")
    common_stocks_filtered = common_stocks.nlargest(1, keep='all').reset_index()
    print("Common stocks filtered: %d" % len(common_stocks_filtered))
    # print(common_stocks_filtered)

    common_stocks_final = stocks_monthly[stocks_monthly["ticker"].isin(common_stocks_filtered["ticker"])]
    return common_stocks_final

def build_data_points(common_stocks_filtered: pd.DataFrame):
    common_stocks_filtered = common_stocks_filtered.sort_values("date")

    pd.set_option('display.max_rows', 500)
    pd.set_option('display.max_columns', 500)

    data_dict = {}
    for __, row in common_stocks_filtered.iterrows():
        row_date = row["date"]
        month = row_date.month
        year = row_date.year
        ticker = row["ticker"]
        date_key = (year, month)
        if date_key not in data_dict:
            data_dict[date_key] = {}
        data_dict[date_key][ticker] = row["close_median_month"]
    expected_len = len(data_dict[list(data_dict.keys())[0]])
    for date_key in data_dict:
        assert len(data_dict[date_key]) == expected_len

    date_keys_sorted = list(data_dict.keys())
    # print(date_keys_sorted)

    date_keys_train = date_keys_sorted[:int(len(date_keys_sorted) * 0.7)]
    date_keys_val = date_keys_sorted[len(date_keys_train):]

    print("Train date_keys: %s" % date_keys_train)
    print("Val date keys: %s" % date_keys_val)

    tickers = common_stocks_filtered["ticker"].unique()
    # print("Tickers: %s" % tickers)
    ticker_to_id = {}
    for t in tickers:
        ticker_to_id[t] = len(ticker_to_id)
    print(ticker_to_id)


# close_monthly()
common_stocks_filtered = (
    filter_stocks(datetime.datetime(2007, 6, 1), datetime.datetime(2023, 8, 10)))

build_data_points(common_stocks_filtered)
