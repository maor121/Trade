import datetime
from collections import OrderedDict
from typing import Dict

import numpy as np
import pandas as pd
import torch
from torch import nn
from torch.utils.data import Dataset, DataLoader


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
    id_to_ticker = {}
    for t in tickers:
        new_id = len(ticker_to_id)
        ticker_to_id[t] = new_id
        id_to_ticker[new_id] = t
    print(ticker_to_id)

    data_dict_np = {}

    for date_key in date_keys_sorted:
        date_data_dict = data_dict[date_key]

        date_np = np.array([np.nan]*len(tickers))
        for i in range(len(tickers)):
            ticker = id_to_ticker[i]
            val = date_data_dict[ticker]
            date_np[i] = val
        data_dict_np[date_key] = date_np

    print(data_dict_np)
    train_np = np.array([
        data_dict_np[date_key]
        for date_key in date_keys_train])
    train_dict = {
        'dates': np.array(date_keys_train), 'val': train_np}
    val_np = np.array([
        data_dict_np[date_key]
        for date_key in date_keys_val])
    val_dict = {
        'dates': np.array(date_keys_val), 'val': val_np
    }

    return train_dict, val_dict, id_to_ticker


class FollowingMonthsDataset(Dataset):
    def __init__(self, data_dict: Dict[str, np.ndarray]):
        self.pairs_count = len(data_dict['dates']) - 2
        self.data_dict = data_dict

    def __getitem__(self, idx):
        three_months = self.data_dict['val'][idx:idx+3]
        delta0 = three_months[1] - three_months[0]
        delta1 = three_months[2] - three_months[1]

        return ({
                    'date_i': self.data_dict['dates'][idx+1],
                    'month_i': three_months[1].astype(np.float32), 'month_i_delta': delta0.astype(np.float32)},
                delta1.astype(np.float32))

    def __len__(self):
        return self.pairs_count

def loss_per_stock(month_i: torch.Tensor, output: torch.Tensor, label: torch.Tensor):
    output_normed = output / month_i
    label_normed = label / month_i
    error = torch.abs(label_normed - output_normed)
    return error

def train_model(train_loader, val_loader, id_to_ticker):
    model = nn.Sequential(OrderedDict([
        ('dense1', nn.Linear(len(id_to_ticker), len(id_to_ticker))),
        # ('sigmoid', nn.ReLU()),
        # ('dense2', nn.Linear(len(id_to_ticker), len(id_to_ticker))),
    ]))
    optimizer = torch.optim.Adam(model.parameters(), lr=0.001)

    iter_counter = 0
    for epoch in range(epoch_count):
        for train_batch in train_loader:
            model.train()
            iter_counter += 1

            pred_next_month_delta = model(train_batch[0]['month_i_delta'])
            next_month_delta = train_batch[1]
            loss_arr = loss_per_stock(train_batch[0]['month_i'], pred_next_month_delta, next_month_delta)
            loss_avg = torch.mean(loss_arr)
            optimizer.zero_grad()
            loss_avg.backward()
            optimizer.step()

            val_loss_avg_list = []
            if iter_counter % 2 == 0:
                with torch.no_grad():
                    model.eval()
                    for val_batch in val_loader:
                        pred_next_month_delta = model(val_batch[0]['month_i_delta'])
                        next_month_delta = val_batch[1]
                        val_loss_arr = loss_per_stock(val_batch[0]['month_i'], pred_next_month_delta, next_month_delta)
                        val_loss_avg = torch.mean(val_loss_arr)

                        val_loss_avg_list.append(val_loss_avg.detach().numpy())

                    print("Train Loss: %.3f, val_loss: %.3f" % (loss_avg,
                                                                np.mean(np.array(val_loss_avg_list))))

    return model

def evaluate_model(model, val_loader, id_to_ticker):
    pred_df_dict = {'date': []}
    with torch.no_grad():
        model.eval()
        for val_batch in val_loader:
            pred_next_month_delta = model(val_batch[0]['month_i_delta'])
            next_month_delta = val_batch[1]
            val_loss_arr = loss_per_stock(val_batch[0]['month_i'], pred_next_month_delta, next_month_delta)
            # val_loss_avg = torch.mean(val_loss_arr)

            val_x_batch, val_y_batch = val_batch
            batch_len = len(val_y_batch)
            for ex_i in range(batch_len):
                pred_df_dict['date'].append(val_x_batch['date_i'][ex_i].numpy())
                for ticker_id in range(len(id_to_ticker)):
                    ticker = id_to_ticker[ticker_id]
                    symbol = common_stocks_filtered[common_stocks_filtered["ticker"] == ticker]["symbol"].iloc[0]
                    if symbol not in pred_df_dict:
                        pred_df_dict[symbol] = []
                        pred_df_dict[symbol + "_next"] = []
                        pred_df_dict[symbol + "_pred"] = []
                        # pred_df_dict[symbol + "_loss"] = []
                        pred_df_dict[symbol + "_error"] = []
                    symbol_val = val_x_batch["month_i"][ex_i][ticker_id].item()
                    symbol_next_val = symbol_val + val_y_batch[ex_i][ticker_id].item()
                    symbol_pred = symbol_val + pred_next_month_delta[ex_i][ticker_id].item()

                    pred_df_dict[symbol].append(symbol_val)
                    next_prec_change = symbol_next_val / symbol_val - 1
                    pred_prec_change = symbol_pred / symbol_val - 1
                    pred_loss = val_loss_arr[ex_i][ticker_id].item()
                    pred_df_dict[symbol + "_next"].append(next_prec_change)
                    pred_df_dict[symbol + "_pred"].append(pred_prec_change)
                    # pred_df_dict[symbol + "_loss"].append(pred_loss)
                    pred_df_dict[symbol + "_error"].append(abs(pred_prec_change - next_prec_change))

            # val_loss_avg_list.append(val_loss_avg.detach().numpy())
    pred_df = pd.DataFrame.from_dict(pred_df_dict)
    # pred_df["date"] = pd.to_datetime(pred_df["date"])
    # pred_df = pred_df.sort_values("date")
    pred_df.to_csv("pred.csv", index=False)

if __name__ == "__main__":
    # close_monthly()
    common_stocks_filtered = (
        filter_stocks(datetime.datetime(1995, 1, 1), datetime.datetime(2023, 8, 10)))

    train_dict, val_dict, id_to_ticker = build_data_points(common_stocks_filtered)

    # print(val_dict)

    train_dataset = FollowingMonthsDataset(train_dict)
    train_loader = DataLoader(train_dataset, batch_size=20, shuffle=True)
    val_dataset = FollowingMonthsDataset(val_dict)
    val_loader = DataLoader(val_dataset, batch_size=20, shuffle=True)

    epoch_count = 200

    should_train_model = True
    if should_train_model:
        model = train_model(train_loader, val_loader, id_to_ticker)
        torch.save(model, "model.pt")
    else:
        model = torch.load("model.pt")

    should_eval_model = True
    if should_eval_model:
        evaluate_model(model, val_loader, id_to_ticker)

    pred_df = pd.read_csv("pred.csv")
    loss_cols = [l for l in pred_df.columns if "_error" in l]
    pred_loss_df = pred_df[loss_cols].mean().sort_values()
    print(pred_loss_df)
    pred_loss_df = pred_df[loss_cols].std().sort_values()
    print(pred_loss_df)

    # for param in model.parameters():
    #     print(param.data)
