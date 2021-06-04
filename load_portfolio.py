from txt_to_df import raw_assets_to_df, raw_schwab_to_df, raw_fidelity_to_df
import pandas as pd
from pathlib import Path
import pickle
import alpaca_trade_api as tradeapi
import arrow
import numpy as np
import sys

START_DATE = pd.to_datetime('2021-02-05')
CLASSIFICATIONS = r"C:\Users\Scott\Documents\Brokerage\Classifications.ods"
TYPES = r"C:\Users\Scott\Documents\Brokerage\Types.ods"
DIVERSITY = r"C:\Users\Scott\Documents\Brokerage\Diversity.ods"
PICKLE_DIR = r"C:\Users\Scott\Documents\Brokerage\pickles"
FILES_PROCESSED = r"C:\Users\Scott\Documents\Brokerage\pickles\files_processed.pkl"
HISTORY = r"C:\Users\Scott\Documents\Brokerage\pickles\holdings.pkl"
TXT_FILE_DIR = r"C:\Users\Scott\Documents\Brokerage"
ASSETS = r"C:\Users\Scott\Documents\Brokerage\Assets.csv"
TARGET_GLOB_FUNCTION = {
    'All-Accounts*': raw_schwab_to_df,
    'Portfolio_Position*': raw_fidelity_to_df,
    'Assets.csv': raw_assets_to_df,
}

idx = pd.IndexSlice


def update_history_old(types):  # TODO: This should also look for file changes
    if not Path(FILES_PROCESSED).exists():
        with Path(FILES_PROCESSED).open("wb") as fp:
            pickle.dump([], fp)
    if Path(HISTORY).exists():
        history = pd.read_pickle(HISTORY)
    with Path(FILES_PROCESSED).open("rb") as fp:
        files_processed = pickle.load(fp)
    for g in TARGET_GLOB_FUNCTION:
        for f in Path(TXT_FILE_DIR).glob(g):
            if f not in files_processed:
                if not Path(HISTORY).exists():
                    history = TARGET_GLOB_FUNCTION[g](f)
                    history.to_pickle(HISTORY)
                else:
                    history = pd.concat([history, TARGET_GLOB_FUNCTION[g](f)])
                files_processed.append(f)
    # history = pd.concat([history, raw_assets_to_df(ASSETS)])
    history.replace(r'BRK/B', 'BRK.B', inplace=True)
    # history.drop_duplicates(inplace=True)
    history.to_pickle(HISTORY)
    with Path(FILES_PROCESSED).open("wb") as fp:
        pickle.dump(files_processed, fp)
    return history


def update_history():  # TODO: This just reads 'em all in fresh every time
    dfs = []
    for g in TARGET_GLOB_FUNCTION:
        for f in Path(TXT_FILE_DIR).glob(g):
            dfs.append(TARGET_GLOB_FUNCTION[g](f))
    history = pd.concat(dfs)
    history.replace(r'BRK/B', 'BRK.B', inplace=True)
    assert ~history.reset_index().duplicated().any(), "Duplicates line in history! Panic!"
    return history


def update_quotes(history):
    # TODO: Pickle quotes and only update when requested. Only pick up missing dates.
    quotes = history.loc[:, ['symbol', 'price']].drop_duplicates().copy()
    not_dollar_symbols = list(quotes.symbol[quotes.price != 1.0].unique())
    api = tradeapi.REST(
        'AK82XKYMNMGNFTT24V6J', 'tUcBbHdtVb5aT4nY0HuBVw1szpnfgaF4M7Vi4GS3', base_url='https://data.alpaca.markets/v1'
    )  # TODO: Change this to use env variables
    days = (pd.datetime.now() - START_DATE).days
    # try:
    #     bars = api.get_barset(not_dollar_symbols, "day", days)
    # except tradeapi.rest.APIError:
    # print("Alpaca not responding. Using brokerage quotes.")
    quotes = (
        quotes.set_index(['symbol'], append=True).unstack(1).ffill().resample('D').ffill().droplevel(0, axis='columns')
    )
    return quotes
    close = bars.df.loc[:, idx[:, 'close']].copy()
    close.columns = close.columns.droplevel(1).copy()
    close.dropna(axis='columns', how='all', inplace=True)
    close = close.stack().reset_index(1).rename({'level_1': 'symbol', 0: 'price'}, axis='columns')
    close.reset_index(inplace=True)
    quotes = quotes.append(close)
    quotes = quotes.drop_duplicates().set_index(['time', 'symbol']).unstack().droplevel(0, axis='columns')
    quotes.sort_index(inplace=True)
    # quotes.resample("D", origin='start_day').ffill()
    quotes.ffill(axis='index', inplace=True)
    quotes.fillna(0, inplace=True)
    quotes = quotes.iloc[2:, :].copy()
    quotes = pd.concat([quotes], keys=['price'], axis='columns')
    return quotes


def get_portfolio(history):
    portfolio = (
        history.drop(columns=['basis'])
        .pivot(columns=['broker', 'account', 'symbol'])
        .reorder_levels([1, 2, 3, 0], axis='columns')
    )
    start_date = arrow.get('2021/02/05').datetime
    account_frames = []
    for account in history.account.unique():  # TODO: This should be done on unique broker/acct pairs
        account_frames.append(portfolio.loc[:, idx[:, account, :, :]].dropna(how='all').fillna(0))
    portfolio = pd.DataFrame().join(account_frames, how='outer').resample('D').last().ffill()
    return portfolio[portfolio.index >= start_date].copy()


def load_data():
    """
    Loads brokerage data, assett data, and classifications for analysis.
    Currently reads from text files; will migrate to a database read
    """

    types = pd.read_excel(TYPES, index_col=0)
    assert ~any(types.duplicated()), "Error: Duplicate row in Types"
    classes = pd.read_excel(CLASSIFICATIONS, index_col=0, dtype=np.str)
    assert ~any(classes.duplicated()), "Error: Duplicate row in Classifications"
    diversity = pd.read_excel(DIVERSITY, usecols='A:C', header=0, skiprows=9, nrows=8).set_index(['kind', 'region'])
    # TODO: also check that all accounts and types have been covered
    history = update_history()
    history = history[history.account.isin(types.index[types.include])]
    portfolio = get_portfolio(history)
    return portfolio, diversity, types, classes, history


if __name__ == '__main__':
    load_data()
