from txt_to_df import raw_assets_to_df, raw_schwab_to_df, raw_fidelity_to_df
import pandas as pd
from pathlib import Path
import pickle
import alpaca_trade_api as tradeapi
import datetime
import numpy as np

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

# def get_history():
#     if Path(HISTORY).exists():
#         history = pd.read_pickle(HISTORY)
#         return history


def update_history(types):  # TODO: This should also look for file changes
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
    return history  # TODO: If symbol goes to nan after being purchased, it must be set to zero


def update_quotes(history):
    # TODO: Pickle quotes and only update when requested. Only pick up missing dates.
    quotes = history.loc[:, ['symbol', 'price']].copy()
    not_dollar_symbols = list(quotes.symbol[quotes.price != 1.0].unique())
    api = tradeapi.REST(
        'AKN66Y14SWULD1APGK5U', 'T56WK4tNz6lkmaNdMH3hViIIYOV4nJZs72S5IXbO',
        base_url='https://data.alpaca.markets/v1'
    )  # TODO: Change this to use env variables
    days = (pd.datetime.now() - START_DATE).days
    bars = api.get_barset(not_dollar_symbols, "day", days)
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


def get_holdings(history):
    h = (
        history.loc[:, ['broker', 'account', 'symbol', 'quantity']]
        .set_index(['broker', 'account', 'symbol'], append=True)
        .unstack([1, 2, 3])
        .reorder_levels([1, 2, 3, 0], axis=1)
    )
    broker_frames = []
    for broker in history.broker.unique():
        broker_frames.append(
            h.loc[:, idx[broker, :, :, :]]
            .dropna(how='all')  # Drop account/symbols not with this broker
            .fillna(0)  # nan here means the symbol has been sold to zero, so zero out the nan
            .resample('D')  # create daily summary
            .last()  # keep latest holdings if more than one update in a day
            .ffill()  # fill in days missing holdings updates
        )
    holdings = (broker_frames[0]
                .join(broker_frames[1:], how='outer')
                .ffill()
                .resample('D')
                .ffill()
                .fillna(0)
                )
    return holdings


def load_data():
    """
    Loads brokerage data, assett data, and classifications for analysis.
    Currently reads from text files; will migrate to a database read
    """

    types = pd.read_excel(TYPES)
    classes = pd.read_excel(CLASSIFICATIONS)
    diversity = pd.read_excel(DIVERSITY, usecols='A:C', header=0, skiprows=9, nrows=8).set_index(['kind', 'region'])
    history = update_history(types)
    holdings = get_holdings(history)
    quotes = update_quotes(history)

    # history = history.loc[~history.account.isin(list(types.account[types.ignore])), :].copy()
    position = history.merge(classes, how="left", on="symbol")
    position = position.merge(types, how="left", on="account")

    return position, diversity


if __name__ == '__main__':
    load_data()
