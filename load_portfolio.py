from txt_to_df import raw_assets_to_df, raw_schwab_to_df, raw_fidelity_to_df
import pandas as pd


def load_portfolio():
    """
    Loads brokerage data, assett data, and classifications for analysis.
    Currently reads from text files; will migrate to a database read
    """

    SCHWAB = r"C:\Users\Scott\Documents\Brokerage\All-Accounts-Positions-2021-02-01-080525.CSV"
    FIDELITY = r"C:\Users\Scott\Documents\Brokerage\Portfolio_Position_Feb-01-2021.csv"
    ASSETS = r"C:\Users\Scott\Documents\Brokerage\Assets.csv"
    CLASSIFICATIONS = r"C:\Users\Scott\Documents\Brokerage\Classifications.csv"
    TYPES = r"C:\Users\Scott\Documents\Brokerage\Types.csv"
    DIVERSITY = r"C:\Users\Scott\Documents\Brokerage\Diversity.ods"

    schwab = raw_schwab_to_df(SCHWAB)
    fidelity = raw_fidelity_to_df(FIDELITY)
    assets = raw_assets_to_df(ASSETS)
    classes = pd.read_csv(CLASSIFICATIONS)
    types = pd.read_csv(TYPES)
    diversity = pd.read_excel(DIVERSITY, usecols='A:C', header=0, skiprows=9, nrows=8)
    diversity = diversity.set_index(['kind', 'region'])
    portfolio = pd.concat([schwab, fidelity, assets])
    portfolio = portfolio.merge(classes, how="left", on="symbol")
    portfolio = portfolio.merge(types, how="left", on="account")

    return portfolio, diversity
