import numpy as np
import pandas as pd
import arrow
import io
import re
import csv
from pydantic import BaseModel, validator
from datetime import datetime

SCHWAB_FILE = r"C:\Users\Scott\Desktop\All-Accounts-Positions-2021-01-29-111408.CSV"
FIDELITY_FILE = r"C:\Users\Scott\Desktop\Portfolio_Position_Jan-26-2021.csv"


# COL_ORDER = [
#     "date",
#     "broker",
#     "account",
#     "symbol",
#     "quantity",
#     "price",
#     "value",
#     "basis",
#     "kind",
#     "qualified",
# ]


class Record(BaseModel):
    date: datetime = None
    broker: str
    account: str
    symbol: str
    quantity: float
    price: float
    value: float
    basis: float

    @validator('symbol', 'quantity', 'price', 'value', 'basis', pre=True)
    def prep_dollars(cls, d):
        if d in ["--", "Incomplete", "n/a"]:
            return 0
        return d.replace('$', '').replace(',', '')


def raw_schwab_to_df(schwab_file):
    records = []
    with open(schwab_file) as f:
        raw = csv.reader(f)
        timestamp = arrow.get(next(raw)[0], "hh:mm A[ ET, ]MM/DD/YYYY").datetime
        regex = re.compile(r".*(XXXX-....).*")
        for r in raw:
            if len(r) == 0:
                continue
            if match := regex.search(r[0]):
                account = match.group(1)
                continue
            if r[0] in [
                "Symbol",
                "Account Total",
            ]:
                continue
            records.append(
                Record(date=timestamp,
                       broker="Schwab",
                       account=account,
                       symbol=r[0],
                       quantity=r[2],
                       price=r[3],
                       value=r[6],
                       basis=r[9],
                       )
            )
        return records


# def raw_schwab_to_df2(schwab_file):
#     with open(schwab_file) as f:
#         schwab_raw = [r.replace("$", "") for r in f.readlines()]
#     # Columns must be first row or read_csv fails or skips rows above idenified col row
#     schwab_raw[0], schwab_raw[1] = schwab_raw[3], schwab_raw[0]
#     # Pack it all up to look like a file for pd.read_csv
#     buffer = io.StringIO()
#     [buffer.write(f"{s}\n") for s in schwab_raw]
#     buffer.seek(0)
#
#     schwab = pd.read_csv(
#         buffer,
#         usecols=["Symbol", "Quantity", "Price", "Market Value", "Cost Basis"],
#         skip_blank_lines=True,
#     )
#     schwab.rename(
#         columns={
#             "Symbol": "symbol",
#             "Quantity": "quantity",
#             "Price": "price",
#             "Market Value": "value",
#             "Cost Basis": "basis",
#         },
#         inplace=True,
#     )
#     schwab = schwab.loc[schwab.symbol != "Symbol", :].copy()
#     schwab = schwab.loc[schwab.symbol != "Account Total", :].copy()
#     schwab_timestamp = arrow.get(schwab.iloc[0, 0], "hh:mm A[ ET, ]MM/DD/YYYY")
#     schwab.drop(index=0, inplace=True)
#     schwab[schwab == "--"] = np.nan
#     schwab[schwab == "Incomplete"] = np.nan
#     dollar_cols = ["quantity", "price", "value", "basis"]
#     schwab[dollar_cols] = (
#         schwab[dollar_cols]
#             .applymap(lambda d: d.replace(",", ""), na_action="ignore")
#             .astype(float)
#             .copy()
#     )
#     schwab = schwab.fillna(0)
#
#     # At this point we have clean input data and begin augmenting it
#     schwab["broker"] = "Schwab"
#     schwab["account"] = schwab["symbol"].str.extract(r".*(XXXX-....).*")
#     schwab["kind"] = None
#     schwab["qualified"] = None
#     schwab["date"] = schwab_timestamp
#     schwab = schwab[COL_ORDER].copy()
#     acct_filter = schwab.account.isna()
#     schwab.account.ffill(inplace=True)
#     schwab = schwab.loc[acct_filter, :].copy()
#     schwab.replace("Cash & Cash Investments", "Cash", inplace=True)
#     cash_filter = schwab.symbol == "Cash"
#     schwab.loc[cash_filter, "price"] = 1
#     schwab.loc[cash_filter, "quantity"] = schwab.loc[cash_filter, "value"]
#     schwab.replace("No Number", "Golub", inplace=True)
#     return schwab


def raw_fidelity_to_df(fidelity_file):
    with open(fidelity_file) as f:
        records = [r for r in csv.reader(f)]
    timestamp = arrow.get(records[-1][0], "MM/DD/YYYY hh:mm A").datetime
    answer = []
    for r in records:
        if len(r) == 0:
            break
        if r[0] in ["Account Name/Number"]:
            continue
        answer.append(
            Record(
                date=timestamp,
                broker="Fidelity",
                account=r[0],
                symbol=r[1].replace("*", ""),
                quantity=r[3],
                price=r[4],
                value=r[6],
                basis=r[12],
            ).dict()
        )
    return answer


# def raw_fidelity_to_df2(fidelity_file):
#     fidelity = pd.read_csv(fidelity_file, skip_blank_lines=True)
#     fidelity = fidelity.rename(
#         columns={
#             "Account Name/Number": "account",
#             "Symbol": "symbol",
#             "Quantity": "quantity",
#             "Last Price": "price",
#             "Current Value": "value",
#             "Cost Basis Total": "basis",
#         }
#     )
#     fidelity_timestamp = arrow.get(fidelity.iloc[-1, 0], "MM/DD/YYYY hh:mm A")
#     # Pare down to needed columns
#     fidelity = fidelity[
#         ["account", "symbol", "quantity", "price", "value", "basis"]
#     ].copy()
#     # Eliminate unneeded rows
#     fidelity = fidelity[fidelity.symbol.notnull()].copy()
#     dollar_cols = ["price", "value", "basis"]
#     fidelity[dollar_cols] = (
#         fidelity[dollar_cols]
#             .applymap(lambda d: d.replace("$", "").replace(",", ""), na_action="ignore")
#             .astype(float)
#             .copy()
#     )
#     fidelity = fidelity.fillna(0)
#
#     # At this point we have clean input data and begin augmenting it
#     fidelity["broker"] = "fidelity"
#     fidelity["kind"] = None
#     fidelity["qualified"] = None
#     fidelity["date"] = fidelity_timestamp
#     fidelity = fidelity[COL_ORDER].copy()
#     return fidelity


fidelity = pd.DataFrame(raw_fidelity_to_df2(FIDELITY_FILE))
schwab = pd.DataFrame(raw_schwab_to_df2(SCHWAB_FILE))
print("Done")
