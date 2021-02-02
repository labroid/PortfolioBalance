import pandas as pd
import arrow
import re
import csv
from pydantic import BaseModel, validator
from datetime import datetime

SCHWAB_FILE = r"C:\Users\Scott\Desktop\All-Accounts-Positions-2021-01-29-111408.CSV"
FIDELITY_FILE = r"C:\Users\Scott\Desktop\Portfolio_Position_Jan-26-2021.csv"


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
