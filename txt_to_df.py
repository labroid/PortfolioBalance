import pandas as pd
import arrow
import re
import csv
from pydantic import BaseModel, validator, ValidationError
from datetime import datetime


class Record(BaseModel):
    time: datetime = None
    broker: str
    account: str
    symbol: str
    quantity: float
    price: float
    value: float
    basis: float

    @validator("symbol", "quantity", "price", "value", "basis", pre=True, allow_reuse=True)
    def prep_dollars(cls, d):
        if isinstance(d, (int, float)):
            return d
        if d in ["--", "Incomplete", "n/a"]:
            return 0
        return d.replace("$", "").replace(",", "")


def convert_to_datetime_index(records):
    df = pd.DataFrame.from_dict(records)
    df = df.set_index(pd.DatetimeIndex(df.time)).drop('time', axis='columns')
    return df


def raw_schwab_to_df(schwab_file):
    records = []
    with open(schwab_file) as f:
        raw = csv.reader(f)
        timestamp = arrow.get(next(raw)[0], "hh:mm A[ ET, ]MM/DD/YYYY").datetime
        regex = re.compile(r".*XXXX-(....).*")
        for r in raw:
            if len(r) == 0:
                continue
            if match := regex.search(r[0]):
                account = f"x{match.group(1)}"
                continue
            if r[0] in [
                "Symbol",
                "Account Total",
                ""
            ]:
                continue
            symbol_translate = {"No Number": "Golub", "Cash & Cash Investments": "Cash"}
            if r[0] in symbol_translate:
                r[0] = symbol_translate[r[0]]
            if r[0] == "Cash":
                r[3] = 1.0
                r[2] = r[6]
            try:
                records.append(
                    Record(
                        time=timestamp,
                        broker="Schwab",
                        account=account,
                        symbol=r[0],
                        quantity=r[2],
                        price=r[3],
                        value=r[6],
                        basis=r[9],
                    ).dict()
                )
            except ValidationError as e:
                print(e)
                print(f"Input: {r}")
        return convert_to_datetime_index(records)


def raw_fidelity_to_df(fidelity_file):
    with open(fidelity_file) as f:
        records = [r for r in csv.reader(f)]
    timestamp = arrow.get(records[-1][0], "MM/DD/YYYY h:mm A").datetime
    answer = []
    for r in records:
        if len(r) == 0:
            break
        if r[0] in ["Account Name/Number"]:
            continue
        answer.append(
            Record(
                time=timestamp,
                broker="Fidelity",
                account=f"x{r[0][-4:]}",
                symbol=r[1].replace("*", ""),
                quantity=r[3],
                price=r[4],
                value=r[6],
                basis=r[12],
            ).dict()
        )
    return convert_to_datetime_index(answer)


def raw_assets_to_df(assets_file):
    with open(assets_file) as f:
        raw = csv.reader(f)
        _ = next(raw)  # Toss header row
        answer = []
        for r in raw:
            if len(r) == 0:
                break
            answer.append(
                Record(
                    time=arrow.get(r[4], 'MM/DD/YY').datetime,
                    broker=r[0],
                    account=r[2],
                    symbol=r[1],
                    quantity=r[3],
                    price=1,
                    value=r[3],
                    basis=r[3],
                ).dict()
            )
        return convert_to_datetime_index(answer)


if __name__ == "__main__":
    SCHWAB = r"C:\Users\Scott\Documents\Brokerage\All-Accounts-Positions-2021-02-01-080525.CSV"
    # FIDELITY = r"C:\Users\Scott\Documents\Brokerage\Portfolio_Position_Feb-01-2021.csv"
    FIDELITY = r"C:\Users\Scott\Documents\Brokerage\Portfolio_Position_Jan-26-2021.csv"
    ASSETS = r"C:\Users\Scott\Documents\Brokerage\Assets.csv"

    raw_fidelity_to_df(FIDELITY)
    raw_schwab_to_df(SCHWAB)
    raw_assets_to_df(ASSETS)
