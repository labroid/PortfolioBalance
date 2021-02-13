"""
TODO:
Migrate to database
Quotes from web
Build scrapers
"""

import numpy as np
import pandas as pd
import streamlit as st

import urllib
from load_portfolio import load_portfolio

portfolio, diversity = load_portfolio()
p = portfolio[~portfolio.charity]

dist = p.groupby(['kind', 'region'], as_index=False).value.sum().fillna(0)
values = dist.set_index(['kind', 'region']).value.unstack(level=0).fillna(0).transpose()
target_alloc = diversity.allocation.unstack(level=0).transpose()

net_worth = p.value.sum()

st.title("Investments Summary")
st.write(f"### Net worth: ${portfolio.value.sum():,.2f}")
st.write(f"### Net worth less charity: ${net_worth:,.2f}")

missing = portfolio.kind.isna() | portfolio.region.isna()
if missing.any():
    st.write("**Warning** Some symbols have incomplete classification. Update classification table.")
    st.write(portfolio.loc[missing, ['broker', 'symbol']])

st.write('## Distribution')
v = values.applymap(lambda x: f"${x:,.0f}")
f = (values / net_worth).fillna(0).applymap(lambda x: f"{100 * x:,.0f}")
a = target_alloc.applymap(lambda x: f"{100 * x:,.0f}")
target_value = target_alloc * net_worth
t = target_value.applymap(lambda x: f"${x:,.0f}")
move_needed = target_alloc * net_worth - values
m = move_needed.applymap(lambda x: f"${x:,.0f}")

display = pd.concat([v.Domestic, f.Domestic, a.Domestic, v.International, f.International, a.International], axis=1)
display.columns = ['Domestic', 'Amt %', 'Goal %', 'Int', 'Amt %', 'Goal %']
display[display == "$0"] = "-"
display

st.write("## Necessary moves")
display_moves = pd.concat([v.Domestic, t.Domestic, m.Domestic, v.International, t.International, m.International], axis=1)
display_moves.columns = ['Domestic', 'Target', 'Diff', "Int'l", 'Target', 'Diff']
display_moves[display_moves == "$0"] = "-"
display_moves


