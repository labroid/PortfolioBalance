"""
TODO:
Get reasonable 'report' display
Migrate to database
Build scrapers
"""

import numpy as np
import pandas as pd
import streamlit as st

import urllib
from load_portfolio import load_portfolio

equity_to_bond_target = 0.6
domestic_to_international_target = 0.6
real_estate_target = 0.3
cash_target = 0.25

portfolio, diversity = load_portfolio()
p = portfolio[~portfolio.charity]

dist = p.groupby(['kind', 'region'], as_index=False).value.sum()
values = dist.set_index(['kind', 'region']).value.unstack(level=0)
if 'International' not in values.index:
    values = values.append(pd.Series(np.nan, name='International'))
target_alloc = diversity.allocation.unstack(level=0)

total_net_worth = portfolio.value.sum()
net_worth = p.value.sum()

st.title("Investments Summary")
st.write(f"Net worth: ${total_net_worth:,.2f}")
st.write(f"Net worth less charity: ${net_worth:,.2f}")
'''
## Diversity
Percentages
'''
fraction = values / net_worth
f = fraction.style.format("{:.0%}")
f
'''
Amounts'''
v = values.style.format("${:,.0f}")
v

'''
# Rebalance needs

Targets
'''
dist

d = dist.set_index(['kind', 'region'])
d




# number = 5
# st.write(f"Here is a number: {number}")
# newnumber = st.number_input("Updated value")
#
# st.write(f"New number = {newnumber}, old number = {number}")
# number = newnumber
# st.write(f"Now number is {number}")
