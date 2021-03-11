"""
TODO:
Migrate to database
Quotes from web
Build scrapers
"""

import numpy as np
import pandas as pd
import streamlit as st
import matplotlib
import altair as alt

import urllib
from load_portfolio import load_data

idx = pd.IndexSlice

portfolio, diversity, types, classes, history = load_data()
total_values = portfolio.loc[:, idx[:, :, :, 'value']]
total_worth_timeline = total_values.sum(axis='columns')
net_values = total_values.drop(types.index[types.charity], level=1, axis='columns')
net_worth_timeline = net_values.sum(axis='columns').reset_index().rename(columns={0: 'Net Worth'})
dist = (
    net_values.tail(1)
    .stack(['symbol', 'broker', 'account'])
    .droplevel(0)
    .join(classes)
    .groupby(['kind', 'region'])
    .sum()
    .unstack()
    .fillna(0)
    .droplevel(0, axis='columns')
)
target_alloc = diversity.allocation.unstack(level=0).transpose()
net_worth = net_worth_timeline.iloc[-1, 1]

st.title("Investments Summary")
st.write(f"### Net worth: ${total_worth_timeline.iloc[-1]:,.2f}")
st.write(f"### Net worth net charity: ${net_worth:,.2f}")
st.write(
    f"Latest Schwab update: {history.loc[history.broker == 'Schwab'].tail(1).index.strftime('%d %B %Y').values[0]}  \n"
    f"Latest Fidelity update: {history.loc[history.broker == 'Fidelity'].tail(1).index.strftime('%d %B %Y').values[0]}"
)

st.write('## Distribution')
v = dist.applymap(lambda x: f"${x:,.0f}")
f = (dist / net_worth).applymap(lambda x: f"{x:,.0%}")
a = target_alloc.applymap(lambda x: f"{x:,.0%}")
target_value = target_alloc * net_worth
t = target_value.applymap(lambda x: f"${x:,.0f}")
move_needed = target_alloc * net_worth - dist
m = move_needed.applymap(lambda x: f"${x:,.0f}")

display = pd.concat([v.Domestic, f.Domestic, a.Domestic, v.International, f.International, a.International], axis=1)
display.columns = ['Domestic', 'Amt %', 'Goal %', "Int'l", 'Amt %', 'Goal %']
display[display == "$0"] = "-"
display[display == '0%'] = "-"
display

st.write("## Necessary moves")
display_moves = pd.concat(
    [v.Domestic, t.Domestic, m.Domestic, v.International, t.International, m.International], axis=1
)
display_moves.columns = ['Domestic', 'Target', 'Diff', "Int'l", 'Target', 'Diff']
display_moves[display_moves == "$0"] = "-"
display_moves

p = (
    net_values.tail(1)
    .transpose()
    .droplevel(3)
    .reset_index(['broker', 'account'])
    .join(classes)
    .set_index('account', append=True)
    .reset_index('symbol')
    .join(types.qualified)
    .assign(deferred=lambda x: np.where(x.qualified, 'Qualified', 'Unqualified'))
    .drop(columns='qualified')
    .set_index(['broker', 'symbol', 'region', 'kind', 'deferred'], append=True)
    .unstack(['region', 'kind'])
    .droplevel(0, axis='columns')  # This is the time index, which has label 'None' so isn't referenceable by name
    .unstack(['deferred', 'broker'])  # col_fill='ref')
    .groupby('account')
    .sum()
    .stack(['deferred', 'broker'])
    .reorder_levels([1, 2, 0])
    .sort_index(level=[0, 1])
)
p = p.loc[(p != 0).any(axis=1)].copy()

target_row = pd.Series(
    target_alloc.unstack().drop(index=[('International', 'Real Estate'), ('International', 'Cash')]) * net_worth,
    name="Target",
)

p_summary = (p
             .stack(['region', 'kind'])
             .rename('Total')
             .groupby(['region', 'kind'])
             .sum()
             .to_frame()
             .join(target_row.to_frame())
             )
p_summary = p.transpose().join(p_summary)
p_summary['Move'] = p_summary.Target - p_summary.Total
p_display = (p_summary.transpose() / 1000).round(0).applymap(lambda x: f"{x:,.0f}").applymap(lambda x: '' if x == '0' else x)
p_display


st.altair_chart(
    alt.Chart(net_worth_timeline)
    .mark_line()
    .encode(
        alt.X('time'),
        alt.Y(
            'Net Worth:Q',
            scale=alt.Scale(zero=False),
        ),
    )
)

alloc_v_time = (
    net_values.transpose()
    .join(classes)
    .groupby(['kind', 'region'])
    .sum()
    .transpose()
    .unstack()
    .reset_index()
    .rename(columns={'level_2': 'time', 0: 'value'})
)
alloc_v_time['bucket'] = alloc_v_time.region + " " + alloc_v_time.kind
alloc_v_time['allocation'] = alloc_v_time.value / net_worth
alloc_v_time = alloc_v_time.drop(columns=['region', 'kind']).replace(
    to_replace={'Domestic Cash': 'Cash', 'Domestic Real Estate': 'Real Estate'}
)
order = pd.Series(
    {
        'Real Estate': 1,
        'Cash': 2,
        'Domestic Equity': 3,
        'Domestic Bond': 4,
        'International Equity': 5,
        'International Bond': 6,
    },
    name='order',
)

alloc_v_time = alloc_v_time.merge(order, how='left', left_on='bucket', right_index=True)
target_bars = diversity.reset_index()
target_bars['bucket'] = target_bars.region + " " + target_bars.kind
target_bars = (
    target_bars.replace(to_replace={'Domestic Cash': 'Cash', 'Domestic Real Estate': 'Real Estate'})
    .drop(columns=['kind', 'region'])
    .set_index('bucket', drop=False)
    .drop(index=['International Cash', 'International Real Estate'])
    .reindex(
        index=['International Bond', 'International Equity', 'Domestic Bond', 'Domestic Equity', 'Cash', 'Real Estate']
    )
)

target_bars['target'] = target_bars.loc[:, 'allocation'][::-1].cumsum()


st.altair_chart(
    alt.Chart(alloc_v_time).mark_line().encode(alt.X('time:T'), alt.Y('value:Q'), alt.Color('bucket:N')),
    use_container_width=True,
)


area_chart = (
    alt.Chart(alloc_v_time)
    .mark_area()
    .encode(
        alt.X('time:T'),
        alt.Y('allocation:Q', stack='normalize', axis=alt.Axis(format='%')),
        alt.Color('bucket:N', sort=alt.SortField("order", "descending")),
        order='order:N',
    )
)
line = alt.Chart(target_bars).mark_rule().encode(y='target')
st.altair_chart(area_chart + line, use_container_width=True)
