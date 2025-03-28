#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Fri Oct 22 16:08:07 2021

@author: grace
"""


import os
import sys
import pandas as pd
import numpy as np
from datetime import datetime
from pandas.tseries.offsets import MonthEnd


def create_combo_factor_ptf(mfactor_rank, Top_cutoff):
    all_dates = mfactor_rank.reset_index().Date.unique()
    
    multi_factor=pd.DataFrame()
    for date in all_dates:
        X = mfactor_rank.loc[mfactor_rank.Date == date, :]
        Y = X.dropna(subset=["ComboRank"]).sort_values(["ComboRank"]).reset_index(drop=True)
        Z = Y.iloc[0:Top_cutoff, :]
        multi_factor = multi_factor.append(Z)
    
    return(multi_factor)
    
def create_low_turnover_ptf(ptf, Top_cutoff):
    all_dates = ptf.reset_index().Date.unique()

    low_turn_ptf = pd.DataFrame()
    replace_num_ls = []

    date = all_dates[0]
    X = ptf.loc[ptf.Date == date, :]    
    Y = X.iloc[0: Top_cutoff, :]
    low_turn_ptf = low_turn_ptf.append(Y)
    
    for i in range(1, len(all_dates)):
        
        date = all_dates[i]
        prev_date = all_dates[i-1]
        
        prev = low_turn_ptf.loc[low_turn_ptf.Date == prev_date, :]
        current = ptf.loc[ptf.Date == date, :]
        
        existing = np.in1d(current.Ticker, prev.Ticker)
        
        new = current.loc[existing, :]
        new = new.drop_duplicates(subset=["Ticker"])
        
        drop_names = current.Ticker[existing]
        replace = current.set_index("Ticker").drop(drop_names).reset_index()
        replace_num = Top_cutoff - len(new)
        new = new.append(replace[0:replace_num]).reset_index(drop=True)
        
        replace_num_ls.append(replace_num)
        low_turn_ptf = low_turn_ptf.append(new)
    
    return(low_turn_ptf)
    

sp_assets = pd.read_csv('Index_Constituents.csv', encoding = 'cp1252')

raw_data = pd.read_csv("raw_data.csv")
fundamentals = pd.read_csv("fundamentals.csv")


rank_data = raw_data.loc[:, ["Date", "Ticker", "Momentum_6M", "Momentum_6M_Chg_3M", "PX_VOLUME_YoY"]]

rank_data = rank_data.set_index(["Date", "Ticker"]).dropna(axis=0, how="all").reset_index()

all_dates = rank_data.Date.unique()

ranked_tbl = pd.DataFrame()

for date in all_dates:
                 
    X = rank_data.loc[rank_data.Date == date, :]
    X = X.set_index(["Date", "Ticker"])
    Y = X.rank(ascending = False)
    ranked_tbl = ranked_tbl.append(Y)

this_combo = ["Momentum_6M", "Momentum_6M_Chg_3M", "PX_VOLUME_YoY"]

mfactor_name = this_combo.copy()
mfactor_rank = ranked_tbl.loc[:, mfactor_name]

mfactor_rank.loc[:, "ComboRank"] = mfactor_rank.loc[:, this_combo].mean(axis=1, skipna = False)
mfactor_rank = mfactor_rank.reset_index()

# Create low turnover ptf
Top_cutoff = 120
mfactor = create_combo_factor_ptf(mfactor_rank, Top_cutoff)

first_ptf = mfactor.loc[:, ["Date", "Ticker"]]

Top_cutoff = 60
ptf = create_low_turnover_ptf(first_ptf, Top_cutoff)


ptf_latest = ptf.loc[ptf.Date == "2024-03-01", :]

# Add price info

ptf_latest = ptf_latest.merge(raw_data.loc[:, ["Date", "Ticker", "PX_LAST"]], how="left", on= ["Date", "Ticker"])
ptf_latest = ptf_latest.merge(fundamentals.loc[:, ["Ticker", "shortName", "symbol", "country", 'sector', 'industry']], how="left", on = "Ticker")

ptf_latest.to_excel("Momentum_ptf_latest.xlsx")


###### Growth


rank_data = fundamentals.copy()

rank_data = rank_data.set_index(["Ticker"]).dropna(axis=0, how="all")

ranked_tbl = pd.DataFrame()

# Sort ascending  
group = ["PSG", "priceToSalesTrailing12Months"]
X = rank_data.loc[:, group]               
Y = X.rank(ascending = True)
ranked_tbl = Y


group = ["OpMargin_Chg", "operatingMargins", "returnOnEquity", "Revenue_Growth"]
X = rank_data.loc[:, group]               
Y = X.rank(ascending = False)

ranked_tbl = ranked_tbl.merge(Y, how="left", left_index=True, right_index=True)


this_combo = ["PSG", "Revenue_Growth", "OpMargin_Chg"]
mfactor_name = this_combo.copy()
mfactor_rank = ranked_tbl.loc[:, mfactor_name]

mfactor_rank.loc[:, "ComboRank"] = mfactor_rank.loc[:, this_combo].mean(axis=1, skipna = False)
mfactor_rank = mfactor_rank.reset_index()

mfactor_rank.loc[:, "Date"] = "2023-10-15"

# Create low turnover ptf
Top_cutoff = 600
mfactor = create_combo_factor_ptf(mfactor_rank, Top_cutoff)

ptf_latest = mfactor.copy()


# Add price info

ptf_latest = ptf_latest.merge(fundamentals.loc[:, ["Ticker", "priceToSalesTrailing12Months", "Revenue_Growth", "OpMargin_Chg", "operatingMargins",]], how="left", on= ["Ticker"])
ptf_latest = ptf_latest.merge(fundamentals.loc[:, ["Ticker", "shortName", "symbol", "country", 'sector', 'industry']], how="left", on = "Ticker")

ptf_latest.to_excel("Growth_ptf_latest.xlsx")










