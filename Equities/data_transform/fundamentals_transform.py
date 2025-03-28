import pandas as pd
import os 
from google.cloud import bigquery
import numpy as np
from datetime import datetime
from pandas.tseries.offsets import MonthEnd

credentials_path = 'gcp-bigquery-privatekey.json'
transform_map_path = 'field_transform_map.csv'
dimension = 'ART'

#### start function

os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = credentials_path

### gather ticker universe
client = bigquery.Client()
query_job = client.query("""
SELECT distinct ticker
FROM financials.ART_fundamentals_cleaned
""")

ticker_list = query_job.result().to_dataframe()

sub_tickers = np.array_split(ticker_list.ticker, 20)


# get data quality
client = bigquery.Client()
query_job = client.query("""
SELECT *
FROM financials.ART_fundamentals_data_quality
""")

data_quality = query_job.result().to_dataframe()


## make indicator transform map
indicator_list = pd.read_csv(transform_map_path)

### clear old table
query = ''' truncate table boreal-pride-417020.transformed.monthly_financials_filled'''
query_job = client.query(
    query
) 

#### run transformations 

for j in range(0, 20):

    client = bigquery.Client()
    this_list = sub_tickers[j]
    table_id = 'boreal-pride-417020.transformed.temp_tickers'
    job_config = bigquery.LoadJobConfig(write_disposition="WRITE_TRUNCATE")
    job = client.load_table_from_dataframe(
                    pd.DataFrame(this_list), table_id, job_config=job_config
                )
    job.result()

    query_job = client.query("""
    SELECT 
    f.*,                                               
    FROM financials.ART_fundamentals_cleaned f
    inner join transformed.temp_tickers t on f.ticker = t.ticker
    order by ticker, datekey              
    """.format(dimension))
    raw_data = query_job.result().to_dataframe()

    raw_data.loc[:, 'date'] = raw_data.datekey + MonthEnd(0)
    raw_data.loc[:, 'date'] = raw_data.loc[:, 'date'].dt.tz_localize(None)

    financials = raw_data.copy()
    indx = financials.ticker
    financials = financials.groupby('ticker', as_index = False).ffill()
    financials = pd.concat([indx, financials], axis = 1)

    financials = financials.sort_values(by=['ticker', 'datekey'], ascending = False).reset_index(drop=True)
    financials = financials.drop_duplicates(subset = ['ticker', 'date'])
    financials = financials.sort_values(by=['ticker', 'date'], ascending = True).reset_index(drop=True)

    ### Data Quality post fill NA
    ### 0.0002 < fields < 0.01 
    # negatives
    this_field = data_quality.loc[(data_quality.pct_neg > 0.0002) & (data_quality.pct_neg < 0.01), 'indicator']
    metrics_list =  financials.loc[:, this_field]
    filled = metrics_list.clip(lower=-0.1)
    filled = filled.replace(-0.1, np.nan)

    financials.loc[:, this_field] = filled

    # zeros
    this_field = data_quality.loc[(data_quality.pct_zero > 0.0002) & (data_quality.pct_zero < 0.01), 'indicator']
    metrics_list =  financials.loc[:, this_field]
    filled = metrics_list.replace(0, np.nan)

    financials.loc[:, this_field] = filled

    query_job = client.query("""
    SELECT
    v.ticker,
    v.date, 
    v.ev,
    v.evebit,
    v.pb,
    v.pe,
    v.ps                    
    from transformed.monthly_avg_valuation v
    inner join transformed.temp_tickers t on v.ticker = t.ticker 
    order by ticker, date          
    """)
    metrics = query_job.result().to_dataframe()

    metrics.loc[:, 'date'] = pd.to_datetime(metrics.loc[:, 'date'])

    all_data = financials.merge(metrics, how = 'right', on = ['ticker', 'date'])
    indx = all_data.ticker
    all_data = all_data.groupby('ticker', as_index = False).ffill()
    all_data = pd.concat([indx, all_data], axis = 1)

    # additional ratios
    all_data.loc[:, 'ebitmargin'] = round(all_data.loc[:, 'ebit'] / all_data.loc[:, 'revenue'], 3)
    all_data.loc[:, 'opmargin'] = round(all_data.loc[:, 'opinc'] / all_data.loc[:, 'revenue'], 3)
    all_data.loc[:, 'fcfmargin'] = round(all_data.loc[:, 'fcf'] / all_data.loc[:, 'revenue'], 3)

    all_data.loc[:, 'netdebt'] = all_data.loc[:, 'debt'] - all_data.loc[:, 'cashneq']
    all_data.loc[:, 'debt_ebit'] = round(all_data.loc[:, 'debt'] / all_data.loc[:, 'ebit'], 3)
    all_data.loc[:, 'debt_ebitda'] = round(all_data.loc[:, 'debt'] / all_data.loc[:, 'ebitda'], 3)
    all_data.loc[:, 'netdebt_ebit'] = round(all_data.loc[:, 'netdebt'] / all_data.loc[:, 'ebit'], 3)
    all_data.loc[:, 'netdebt_ebitda'] = round(all_data.loc[:, 'netdebt'] / all_data.loc[:, 'ebitda'], 3)
    all_data.loc[:, 'interest_ebit'] = round(all_data.loc[:, 'intexp'].fillna(0) / all_data.loc[:, 'ebit'], 3)

    all_data.loc[:, 'capex_asset'] = round(all_data.loc[:, 'capex'] / all_data.loc[:, 'assets'], 3)
    all_data.loc[:, 'equity_asset'] = round(all_data.loc[:, 'equity'] / all_data.loc[:, 'assets'], 3)
    all_data.loc[:, 'retearn_asset'] = round(all_data.loc[:, 'retearn'] / all_data.loc[:, 'assets'], 3)

    all_metrics = pd.DataFrame()
    for this_ticker in this_list:
        
        data = all_data.loc[all_data.ticker == this_ticker, :]               
        # transformations

        this_fields = indicator_list.loc[indicator_list.dyoy == 1, 'indicator'].tolist()
        original = data.loc[:, this_fields]
        lag = original.shift(12)
        transform = original - lag
        transform.columns = original.columns.values + '_dyoy'
        data = pd.concat([data, transform], axis = 1)

        this_fields = indicator_list.loc[indicator_list.yoy == 1, 'indicator'].tolist()
        original = data.loc[:, this_fields]
        lag = original.shift(12)
        transform = original / lag - 1
        transform.columns = original.columns.values + '_yoy'
        data = pd.concat([data, transform], axis = 1)

        this_fields = indicator_list.loc[indicator_list['3mom'] == 1, 'indicator'].tolist()
        original = data.loc[:, this_fields]
        lag = original.shift(3)
        transform = original / lag - 1
        transform.columns = original.columns.values + '_3mom'
        data = pd.concat([data, transform], axis = 1)

        this_fields = indicator_list.loc[indicator_list.d3mom == 1, 'indicator'].tolist()
        original = data.loc[:, this_fields]
        lag = original.shift(3)
        transform = original - lag
        transform.columns = original.columns.values + '_d3mom'
        data = pd.concat([data, transform], axis = 1)

        # other
        data.loc[:, 'peg'] = data.loc[:, 'pe'] / (data.loc[:, 'revenue_yoy']*100)
        data.loc[:, 'psg'] = data.loc[:, 'ps'] /(data.loc[:, 'revenue_yoy']*100)
        
        # append table
        all_metrics = pd.concat([all_metrics, data])

    # write to database
    table_id = 'boreal-pride-417020.transformed.monthly_financials_filled'
    job_config = bigquery.LoadJobConfig(write_disposition="WRITE_APPEND")
    job = client.load_table_from_dataframe(
                    all_metrics, table_id, job_config=job_config
                )
    print('written chunk: '+ str(j))