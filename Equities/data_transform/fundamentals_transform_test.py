import pandas as pd
import os 
from google.cloud import bigquery
import numpy as np
from datetime import datetime
from pandas.tseries.offsets import MonthEnd

credentials_path = 'gcp-bigquery-privatekey.json'
os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = credentials_path
client = bigquery.Client()

### fundamentals
def clean_fundamentals(raw_data, dimension, write_table_id):

    client = bigquery.Client()
    query_job = client.query("""
    SELECT *
    FROM financials.{}_fundamentals_data_quality
    """.format(dimension))

    data_quality = query_job.result().to_dataframe()

    client = bigquery.Client()
    query_job = client.query("""
    SELECT *
    FROM financials.{}_fundamentals_data_quality_by_ticker
    """.format(dimension))

    data_quality_by_ticker = query_job.result().to_dataframe()

    all_data = raw_data.copy()
    all_data = all_data.drop(columns = ['dimension', 'calendardate', 'reportperiod', 'lastupdated'])
    all_data.loc[(all_data.ticker == 'AAPL') & (all_data.liabilities <= 0), 'liabilities'] = np.nan

    #### revenue, revenueusd
    idx = all_data.revenue < 0.0
    all_data.loc[idx, 'revenue'] = 10

    idx = all_data.revenueusd < 0.0
    all_data.loc[idx, 'revenue'] = 10

    ### <.0002 fields
    # zeros
    this_field = data_quality.loc[data_quality.pct_zero < 0.0002, 'indicator']

    metrics_list =  all_data.loc[:, this_field]
    X = metrics_list == 0.0
    Y = X.any(axis = 1)
    drop_tickers = all_data.ticker[Y].unique()

    idx = np.in1d(all_data.ticker, drop_tickers)
    all_data = all_data.loc[~idx, :]

    # neg
    this_field = data_quality.loc[data_quality.pct_neg < 0.0002, 'indicator']

    metrics_list =  all_data.loc[:, this_field]
    X = metrics_list < 0.0
    Y = X.any(axis = 1)

    drop_tickers = all_data.ticker[Y].unique()

    idx = np.in1d(all_data.ticker, drop_tickers)
    all_data = all_data.loc[~idx, :]

    # small pct is zeros
    this_field = data_quality_by_ticker.loc[(data_quality_by_ticker.pct_zero > 0.0) & (data_quality_by_ticker.pct_zero < 0.04), ['ticker', 'indicator']]
    this_field.columns = ['ticker', 'variable']
    this_field.loc[:, 'replace'] = 1

    long_data = pd.melt(all_data, id_vars = ['ticker', 'datekey'])
    long_data = long_data.merge(this_field, how = 'left', on = ['ticker', 'variable'])

    long_data.loc[long_data['replace'] == 1, 'value'] = long_data.loc[long_data['replace'] == 1, 'value'].replace(0, np.nan)
    long_data = long_data.drop(columns = 'replace')

    fnl_data = pd.pivot_table(long_data, index = ['ticker', 'datekey'], columns = 'variable', values = 'value')

    
    job_config = bigquery.LoadJobConfig(write_disposition="WRITE_TRUNCATE")
    job = client.load_table_from_dataframe(
        fnl_data, write_table_id, job_config=job_config
    )

dimension = 'ARQ'
query_job = client.query("""
        SELECT 
        f.* EXCEPT (ev, evebit, pb,pe,ps),                                               
        FROM financials.sharadar_fundamentals f
        where f.dimension = '{}'
        order by f.ticker, f.datekey              
        """.format(dimension))
raw_data = query_job.result().to_dataframe()

write_table_id = 'financials.ARQ_fundamentals_cleaned'
clean_fundamentals(raw_data, dimension, write_table_id)

query_job = client.query("""
    CREATE OR REPLACE table financials.ARQ_fundamentals_fnl as (
    select
    q.*,
    t.assetturnover,
    t.roa,
    t.roe,
    t.roic,
    t.ros
    from financials.ARQ_fundamentals_cleaned q
    left join financials.ART_fundamentals_cleaned t on q.ticker = t.ticker and q.datekey = t.datekey            
    )
    """) 
query_job.result()


def fundamentals_transform(credentials_path, transform_map_path,dimension, read_tbl, write_tbl):
 
    os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = credentials_path

    ### gather ticker universe
    client = bigquery.Client()
    query_job = client.query("""
    SELECT distinct ticker
    FROM {}
    """.format(read_tbl))

    ticker_list = query_job.result().to_dataframe()

    sub_tickers = np.array_split(ticker_list.ticker, 20)

    # get data quality
    client = bigquery.Client()
    query_job = client.query("""
    SELECT *
    FROM financials.{}_fundamentals_data_quality
    """.format(dimension))

    data_quality = query_job.result().to_dataframe()

    ## make indicator transform map
    indicator_list = pd.read_csv(transform_map_path)

    ### clear old table
    query = ''' truncate table ''' + write_tbl
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
        FROM {} f
        inner join transformed.temp_tickers t on f.ticker = t.ticker
        order by ticker, datekey              
        """.format(read_tbl))
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
    
        job_config = bigquery.LoadJobConfig(write_disposition="WRITE_APPEND")
        job = client.load_table_from_dataframe(
                        all_metrics, write_tbl, job_config=job_config
                    )
        print('written chunk: '+ str(j))

credentials_path = 'gcp-bigquery-privatekey.json'
transform_map_path = 'field_transform_map.csv'
read_tbl = 'financials.ARQ_fundamentals_fnl'
write_tbl = 'transformed.ARQ_monthly_financials_filled'
fundamentals_transform(credentials_path, transform_map_path, dimension, read_tbl, write_tbl)
