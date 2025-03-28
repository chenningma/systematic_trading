
import pandas as pd
import numpy as np
from datetime import datetime
from pandas.tseries.offsets import MonthEnd
from google.cloud import bigquery
import os
import time 
import yfinance as yf

credentials_path = 'gcp-bigquery-privatekey.json'
os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = credentials_path
client = bigquery.Client()

def download_gics_sector(symbols, batch_size=100):
    data = pd.DataFrame()
    info_col = ['city', 'country', 'website',
       'industry', 'sector']

    for i in range(0, len(symbols), batch_size):
        batch_symbols = symbols[i:i + batch_size]
        batch_data = pd.DataFrame()
        for symbol in batch_symbols:
            try:
                stock = yf.Ticker(symbol)
                info = pd.DataFrame([stock.info])
                info_select = info[info_col].iloc[0]
                if 'fullTimeEmployees' in info.columns:
                    info_select['fullTimeEmployees'] = info['fullTimeEmployees'].iloc[0]
                else:
                    info_select['fullTimeEmployees'] = np.nan
                info_select['symbol'] = symbol
                batch_data = pd.concat([batch_data, pd.DataFrame([info_select])])
            except Exception as e:
                print(f"Error retrieving data for {symbol}: {e}")
        
        # Save the batch data to BigQuery
        table_id = 'boreal-pride-417020.prices.info_table'
        job_config = bigquery.LoadJobConfig(write_disposition="WRITE_APPEND")
        job = client.load_table_from_dataframe(batch_data, table_id, job_config=job_config)
        job.result()
        print(f"Batch {i // batch_size + 1} saved to BigQuery")
        
        # Wait for 5 seconds after each batch
        time.sleep(10)

    return data

last_month_end = (datetime.now() - MonthEnd(1)).strftime('%Y-%m-%d')
query = f'''
select 
ticker as symbol
from transformed.marketcap_volume_rank
where date = '{last_month_end}'
order by mcap_pct_rank
'''

query_job = client.query(query)
coin_list = query_job.result().to_dataframe()

# Example usage
symbols = coin_list['symbol'].tolist()
download_gics_sector(symbols, batch_size=50)

print("All GICS sector data saved to BigQuery table")
