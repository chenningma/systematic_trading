import sys
import time
import nasdaqdatalink
import pandas as pd
import os
from datetime import datetime, timedelta
from dotenv import load_dotenv
import clickhouse_connect

parent_dir = os.path.dirname(os.path.dirname(os.getcwd()))
dotenv_path = parent_dir + '/.env'
load_dotenv(dotenv_path)

client = clickhouse_connect.get_client(
        host='fcnre0n2gm.us-central1.gcp.clickhouse.cloud',
        user='default',
        password=os.getenv('CLICKHOUSE_PASSWORD'),
        secure=True
    )

nasdaqdatalink.ApiConfig.api_key = os.getenv('NASDAQ_API_KEY')

start_day = 1

### fundamentals
last_update = datetime.today() - timedelta(days = start_day)
last_fundamental_dt = datetime.today() - timedelta(days = 365)
fundamentals = nasdaqdatalink.get_table('SHARADAR/SF1', \
    datekey={'gte':last_fundamental_dt.strftime("%Y-%m-%d")}, lastupdated={'gte':last_update.strftime("%Y-%m-%d")}, paginate=True)
fundamentals = fundamentals.reset_index(drop = True)

# Insert new records
client.insert_df('raw.financials_update', fundamentals)

# Delete existing records that will be updated
delete_query = '''
DELETE from raw.financials 
WHERE (ticker, datekey, dimension) IN 
    (SELECT ticker, datekey, dimension FROM raw.financials_update)
'''
client.command(delete_query)

# Insert new records
client.insert_df('raw.financials', fundamentals)
client.command('truncate table raw.financials_update')



### prices
for i in range(start_day, 0, -1):
    print(i)
    last_update = datetime.today() - timedelta(days = i)
    if last_update.weekday() > 4: # Mon-Fri are 0-4
        continue

    px_tbl = nasdaqdatalink.get_table('SHARADAR/SEP', \
        lastupdated=last_update.strftime("%Y-%m-%d"), paginate=True)

    px_tbl = px_tbl.reset_index(drop = True)

    client.insert_df('raw.prices_update', px_tbl)

    # Delete existing records that will be updated
    delete_query = '''
    DELETE from raw.prices 
    WHERE (ticker, date) IN 
    (SELECT ticker, date FROM raw.prices_update)
    '''
    client.command(delete_query)

    # Insert new records
    client.insert_df('raw.prices', px_tbl)
    client.command('truncate table raw.prices_update')


### daily valuations
for i in range(start_day, 0, -1):
    print(i)
    last_update = datetime.today() - timedelta(days = i)

    if last_update.weekday() > 4: # Mon-Fri are 0-4
        continue

    valuation_tbl = nasdaqdatalink.get_table('SHARADAR/DAILY', \
        lastupdated=last_update.strftime("%Y-%m-%d"), paginate=True)
    valuation_tbl = valuation_tbl.reset_index(drop = True)


    client.insert_df('raw.valuations_update', valuation_tbl)

    # Delete existing records that will be updated
    delete_query = '''
    DELETE from raw.valuations 
    WHERE (ticker, date) IN 
    (SELECT ticker, date FROM raw.valuations_update)
    '''
    client.command(delete_query)

    # Insert new records
    client.insert_df('raw.valuations', valuation_tbl)
    client.command('truncate table raw.valuations_update')


print("nasdaq_data_download.py run success")

