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

### backfill prices
date_list = ['2025-01-01','2025-01-15', '2025-01-31', '2025-02-15', '2025-02-28', '2025-03-15', '2025-03-26']
for i in range(1, len(date_list)):
	px_tbl = nasdaqdatalink.get_table('SHARADAR/SEP', \
		date={'gte':date_list[i-1], 'lte':date_list[i]}, paginate=True)

	px_tbl = px_tbl.reset_index(drop = True)
	client.insert_df('raw.prices', px_tbl)

### backfill valuations
date_list = ['2025-01-01','2025-01-15', '2025-01-31', '2025-02-15', '2025-02-28', '2025-03-15', '2025-03-26']
for i in range(1, len(date_list)):
	print(date_list[i-1], date_list[i])
	px_tbl = nasdaqdatalink.get_table('SHARADAR/DAILY', \
		date={'gte':date_list[i-1], 'lte':date_list[i]}, paginate=True)

	px_tbl = px_tbl.reset_index(drop = True)
	client.insert_df('raw.valuations', px_tbl)
