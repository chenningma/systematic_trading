import json
import sys
import time
import nasdaqdatalink
import requests
import pandas as pd
import os
from datetime import datetime, timedelta
from dotenv import load_dotenv
from google.cloud import bigquery, bigquery_storage
from google.cloud.bigquery_storage import BigQueryReadClient
from google.cloud.bigquery_storage import types

parent_dir = os.path.dirname(os.path.dirname(os.getcwd()))
dotenv_path = parent_dir + '/.env'
load_dotenv(dotenv_path)


credentials_path = 'gcp-bigquery-privatekey.json'
os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = credentials_path
client = bigquery.Client()

nasdaqdatalink.ApiConfig.api_key = os.getenv('NASDAQ_API_KEY')

### actions
last_update = datetime.today() - timedelta(days = 2)
raw_tbl = nasdaqdatalink.get_table('SHARADAR/ACTIONS', \
     date={'gte':last_update.strftime("%Y-%m-%d")}, paginate=True)
raw_tbl = raw_tbl.reset_index(drop = True)

table_id = 'boreal-pride-417020.financials.actions_update'
job_config = bigquery.LoadJobConfig(
    #schema = [
    #bigquery.SchemaField("filingDate", bigquery.enums.SqlTypeNames.DATE),
    #bigquery.SchemaField("periodEndDate", bigquery.enums.SqlTypeNames.DATE)],
    write_disposition="WRITE_TRUNCATE",
)
job = client.load_table_from_dataframe(
                raw_tbl, table_id, job_config=job_config
            )
job.result()

query = '''
CREATE OR REPLACE table `boreal-pride-417020.financials.actions` as (
select * from financials.actions
union distinct
select * from financials.actions_update
)

'''
query_job = client.query(
    query
)  
query_job.result()

query = ''' truncate table boreal-pride-417020.financials.actions_update'''
query_job = client.query(
    query
)  
