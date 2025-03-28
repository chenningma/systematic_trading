import pandas as pd
pd.options.mode.chained_assignment = None
import os
from datetime import datetime, timedelta
from pandas.tseries.offsets import MonthEnd
import numpy as np

from google.cloud import bigquery, bigquery_storage
from google.cloud.bigquery_storage import BigQueryReadClient
from google.cloud.bigquery_storage import types

from dotenv import load_dotenv, find_dotenv

parent_dir = os.path.dirname(os.path.dirname(os.getcwd()))
dotenv_path = parent_dir + '/.env'
load_dotenv(dotenv_path)

credentials_path = parent_dir + '/' +os.getenv('GCP_CREDENTIALS_PATH')
os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = credentials_path
client = bigquery.Client()

def get_account_returns(account):
	query = f'''
	with daily_returns as (
	select
	a.account,
	a.as_of_date as purchase_date,
	p.date as px_date,
	a.snapshot_type,
	a.ticker,
	a.position,
	a.avgCost,
	a.position*avgCost as cost,
	p.closeadj as px,
	a.position*p.closeadj as mktval
	from monitor.ibkr_ptf_snapshot a
	left join prices.px p on a.ticker = p.ticker and FORMAT_DATE( '%Y-%m', date(a.as_of_date)) = FORMAT_DATE( '%Y-%m',p.date) 
	where account = '{account}' and snapshot_type = 'start'
	order by as_of_date, date
	),
	mtd_return as (
	select
	*,
	mktval / cost - 1 as pos_return_mtd
	from daily_returns
	)
	select
	*
	from mtd_return
	qualify row_number() over (partition by ticker, purchase_date order by px_date desc) = 1
	'''

	query_job = client.query(query)
	account_returns = query_job.result().to_dataframe()

	return account_returns

account = 'U14784949'
account_returns = get_account_returns(account)

monthly_returns = account_returns.groupby(['purchase_date'])[['cost', 'mktval']].sum()
monthly_returns['return'] = (monthly_returns['mktval']/monthly_returns['cost'] - 1) * 100


latest_date = account_returns['purchase_date'].max()
this_month_returns = account_returns.loc[account_returns['purchase_date'] == latest_date, :]