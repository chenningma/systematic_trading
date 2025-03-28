import pandas as pd
pd.options.mode.chained_assignment = None
import os
from datetime import datetime, timedelta
from pandas.tseries.offsets import MonthEnd
import numpy as np
from dotenv import load_dotenv

from google.cloud import bigquery, bigquery_storage
from google.cloud.bigquery_storage import BigQueryReadClient
from google.cloud.bigquery_storage import types

parent_dir = os.path.dirname(os.path.dirname(os.getcwd()))
dotenv_path = parent_dir + '/.env'
load_dotenv(dotenv_path)

credentials_path = parent_dir + '/' +os.getenv('GCP_CREDENTIALS_PATH')
os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = credentials_path
client = bigquery.Client()


# create new table
def create_output_table(output_table):
    query = f"""
        CREATE or REPLACE TABLE {output_table} (
            ticker STRING,
            date_start DATETIME,
            date_end DATETIME,
            field STRING,
            value FLOAT64
        )
        """
    query_job = client.query(
        query
    )
    query_job.result()

# Aggregate functions
def insert_aggregate_avg_by_period(input_table,output_table,price_field, period, start_date, new_field_name=None):

    if period == 'WEEK':
        date_end_statement = f'DATE_ADD(DATE_TRUNC(date, {period}), INTERVAL 5 DAY) AS date_end'
    elif period == 'MONTH':
        date_end_statement = f'LAST_DAY(date) AS date_end'

    if new_field_name is not None:
        fieldname = new_field_name
    else:
        fieldname = f'{price_field}_avg'

    query = f"""
    INSERT INTO {output_table} (
        SELECT
            ticker,
            DATE_TRUNC(date, {period}(MONDAY)) AS date_start,
            {date_end_statement},
            '{fieldname}' AS field,
            AVG({price_field}) AS value
        FROM
            {input_table}
        WHERE
            date >= '{start_date}'
        GROUP BY
            ticker,
            date_start,
            date_end
        ORDER BY
            date_start
    )
    """

    query_job = client.query(
        query
    )
    query_job.result()

def insert_aggregate_close_by_period(input_table, output_table,price_field, period, start_date, new_field_name=None):

    if period == 'WEEK':
        date_end_statement = f'DATE_ADD(DATE_TRUNC(date, {period}), INTERVAL 5 DAY) AS date_end'
    elif period == 'MONTH':
        date_end_statement = f'LAST_DAY(date) AS date_end'

    if new_field_name is not None:
        fieldname = new_field_name
    else:
        fieldname = f'{price_field}_close'
    
    query = f'''
    INSERT INTO {output_table} (
        SELECT
            ticker,
            DATE_TRUNC(date, {period}(MONDAY)) AS date_start,
            {date_end_statement},
            '{fieldname}' AS field,
            ARRAY_AGG({price_field} ORDER BY date DESC LIMIT 1)[OFFSET(0)] AS value
        FROM
           {input_table}
        WHERE
            date >= '{start_date}'
        GROUP BY
            ticker,
            date_start,
            date_end
        ORDER BY
            date_start
    )
    '''
    query_job = client.query(
        query
    )
    query_job.result()

# transformation functions

def insert_pct_change(input_table, output_table, transform_field, intervals, interval_unit):

    for interval in intervals:
        query = f'''
            INSERT INTO {output_table} (
            SELECT
                ticker,
                date_start,
                date_end,
                "{transform_field}_pct_{interval}{interval_unit}" AS field,
            (value / nullif(LAG(value, {interval}) OVER (PARTITION BY ticker ORDER BY date_start), 0) - 1) * 100 AS value
            FROM
                {input_table}
            WHERE 
                field = '{transform_field}'
            ORDER BY
                ticker,
                date_start
            )
        '''
        query_job = client.query(
            query
        )
        query_job.result()

        #perform_insert(output_table)

def insert_interval_mean(input_table, output_table, transform_field, intervals, interval_unit, new_field_name=None):

    for interval in intervals:
        prev_rows = interval - 1

        if new_field_name is not None:
            fieldname = f'{new_field_name}_{interval}{interval_unit}'
        else:
            fieldname = f'{transform_field}_mean_{interval}{interval_unit}'

        # Calculate mean over interval  
        query = f'''
            INSERT INTO {output_table} (
            SELECT
                ticker,
                date_start,
                date_end,
                "{fieldname}" AS field,

				CASE
				WHEN COUNT(*) OVER (PARTITION BY ticker ORDER BY date_end) >= {interval}
				THEN avg(value) over (partition by ticker order by date_end rows between {prev_rows} preceding and current row)
				ELSE NULL  
				END AS value
					
			FROM
                {input_table}
            WHERE 
                field = '{transform_field}'
            ORDER BY
                ticker,
                date_start
            )
        '''
        query_job = client.query(
            query
        )
        query_job.result()

        #perform_insert(output_table)

def insert_interval_vol(input_table, output_table, transform_field, intervals, interval_unit, new_field_name=None):

    for interval in intervals:
        prev_rows = interval - 1

        if new_field_name is not None:
            fieldname = f'{new_field_name}_{interval}{interval_unit}'
        else:
            fieldname = f'{transform_field}_vol_{interval}{interval_unit}'

        query = f'''
            INSERT INTO {output_table} (
            SELECT
                ticker,
                date_start,
                date_end,
                "{fieldname}" AS field,
            stddev(value) over (partition by ticker order by date_end rows between {prev_rows} preceding and current row) as value,
            FROM
                {input_table}
            WHERE 
                field = '{transform_field}'
            ORDER BY
                ticker,
                date_start
            )
        '''
        query_job = client.query(
            query
        )
        query_job.result()

        #perform_insert(output_table)

def insert_ratio(input_table, output_table, transform_field1, transform_field2, new_field_name):

    query = f'''
        INSERT INTO {output_table} (
        SELECT
            a.ticker,
            a.date_start,
            a.date_end,
            "{new_field_name}" AS field,
        a.value / nullif(b.value,0) as value,
        FROM
           (select * from {input_table} where field = '{transform_field1}') a 
        LEFT JOIN (select * from {input_table} where field = '{transform_field2}') b 
			on a.ticker = b.ticker and a.date_end = b.date_end
        ORDER BY
            a.ticker,
            a.date_start
        )
    '''
    query_job = client.query(
        query
    )
    query_job.result()

def insert_lags(input_table, output_table, transform_field, lags, interval_unit):

    for interval in lags:

        query = f'''
            INSERT INTO {output_table} (
            SELECT
                ticker,
                date_start,
                date_end,
                "{transform_field}_l{interval}{interval_unit}" AS field,
            lag(value, {interval}) over (partition by ticker order by date_end) as value,
            FROM
                {input_table}
            WHERE 
                field = '{transform_field}'
            ORDER BY
                ticker,
                date_start
            )
        '''
        query_job = client.query(
            query
        )
        query_job.result()

        #perform_insert(output_table)

def insert_diff(input_table, output_table, transform_field, intervals, interval_unit):

    for interval in intervals:
       
        query = f'''
            INSERT INTO {output_table} (
            SELECT
                ticker,
                date_start,
                date_end,
                "{transform_field}_diff_{interval}{interval_unit}" AS field,
            value - (lag(value, {interval}) over (partition by ticker order by date_end)) as value,
            FROM
                {input_table}
            WHERE 
                field = '{transform_field}'
            ORDER BY
                ticker,
                date_start
            )
        '''
        query_job = client.query(
            query
        )
        query_job.result()

        #perform_insert(output_table)

def insert_pct_rank(input_table, output_table, transform_field):

    query = f'''
        INSERT INTO {output_table} (
        SELECT
            ticker,
            date_start,
            date_end,
            "{transform_field}_pct_rank" AS field,
            percent_rank() over (partition by date_start order by value desc) as value
        FROM
            {input_table}
        WHERE 
            field = '{transform_field}'
        ORDER BY
            ticker,
            date_start
        )
    '''
    query_job = client.query(
        query
    )
    query_job.result()

    #perform_insert(output_table)

def insert_quintile_rank(input_table, output_table, transform_field):

    query = f'''
        INSERT INTO {output_table} (
        SELECT
            ticker,
            date_start,
            date_end,
            "{transform_field}_quintile_rank" AS field,
            case when value is null then NULL
            else ntile(5) over (partition by date_start order by value NULLS FIRST) end as value
        FROM
            {input_table}
        WHERE 
            field = '{transform_field}' and value is not null
        ORDER BY
            ticker,
            date_start
        )
    '''
    query_job = client.query(
        query
    )
    query_job.result()

    #perform_insert(output_table)

# Table Operations
def pivot_table(input_table, output_table):
	fields = client.query(f'select distinct field from {input_table}').result().to_dataframe().field.tolist()
	field_cases = ',\n        '.join([f"MAX(CASE WHEN field = '{field}' THEN value END) AS {field}" for field in fields])

	query = f'''
		CREATE OR REPLACE TABLE {output_table} AS (
		SELECT
			ticker,
			date_start,
			date_end,
			{field_cases}
		FROM
			{input_table}
		WHERE
			value is not null
		GROUP BY
			ticker,
			date_start,
			date_end
		ORDER BY
			date_start 
		)
	'''
	query_job = client.query(
		query
	)
	query_job.result()



time_start = datetime.now()

# Create Staging Table
output_table = 'stage.weekly_levels'
create_output_table(output_table)

# constants
period = 'WEEK'
start_date = '2014-12-31'

# Prices
input_table = 'prices.px'
price_field = 'closeadj'
new_field_name = 'px_close'
insert_aggregate_close_by_period(input_table, output_table,price_field, period, start_date, new_field_name)

price_field = 'closeadj'
new_field_name = 'px_avg'
insert_aggregate_avg_by_period(input_table, output_table,price_field, period, start_date, new_field_name)

# Volume
price_field = 'volume'
new_field_name = 'volume_avg'
insert_aggregate_avg_by_period(input_table, output_table,price_field, period, start_date, new_field_name )

# Marketcap
input_table = 'prices.daily_valuations'
price_field = 'marketcap'
new_field_name = 'marketcap'
insert_aggregate_avg_by_period(input_table, output_table,price_field, period, start_date, new_field_name )

# Pivot Table
### Pivot Table

input_table = 'stage.weekly_levels'
output_table = 'transformed.weekly_levels'
pivot_table(input_table, output_table)

print('levels data done')

### First Trading Date

query = '''
create or replace table transformed.first_trading_date as (
select
ticker,
min(date) as first_date
from prices.px
where volume > 0
group by ticker
)
'''
query_job = client.query(
    query
)
query_job.result()

### rough filter Universe
query = '''
create or replace table stage.weekly_levels_2bn as(
with tickers as (
select 
distinct ticker 
from `transformed.weekly_levels`
where marketcap > 2000 and date_start > '2015-01-01'
)
select
a.*
from stage.weekly_levels a
inner join tickers b on a.ticker = b.ticker
)
'''
query_job = client.query(
    query
)
query_job.result()

## check nulls
query = '''
select 
a.*
from stage.weekly_levels_2bn a
left join transformed.first_trading_date b on a.ticker = b.ticker and a.date_start >= date_add(b.first_date, interval 90 day)
where a.value is null
'''
nulls = client.query(query).to_dataframe()
tickers = nulls.ticker.unique()

if len(nulls) > 0:
	print(f'{len(tickers)} tickers have nulls')

### Calculate 1st differences data
input_table = 'stage.weekly_levels_2bn'
output_table = 'stage.weekly_first_diff'
create_output_table(output_table)

### PCT Change
interval_unit = 'w'
transform_field = 'px_close'
intervals = [1,4, 12, 24, 36, 52]
insert_pct_change(input_table, output_table, transform_field, intervals, interval_unit)

# PX Avg
transform_field = 'px_avg'
insert_pct_change(input_table, output_table, transform_field, intervals, interval_unit)

# Volume Avg
transform_field = 'volume_avg'
insert_pct_change(input_table, output_table, transform_field, intervals, interval_unit)

transform_field = 'volume_avg'
intervals = [4, 24, 36]
insert_interval_mean(input_table, output_table, transform_field, intervals, interval_unit)

print('first differences done')

### Check Price Return 1W
def check_one():
	query = '''
	select 
	a.*,
	b.closeadj as px_start,
	c.closeadj as px_end,
	(c.closeadj / b.closeadj -1) * 100 as pct_calc,
	a.value - ((c.closeadj / b.closeadj -1) * 100) as diff
	from stage.weekly_first_diff a
	left join `prices`.`px` b on a.ticker = b.ticker and date_sub(a.date_start, interval 3 day) = b.date
	left join `prices`.`px` c on a.ticker = c.ticker and a.date_end = c.date
	where a.field = 'px_close_pct_1w' and a.ticker = 'CVNA'
	order by date_start desc
	'''
	query_job = client.query(
		query
	)
	check = query_job.result().to_dataframe()
	return check

### Save to Transformed Schema
input_table = 'stage.weekly_first_diff'
output_table = 'transformed.weekly_px_return'
pivot_table(input_table, output_table)

### Calculate 2nd differences data - Momentum

input_table = 'stage.weekly_first_diff'
output_table = 'stage.weekly_second_diff'
create_output_table(output_table)

### Momentum - mean over weekly returns
# px_avg_momentum_8w

transform_field = 'px_avg_pct_1w'
new_name = 'px_avg_momentum'
intervals = [4, 8, 12, 24, 36]
insert_interval_mean(input_table, output_table, transform_field, intervals, interval_unit, new_field_name=new_name)

print('momentum done')

# Volatility - stddev over weekly returns

transform_field = 'px_avg_pct_1w'
new_name = 'px_avg_vol'
intervals = [12, 24, 36, 52, 72]
insert_interval_vol(input_table, output_table, transform_field, intervals, interval_unit, new_field_name=new_name)

print('volatility done')

query = '''
create or replace table stage.weekly_all_diff as (

(select * from stage.weekly_first_diff
where value is not null)

union all

(select * from stage.weekly_second_diff
where value is not null)
)
'''
query_job = client.query(
    query
)
query_job.result()

### Third Differences 

input_table = 'stage.weekly_all_diff'
output_table = 'stage.weekly_third_diff'
create_output_table(output_table)

### Ratios 

# Vol-Adjusted Returns
transform_field1 = 'px_avg_pct_4w'
transform_field2 = 'px_avg_vol_24w'
new_field_name = 'px_avg_pct_4w_x_vol_24w'
insert_ratio(input_table, output_table, transform_field1, transform_field2, new_field_name)

transform_field1 = 'px_avg_pct_24w'
transform_field2 = 'px_avg_vol_52w'
new_field_name = 'px_avg_pct_24w_x_vol_52w'
insert_ratio(input_table, output_table, transform_field1, transform_field2, new_field_name)

# Vol-Adjusted Momentum
transform_field1 = 'px_avg_momentum_8w'
transform_field2 = 'px_avg_vol_24w'
new_field_name = 'px_momentum_8w_x_vol_24w'
insert_ratio(input_table, output_table, transform_field1, transform_field2, new_field_name)

transform_field1 = 'px_avg_momentum_12w'
transform_field2 = 'px_avg_vol_52w'
new_field_name = 'px_momentum_12w_x_vol_36w'
insert_ratio(input_table, output_table, transform_field1, transform_field2, new_field_name)

transform_field1 = 'px_avg_momentum_24w'
transform_field2 = 'px_avg_vol_52w'
new_field_name = 'px_momentum_24w_x_vol_52w'
insert_ratio(input_table, output_table, transform_field1, transform_field2, new_field_name)

print('ratios done')

### Lags

transform_field = 'px_avg_momentum_12w'
lags = [12]
insert_lags(input_table, output_table, transform_field, lags, interval_unit)

print('lags done')
### Momentum Change
transform_field = 'px_avg_momentum_12w'
intervals = [4, 12]
insert_diff(input_table, output_table, transform_field, intervals, interval_unit)

transform_field = 'px_avg_momentum_24w'
intervals = [4, 12]
insert_diff(input_table, output_table, transform_field, intervals, interval_unit)

transform_field = 'px_momentum_8w_x_vol_24w'
intervals = [4, 12]
insert_diff(input_table, output_table, transform_field, intervals, interval_unit)

transform_field = 'px_momentum_8w_x_vol_24w'
intervals = [4, 12]
insert_diff(input_table, output_table, transform_field, intervals, interval_unit)

print('momentum change done')

query = '''
insert into stage.weekly_all_diff (

select * from stage.weekly_third_diff
where value is not null)

'''
query_job = client.query(
    query
)
query_job.result()

### Pivot Table
input_table = 'stage.weekly_all_diff'	
output_table = 'transformed.weekly_technicals'
pivot_table(input_table, output_table)

print('all technicals done')
### Marketcap and Volume Rank

output_table = 'transformed.weekly_marketcap_volume_rank'
create_output_table(output_table)

input_table = 'stage.weekly_levels_2bn'
transform_field = 'marketcap'
insert_pct_rank(input_table, output_table, transform_field)

input_table = 'stage.weekly_all_diff'
transform_field = 'volume_avg_mean_4w'
insert_pct_rank(input_table, output_table, transform_field)

print('marketcap and volume rank done')

### Truncate staging tables
query = '''
truncate table stage.weekly_levels;
truncate table stage.weekly_levels_2bn;
truncate table stage.weekly_first_diff;
truncate table stage.weekly_second_diff;
truncate table stage.weekly_third_diff;
'''
query_job = client.query(
    query
)
query_job.result()

print('staging tables truncated')

print(f'Time taken: {datetime.now() - time_start}')