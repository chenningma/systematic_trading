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

parent_dir = os.path.dirname(os.path.dirname(os.path.dirname(os.getcwd())))
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
def perform_upsert(output_table):
    
    query = f'''
        
        MERGE INTO {output_table} AS target
        USING stage.temp_inserts AS source
        
        ON target.ticker = source.ticker 
            AND target.date_start = source.date_start 
            AND target.field = source.field
        WHEN MATCHED THEN
            UPDATE SET
                target.value = source.value
        WHEN NOT MATCHED THEN
            INSERT (ticker, date_start, date_end, field, value)
            VALUES (source.ticker, source.date_start, source.date_end, source.field, source.value);
    '''
    query_job = client.query(
        query
    )
    query_job.result()

def upsert_pct_change(input_table, output_table, transform_field, intervals, interval_unit):

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

        #perform_upsert(output_table)

def upsert_interval_mean(input_table, output_table, transform_field, intervals, interval_unit, new_field_name=None):

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
            avg(value) over (partition by ticker order by date_end rows between {prev_rows} preceding and current row) as value,
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

        #perform_upsert(output_table)

def upsert_interval_vol(input_table, output_table, transform_field, intervals, interval_unit, new_field_name=None):

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

        #perform_upsert(output_table)

def upsert_ratio(input_table, output_table, transform_field1, transform_field2, new_field_name):

    query = f'''
        INSERT INTO {output_table} (
        SELECT
            a.ticker,
            a.date_start,
            a.date_end,
            "{new_field_name}" AS field,
        a.value / nullif(b.value,0) as value,
        FROM
            {input_table} a 
        LEFT JOIN {input_table} b on a.ticker = b.ticker and a.date_end = b.date_end
        WHERE 
            a.field = '{transform_field1}' and b.field = '{transform_field2}'
        ORDER BY
            a.ticker,
            a.date_start
        )
    '''
    query_job = client.query(
        query
    )
    query_job.result()

    #perform_upsert(output_table)

def upsert_lags(input_table, output_table, transform_field, lags, interval_unit):

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

        #perform_upsert(output_table)

def upsert_diff(input_table, output_table, transform_field, intervals, interval_unit):

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

        #perform_upsert(output_table)

def upsert_pct_rank(input_table, output_table, transform_field):

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

    #perform_upsert(output_table)

def upsert_quintile_rank(input_table, output_table, transform_field):

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

    #perform_upsert(output_table)
 
### Aggregate PX to weekly/monthly/etc

# constants
output_table = 'stage.weekly_levels'
period = 'WEEK'
start_date = '1997-12-31'
create_output_table(output_table)

# Prices
input_table = 'boreal-pride-417020.prices.px'
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

### Data Check
# check px_close is correct

query = '''
select 
a.*,
b.closeadj,
a.value - b.closeadj as diff
from stage.weekly_levels a
left join `boreal-pride-417020`.`prices`.`px` b on a.ticker = b.ticker and a.date_end = b.date
where a.field = 'px_close' and a.ticker = 'CVNA'
order by date_start desc
'''
query_job = client.query(
    query
)
check = query_job.result().to_dataframe()

### check completeness
# create a table that is the cross product of ticker and date_start where date_start is greater than the first date_start entry for each ticker
query = '''
create or replace table stage.weekly_key_tbl as (
SELECT
  t.ticker,
  t.field,
  d.date_start,
  d.date_end
FROM
  (select ticker, field, min(date_start) as first_date from `boreal-pride-417020`.`stage`.`weekly_levels` group by 1,2) t
CROSS JOIN
  (select distinct date_start, date_end from `boreal-pride-417020`.`stage`.`weekly_levels`) AS d
WHERE
  d.date_start >= t.first_date 
)
'''
query_job = client.query(
    query
)
query_job.result()

query = '''
create or replace table stage.weekly_levels_filled as (
select 
a.*,
coalesce(b.value, NULL) as value
from stage.weekly_key_tbl a
left join stage.weekly_levels b on a.ticker = b.ticker and a.field = b.field and a.date_start = b.date_start 
order by date_start
)
'''
query_job = client.query(
    query
)
query_job.result()

query = '''
create or replace table stage.ticker_chunks as (
with tb1 as (
select
    distinct ticker
from stage.weekly_levels
),
tb2 as (
select
ticker,
row_number() over () as ticker_id,
from tb1
)
select
*,
floor(ticker_id / 200) as chunk_id
from tb2
)
'''
query_job = client.query(
    query
)
query_job.result()


######## Transformations ########

def transform_technicals(input_table, output_table):
    ### Calculate 1st differences data
    ### PCT Change
    interval_unit = 'w'
    transform_field = 'px_close'
    intervals = [1,4, 12, 24, 36, 52]
    upsert_pct_change(input_table, output_table, transform_field, intervals, interval_unit)

    # PX Avg
    transform_field = 'px_avg'
    upsert_pct_change(input_table, output_table, transform_field, intervals, interval_unit)

    # Volume Avg
    transform_field = 'volume_avg'
    upsert_pct_change(input_table, output_table, transform_field, intervals, interval_unit)

    print('percent change done')

    ### Check 1st differences
    def check_one():
        query = '''
        select 
        a.*,
        b.closeadj as px_start,
        c.closeadj as px_end,
        (c.closeadj / b.closeadj -1) * 100 as pct_calc,
        a.value - ((c.closeadj / b.closeadj -1) * 100) as diff
        from stage.weekly_transformed a
        left join `boreal-pride-417020`.`prices`.`px` b on a.ticker = b.ticker and date_sub(a.date_start, interval 3 day) = b.date
        left join `boreal-pride-417020`.`prices`.`px` c on a.ticker = c.ticker and a.date_end = c.date
        where a.field = 'px_close_pct_1w' and a.ticker = 'CVNA'
        order by date_start desc
        '''
        query_job = client.query(
            query
        )
        check = query_job.result().to_dataframe()

    transform_field = 'volume_avg'
    intervals = [4, 24, 36]
    upsert_interval_mean(input_table, output_table, transform_field, intervals, interval_unit)

    ### Calculate 2nd differences data

    ### Momentum - mean over weekly returns
    # px_avg_momentum_8w

    transform_field = 'px_avg_pct_1w'
    new_name = 'px_avg_momentum'
    intervals = [4, 8, 12, 24, 36]
    upsert_interval_mean(input_table, output_table, transform_field, intervals, interval_unit, new_field_name=new_name)

    print('momentum done')

    # Volatility - stddev over weekly returns

    transform_field = 'px_avg_pct_1w'
    new_name = 'px_avg_vol'
    intervals = [12, 24, 36, 52, 72]
    upsert_interval_vol(input_table, output_table, transform_field, intervals, interval_unit, new_field_name=new_name)

    print('volatility done')

    ### Ratios 

    # Vol-Adjusted Returns
    transform_field1 = 'px_avg_pct_4w'
    transform_field2 = 'px_avg_vol_24w'
    new_field_name = 'px_avg_pct_4w_x_vol_24w'
    upsert_ratio(input_table, output_table, transform_field1, transform_field2, new_field_name)

    transform_field1 = 'px_avg_pct_24w'
    transform_field2 = 'px_avg_vol_52w'
    new_field_name = 'px_avg_pct_24w_x_vol_52w'
    upsert_ratio(input_table, output_table, transform_field1, transform_field2, new_field_name)

    # Vol-Adjusted Momentum
    transform_field1 = 'px_avg_momentum_8w'
    transform_field2 = 'px_avg_vol_24w'
    new_field_name = 'px_momentum_8w_x_vol_24w'
    upsert_ratio(input_table, output_table, transform_field1, transform_field2, new_field_name)

    transform_field1 = 'px_avg_momentum_12w'
    transform_field2 = 'px_avg_vol_52w'
    new_field_name = 'px_momentum_12w_x_vol_36w'
    upsert_ratio(input_table, output_table, transform_field1, transform_field2, new_field_name)

    transform_field1 = 'px_avg_momentum_24w'
    transform_field2 = 'px_avg_vol_52w'
    new_field_name = 'px_momentum_24w_x_vol_52w'
    upsert_ratio(input_table, output_table, transform_field1, transform_field2, new_field_name)

    print('ratios done')

    ### Lags

    transform_field = 'px_avg_momentum_12w'
    lags = [12]
    upsert_lags(input_table, output_table, transform_field, lags, interval_unit)

    print('lags done')
    ### Momentum Change
    transform_field = 'px_avg_momentum_12w'
    intervals = [4, 12]
    upsert_diff(input_table, output_table, transform_field, intervals, interval_unit)

    transform_field = 'px_avg_momentum_24w'
    intervals = [4, 12]
    upsert_diff(input_table, output_table, transform_field, intervals, interval_unit)

    transform_field = 'px_momentum_8w_x_vol_24w'
    intervals = [4, 12]
    upsert_diff(input_table, output_table, transform_field, intervals, interval_unit)

    transform_field = 'px_momentum_8w_x_vol_24w'
    intervals = [4, 12]
    upsert_diff(input_table, output_table, transform_field, intervals, interval_unit)

    print('momentum change done')

max_chunk = client.query("select max(chunk_id) as max_chunk from stage.ticker_chunks").result().to_dataframe().max_chunk.values[0]

input_table = 'stage.temp_weekly_levels_filled'
output_table = 'stage.weekly_transformed'
create_output_table(output_table)

for this_chunk in range(0, max_chunk):

    query_job = client.query(
    f"""
        create or replace table stage.temp_weekly_levels_filled as (
        select
        a.*,
        from stage.weekly_levels_filled a
        inner join stage.ticker_chunks b on a.ticker = b.ticker
        where b.chunk_id = {this_chunk} 
        and a.date_start >= '2009-01-01'
        )
    """
    )
    query_job.result()
   
    transform_technicals(input_table, output_table)
    print(f'chunk {this_chunk} done')

### Add other field
query = f'''
        INSERT INTO stage.weekly_transformed (
        SELECT
           ticker,
           date_start,
           date_end,
           field,
           value
        FROM
            stage.weekly_levels_filled a
        WHERE 
            field = 'marketcap'
        )
    '''
query_job = client.query(
    query
)
query_job.result()

###### Create Transformed Table ########
### Pivot Table
fields = client.query('select distinct field from stage.weekly_transformed').result().to_dataframe().field.tolist()
field_cases = ',\n        '.join([f"MAX(CASE WHEN field = '{field}' THEN value END) AS {field}" for field in fields])

query = f'''
    CREATE OR REPLACE TABLE transformed.weekly_technicals AS (
    SELECT
        ticker,
        date_start,
        date_end,
        {field_cases}
    FROM
        stage.weekly_transformed
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


### Marketcap and Volume Rank

output_table = 'transformed.weekly_marketcap_volume_rank'
create_output_table(output_table)

input_table = 'stage.weekly_levels_filled'
transform_field = 'marketcap'
upsert_pct_rank(input_table, output_table, transform_field)

input_table = 'stage.weekly_transformed'
transform_field = 'volume_avg_mean_4w'
upsert_pct_rank(input_table, output_table, transform_field)

### First Trading Date

query = '''
create or replace table transformed.first_trading_date as (
select
ticker,
min(date_start) as first_date
from stage.weekly_levels
where field = 'volume_avg' and value > 0
group by ticker
)
'''
query_job = client.query(
    query
)
query_job.result()





