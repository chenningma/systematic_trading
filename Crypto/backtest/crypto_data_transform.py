import json
import sys
import time
import requests
import pandas as pd
import os
from datetime import datetime, timedelta

from google.cloud import bigquery, bigquery_storage
from google.cloud.bigquery_storage import BigQueryReadClient
from google.cloud.bigquery_storage import types

credentials_path = 'gcp-bigquery-privatekey.json'
os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = credentials_path
client = bigquery.Client()

## px_key_tbl
query = """
create or replace table crypto_px.px_key_tbl as (
with id_list as (
select
id,
min(DATETIME_TRUNC(timestamp, HOUR)) as first_ts
from crypto_px.px_hourly
group by 1
),
timelist as (
select
distinct
DATETIME_TRUNC(timestamp, HOUR) as ts
from crypto_px.px_hourly
)
select
a.id,
b.ts
from id_list a
left join timelist b on b.ts >= a.first_ts
)
"""
query_job = client.query(query)
query_job.result()

### crypto_px.px_hourly_filled
query = """
create or replace table crypto_px.px_hourly_filled as (
select
a.id,
a.ts,
avg(b.prices) as px,
avg(b.market_caps) as marketcap,
avg(b.total_volumes) as volume

from crypto_px.px_key_tbl a
left join crypto_px.px_hourly b on a.id = b.id and a.ts = date_trunc(b.timestamp, HOUR)
group by 1,2
order by 1,2
)
"""
query_job = client.query(query)
query_job.result()

### chunk_id
query = """ 
create or replace table crypto_px.chunk_id_tbl as (
with tb1 as (
SELECT 
   distinct id
from crypto_px.px_hourly_filled
),
tb2 as (
select
*,
row_number() over () as id_num
from tb1
)
select
*,
cast(ceil(id_num / {chunksize}) as int64) as chunk_id 
from tb2
)
""".format(chunksize = 200)
query_job = client.query(query)
query_job.result()

write_table_id = 'crypto_px.px_tbl_fnl'
max_chunk = client.query("select max(chunk_id) as max_chunk from crypto_px.chunk_id_tbl").to_dataframe().max_chunk[0]

query = '''truncate table crypto_px.px_tbl_fnl'''
query_job = client.query(query)

for i in range(1, max_chunk+1):
    query = """
    select
    b.*
    from crypto_px.chunk_id_tbl a
    left join crypto_px.px_hourly_filled b on a.id = b.id
    where a.chunk_id = {this_chunk_id}
    """.format(this_chunk_id = i)

    this_tbl = client.query(query).to_dataframe()

    ### fill nas

    #this_tbl.loc[:, ['px']] = this_tbl.loc[:, ['px']].fillna(method = 'ffill')
    this_tbl.loc[:, ['marketcap', 'volume']] = this_tbl.loc[:, ['marketcap', 'volume']].fillna(0.0)

    job_config = bigquery.LoadJobConfig(
        write_disposition="WRITE_APPEND",
    )
    job = client.load_table_from_dataframe(
        this_tbl, write_table_id, job_config=job_config
    )
    job.result()

# px_daily

query = '''
create or replace table crypto_px.px_daily as (
select
id,
date(ts) as date,
avg(px) as px_avg,
avg(volume) as volume,
max(case when EXTRACT(HOUR from ts) = 23 then px else NULL end) as px_close,
max(case when EXTRACT(HOUR from ts) = 23 then marketcap else NULL end) as marketcap
from crypto_px.px_tbl_fnl
group by 1,2
)
'''
query_job = client.query(query)
query_job.result()


### solana_marketcap_rank
query = '''
create or replace table crypto_px.solana_marketcap_rank as (
select 
v.*,
percent_rank() over (partition by v.date order by v.marketcap desc) as mcap_pct_rank,
percent_rank() over (partition by v.date order by v.volume desc) as volume_pct_rank,
from crypto_px.px_daily v
inner join crypto_px.solana_coin_id s on v.id=s.id
order by v.date, mcap_pct_rank 
)
'''
query_job = client.query(query)
query_job.result()

### px_returns_daily

query = '''
create or replace table crypto_px.px_returns_daily as (
with tb1 as (
select
  p.*,
  lag(px_close, 1) over (partition by id order by date) as px_close_l1,
  lag(px_close, 2) over (partition by id order by date) as px_close_l2,
  lag(px_close, 3) over (partition by id order by date) as px_close_l3,
  lag(px_close, 5) over (partition by id order by date) as px_close_l5,
  lag(px_close, 10) over (partition by id order by date) as px_close_l10,
  lag(px_close, 28) over (partition by id order by date) as px_close_l28,
  lag(px_close, 56) over (partition by id order by date) as px_close_l56,
  
  lag(px_avg, 1) over (partition by id order by date) as px_avg_l1,
  lag(px_avg, 2) over (partition by id order by date) as px_avg_l2,
  lag(px_avg, 3) over (partition by id order by date) as px_avg_l3,
  lag(px_avg, 5) over (partition by id order by date) as px_avg_l5,
  lag(px_avg, 10) over (partition by id order by date) as px_avg_l10,
  lag(px_avg, 28) over (partition by id order by date) as px_avg_l28,
  lag(px_avg, 56) over (partition by id order by date) as px_avg_l56,

  lag(volume, 1) over (partition by id order by date) as volume_l1,
  lag(volume, 2) over (partition by id order by date) as volume_l2,
  lag(volume, 3) over (partition by id order by date) as volume_l3,
  lag(volume, 5) over (partition by id order by date) as volume_l5,
  lag(volume, 10) over (partition by id order by date) as volume_l10,
  lag(volume, 28) over (partition by id order by date) as volume_l28,
  lag(volume, 56) over (partition by id order by date) as volume_l56

from crypto_px.px_daily p
),
returns_tbl as (
select
    id,
    date,
    px_avg,
    px_close,
    marketcap,
    volume,

    CASE WHEN row_number() OVER (PARTITION BY id order by date) > 2
      then AVG(px_close) OVER (PARTITION BY id order by date ROWS BETWEEN 2 PRECEDING AND current row) 
      end AS px_close_avg_3d,
    CASE WHEN row_number() OVER (PARTITION BY id order by date) > 4  
      then AVG(px_close) OVER (PARTITION BY id order by date ROWS BETWEEN 4 PRECEDING AND current row) 
      end AS px_close_avg_5d,
    CASE WHEN row_number() OVER (PARTITION BY id order by date) > 9
      then AVG(px_close) OVER (PARTITION BY id order by date ROWS BETWEEN 9 PRECEDING AND current row) 
      end AS px_close_avg_10d,
    CASE WHEN row_number() OVER (PARTITION BY id order by date) > 19
      then AVG(px_close) OVER (PARTITION BY id order by date ROWS BETWEEN 19 PRECEDING AND current row) 
      end AS px_close_avg_20d,
    CASE WHEN row_number() OVER (PARTITION BY id order by date) > 27
      then AVG(px_close) OVER (PARTITION BY id order by date ROWS BETWEEN 27 PRECEDING AND current row) 
      end AS px_close_avg_28d,
    CASE WHEN row_number() OVER (PARTITION BY id order by date) > 55
      then AVG(px_close) OVER (PARTITION BY id order by date ROWS BETWEEN 55 PRECEDING AND current row) 
      end AS px_close_avg_56d,

    round(px_close / nullif(px_close_l1, 0) - 1, 6) as px_pct_1d,
    round(px_close / nullif(px_close_l2, 0) - 1, 6) as px_pct_2d,
    round(px_close / nullif(px_close_l3, 0) - 1, 6) as px_pct_3d,
    round(px_close / nullif(px_close_l5, 0) - 1, 6) as px_pct_5d,
    round(px_close / nullif(px_close_l10,0) - 1, 6) as px_pct_10d,
    round(px_close / nullif(px_close_l28,0) - 1, 6) as px_pct_28d,
    round(px_close / nullif(px_close_l56,0) - 1, 6) as px_pct_56d,

    round(px_avg / nullif(px_avg_l1, 0) - 1, 6) as px_avg_pct_1d,
    round(px_avg / nullif(px_avg_l2, 0) - 1, 6) as px_avg_pct_2d,
    round(px_avg / nullif(px_avg_l3, 0) - 1, 6) as px_avg_pct_3d,
    round(px_avg / nullif(px_avg_l5, 0) - 1, 6) as px_avg_pct_5d,
    round(px_avg / nullif(px_avg_l10,0) - 1, 6) as px_avg_pct_10d,
    round(px_avg / nullif(px_avg_l28,0) - 1, 6) as px_avg_pct_28d,
    round(px_avg / nullif(px_avg_l56,0) - 1, 6) as px_avg_pct_56d,

    round(volume / nullif(volume_l1, 0) - 1, 6) as volume_pct_1d,
    round(volume / nullif(volume_l2, 0) - 1, 6) as volume_pct_2d,
    round(volume / nullif(volume_l3, 0) - 1, 6) as volume_pct_3d,
    round(volume / nullif(volume_l5, 0) - 1, 6) as volume_pct_5d,
    round(volume / nullif(volume_l10,0) - 1, 6) as volume_pct_10d,
    round(volume / nullif(volume_l28,0) - 1, 6) as volume_pct_28d,
    round(volume / nullif(volume_l56,0) - 1, 6) as volume_pct_56d

from tb1 
order by id, date 
)
select
  *
from returns_tbl
where px_pct_1d < 100 --remove outliers
)
'''
query_job = client.query(query)
query_job.result()

### px_momentum
query = '''
create or replace table crypto_px.px_momentum as (

with tb1 as (
select
id,
date,

px_pct_3d,
px_pct_5d,
px_pct_10d,

lead(px_pct_1d) over (partition by id order by date) as next_day_return,

avg(px_pct_1d) over (partition by id order by date rows between 2 preceding and current row) as px_momentum_3d,
avg(px_pct_1d) over (partition by id order by date rows between 4 preceding and current row) as px_momentum_5d,
avg(px_pct_1d) over (partition by id order by date rows between 9 preceding and current row) as px_momentum_10d,

avg(px_avg_pct_1d) over (partition by id order by date rows between 2 preceding and current row) as px_avg_momentum_3d,
avg(px_avg_pct_1d) over (partition by id order by date rows between 4 preceding and current row) as px_avg_momentum_5d,
avg(px_avg_pct_1d) over (partition by id order by date rows between 9 preceding and current row) as px_avg_momentum_10d,

stddev(px_pct_1d) over (partition by id order by date rows between 4 preceding and current row) as px_vol_5d,
stddev(px_pct_1d) over (partition by id order by date rows between 9 preceding and current row) as px_vol_10d,
stddev(px_pct_1d) over (partition by id order by date rows between 13 preceding and current row) as px_vol_14d,
stddev(px_pct_1d) over (partition by id order by date rows between 27 preceding and current row) as px_vol_28d,
stddev(px_pct_1d) over (partition by id order by date rows between 55 preceding and current row) as px_vol_56d,

stddev(px_avg_pct_1d) over (partition by id order by date rows between 4 preceding and current row) as px_avg_vol_5d,
stddev(px_avg_pct_1d) over (partition by id order by date rows between 9 preceding and current row) as px_avg_vol_10d,
stddev(px_avg_pct_1d) over (partition by id order by date rows between 13 preceding and current row) as px_avg_vol_14d,
stddev(px_avg_pct_1d) over (partition by id order by date rows between 27 preceding and current row) as px_avg_vol_28d,
stddev(px_avg_pct_1d) over (partition by id order by date rows between 55 preceding and current row) as px_avg_vol_56d
from crypto_px.px_returns_daily p
), 
tb2 as (
select
*,
px_pct_3d / nullif(px_vol_14d,0) as px_chg_3d_vol_14d,
px_pct_5d / nullif(px_vol_14d,0) as px_chg_5d_vol_14d,
px_pct_10d / nullif(px_vol_28d,0) as px_chg_10d_vol_28d,

px_momentum_3d / nullif(px_vol_28d,0) as px_momentum_3d_vol_28d,
px_momentum_5d / nullif(px_vol_28d,0) as px_momentum_5d_vol_28d,
px_momentum_10d / nullif(px_vol_56d,0) as px_momentum_10d_vol_56d,

px_avg_momentum_3d / nullif(px_avg_vol_28d,0) as px_avg_momentum_3d_vol_28d,
px_avg_momentum_5d / nullif(px_avg_vol_28d,0) as px_avg_momentum_5d_vol_28d,
px_avg_momentum_10d / nullif(px_avg_vol_56d,0) as px_avg_momentum_10d_vol_56d,

lag(px_momentum_3d, 3) over (partition by id order by date) as px_momentum_3d_l3,
lag(px_momentum_5d, 3) over (partition by id order by date) as px_momentum_5d_l3,
lag(px_momentum_10d, 3) over (partition by id order by date) as px_momentum_10d_l3,

lag(px_avg_momentum_3d, 3) over (partition by id order by date) as px_avg_momentum_3d_l3,
lag(px_avg_momentum_5d, 3) over (partition by id order by date) as px_avg_momentum_5d_l3,
lag(px_avg_momentum_10d, 3) over (partition by id order by date) as px_avg_momentum_10d_l3

from tb1 t
),
tb3 as (
select
*,
px_momentum_3d - px_momentum_3d_l3 as px_momentum_3d_chg_3d,
px_momentum_5d - px_momentum_5d_l3 as px_momentum_5d_chg_3d,
px_momentum_10d - px_momentum_10d_l3 as px_momentum_10d_chg_3d,

px_avg_momentum_3d - px_avg_momentum_3d_l3 as px_avg_momentum_3d_chg_3d,
px_avg_momentum_5d - px_avg_momentum_5d_l3 as px_avg_momentum_5d_chg_3d,
px_avg_momentum_10d - px_avg_momentum_10d_l3 as px_avg_momentum_10d_chg_3d,

lag(px_momentum_3d_vol_28d, 3) over (partition by id order by date) as px_momentum_3d_vol_28d_l3,
lag(px_momentum_5d_vol_28d, 3) over (partition by id order by date) as px_momentum_5d_vol_28d_l3,
lag(px_momentum_10d_vol_56d, 3) over (partition by id order by date) as px_momentum_10d_vol_56d_l3

from tb2 t
)

select
*,
px_momentum_3d_vol_28d - px_momentum_3d_vol_28d_l3 as px_momentum_3d_vol_28d_chg_3d,
px_momentum_5d_vol_28d - px_momentum_5d_vol_28d_l3 as px_momentum_5d_vol_28d_chg_3d,
px_momentum_10d_vol_56d - px_momentum_10d_vol_56d_l3 as px_momentum_10d_vol_56d_chg_3d

from tb3 t
)

'''
query_job = client.query(query)
query_job.result()

## px_technicals
query = '''
create or replace table crypto_px.px_technicals as (
with tb1 as (
select
*,
--STAGING: short avg over lt avg
px_close - px_close_avg_5d as mean_1d_over_5d,
px_close - px_close_avg_10d as mean_1d_over_10d,
px_close_avg_3d - px_close_avg_28d as mean_3d_over_28d,
px_close_avg_5d - px_close_avg_56d as mean_5d_over_56d,


--STAGING: RSI
case when px_pct_1d > 0 then px_pct_1d else NULL end as up_day,
case when px_pct_1d < 0 then abs(px_pct_1d) else NULL end as down_day,

from crypto_px.px_returns_daily 
)
select
id,
date,

--short avg over lt avg
mean_1d_over_5d,
mean_1d_over_10d,
mean_3d_over_28d,
mean_5d_over_56d,

--short avg over lt avg: crossover from below
case when mean_1d_over_5d > 0 then -1* mean_1d_over_5d else -999 end as crossover_mean_1d_over_5d,
case when mean_1d_over_10d > 0 then -1* mean_1d_over_10d else -999 end as crossover_mean_1d_over_10d,
case when mean_3d_over_28d > 0 then -1* mean_3d_over_28d else -999 end as crossover_mean_3d_over_28d,
case when mean_5d_over_56d > 0 then -1* mean_5d_over_56d else -999 end as crossover_mean_5d_over_56d,

--RSI
CASE WHEN row_number() OVER (PARTITION BY id order by date) > 6
  then avg(up_day) over (partition by id order by date ROWS BETWEEN 6 PRECEDING AND current row ) / 
       avg(down_day) over (partition by id order by date ROWS BETWEEN 6 PRECEDING AND current row ) 
  end as rsi_7d,
CASE WHEN row_number() OVER (PARTITION BY id order by date) > 13
  then avg(up_day) over (partition by id order by date ROWS BETWEEN 13 PRECEDING AND current row ) / 
       avg(down_day) over (partition by id order by date ROWS BETWEEN 13 PRECEDING AND current row ) 
  end as rsi_14d,
CASE WHEN row_number() OVER (PARTITION BY id order by date) > 27
  then avg(up_day) over (partition by id order by date ROWS BETWEEN 27 PRECEDING AND current row ) / 
       avg(down_day) over (partition by id order by date ROWS BETWEEN 27 PRECEDING AND current row ) 
  end as rsi_28d,

--percent from lt avg trend
px_avg_pct_1d / nullif(px_close_avg_5d, 0) - 1 as px_pct_1d_over_5d_avg,
px_avg_pct_1d / nullif(px_close_avg_10d, 0) - 1 as px_pct_1d_over_10d_avg,
px_avg_pct_1d / nullif(px_close_avg_28d, 0) - 1 as px_pct_1d_over_28d_avg,
px_avg_pct_3d / nullif(px_close_avg_28d, 0) - 1 as px_pct_3d_over_28d_avg,
px_avg_pct_5d / nullif(px_close_avg_56d, 0) - 1 as px_pct_3d_over_56d_avg,

--percent from lt avg mean-reversion
px_close_avg_5d / nullif(px_avg_pct_1d, 0) - 1 as mean_reversion_1d_over_5d_avg,
px_close_avg_10d / nullif(px_avg_pct_1d, 0) - 1 as mean_reversion_1d_over_10d_avg,
px_close_avg_28d / nullif(px_avg_pct_1d, 0) - 1 as mean_reversion_1d_over_28d_avg,
px_close_avg_28d / nullif(px_avg_pct_3d, 0) - 1 as mean_reversion_3d_over_28d_avg,
px_close_avg_56d / nullif(px_avg_pct_5d, 0) - 1 as mean_reversion_3d_over_56d_avg

from tb1
)
'''
query_job = client.query(query)
query_job.result()

#### HOURLY 

