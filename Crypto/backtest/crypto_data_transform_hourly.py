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

# px_tbl_fnl_for_hourly

query = '''
create or replace table crypto_px.px_tbl_fnl_for_hourly as (
select
id,
ts as date,
CASE WHEN row_number() OVER (PARTITION BY id order by ts) > 23
    then AVG(px) OVER (PARTITION BY id order by ts ROWS BETWEEN 23 PRECEDING AND current row) 
end AS px_avg,
CASE WHEN row_number() OVER (PARTITION BY id order by ts) > 23
    then AVG(volume) OVER (PARTITION BY id order by ts ROWS BETWEEN 23 PRECEDING AND current row) 
end AS volume,
px as px_close,
marketcap

from crypto_px.px_tbl_fnl
)
'''
query_job = client.query(query)
query_job.result()

# px_returns_byday_hourly

query = '''select * from crypto_px.top_coins_list'''
query_job = client.query(query)
top_coins_list = query_job.result().to_dataframe()

query = '''truncate table crypto_px.px_returns_byday_hourly'''
query_job = client.query(query)

for this_id in top_coins_list.id: 
    try: 
        print(this_id)
        query = '''
        insert into crypto_px.px_returns_byday_hourly (
        with tb1 as (
        select
        *,
        lag(px_close, 24) over (partition by id order by date) as px_close_l1,
        lag(px_close, 48) over (partition by id order by date) as px_close_l2,
        lag(px_close, 72) over (partition by id order by date) as px_close_l3,
        lag(px_close, 120) over (partition by id order by date) as px_close_l5,
        lag(px_close, 240) over (partition by id order by date) as px_close_l10,
        lag(px_close, 672) over (partition by id order by date) as px_close_l28,
        lag(px_close, 1344) over (partition by id order by date) as px_close_l56,
        
        lag(px_avg, 24) over (partition by id order by date) as px_avg_l1,
        lag(px_avg, 48) over (partition by id order by date) as px_avg_l2,
        lag(px_avg, 72) over (partition by id order by date) as px_avg_l3,
        lag(px_avg, 120) over (partition by id order by date) as px_avg_l5,
        lag(px_avg, 240) over (partition by id order by date) as px_avg_l10,
        lag(px_avg, 672) over (partition by id order by date) as px_avg_l28,
        lag(px_avg, 1344) over (partition by id order by date) as px_avg_l56,

        lag(volume, 24) over (partition by id order by date) as volume_l1,
        lag(volume, 48) over (partition by id order by date) as volume_l2,
        lag(volume, 72) over (partition by id order by date) as volume_l3,
        lag(volume, 120) over (partition by id order by date) as volume_l5,
        lag(volume, 240) over (partition by id order by date) as volume_l10,
        lag(volume, 672) over (partition by id order by date) as volume_l28,
        lag(volume, 1344) over (partition by id order by date) as volume_l56

        from crypto_px.px_tbl_fnl_for_hourly
        where id = '{coin_id}'
        ),
        returns_tbl as (
        select
            id,
            date,
            px_close,
            px_avg,
            marketcap,
            volume,

            CASE WHEN row_number() OVER (PARTITION BY id order by date) > 71
            then AVG(px_close) OVER (PARTITION BY id order by date ROWS BETWEEN 71 PRECEDING AND current row) 
            end AS px_close_avg_3d,
            CASE WHEN row_number() OVER (PARTITION BY id order by date) > 119  
            then AVG(px_close) OVER (PARTITION BY id order by date ROWS BETWEEN 119 PRECEDING AND current row) 
            end AS px_close_avg_5d,
            CASE WHEN row_number() OVER (PARTITION BY id order by date) > 239
            then AVG(px_close) OVER (PARTITION BY id order by date ROWS BETWEEN 239 PRECEDING AND current row) 
            end AS px_close_avg_10d,
            CASE WHEN row_number() OVER (PARTITION BY id order by date) > 479
            then AVG(px_close) OVER (PARTITION BY id order by date ROWS BETWEEN 479 PRECEDING AND current row) 
            end AS px_close_avg_20d,
            CASE WHEN row_number() OVER (PARTITION BY id order by date) > 671
            then AVG(px_close) OVER (PARTITION BY id order by date ROWS BETWEEN 671 PRECEDING AND current row) 
            end AS px_close_avg_28d,
            CASE WHEN row_number() OVER (PARTITION BY id order by date) > 1343
            then AVG(px_close) OVER (PARTITION BY id order by date ROWS BETWEEN 1343 PRECEDING AND current row) 
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
        '''.format(coin_id = this_id)

        query_job = client.query(query)
        query_job.result()
    except Exception as e:
        print(str(e))

## px_technicals
query = '''
create or replace table crypto_px.px_technicals_byday_hourly as (
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

from crypto_px.px_returns_byday_hourly 
)
select
id,
date,

lead(px_pct_1d) over (partition by id order by date) as next_day_return,

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
CASE WHEN row_number() OVER (PARTITION BY id order by date) > 167
  then avg(up_day) over (partition by id order by date ROWS BETWEEN 167 PRECEDING AND current row ) / 
       avg(down_day) over (partition by id order by date ROWS BETWEEN 167 PRECEDING AND current row ) 
  end as rsi_7d,
CASE WHEN row_number() OVER (PARTITION BY id order by date) > 335
  then avg(up_day) over (partition by id order by date ROWS BETWEEN 335 PRECEDING AND current row ) / 
       avg(down_day) over (partition by id order by date ROWS BETWEEN 335 PRECEDING AND current row ) 
  end as rsi_14d,
CASE WHEN row_number() OVER (PARTITION BY id order by date) > 671
  then avg(up_day) over (partition by id order by date ROWS BETWEEN 671 PRECEDING AND current row ) / 
       avg(down_day) over (partition by id order by date ROWS BETWEEN 671 PRECEDING AND current row ) 
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


### train_data_daily
query = ''' 
create or replace table `crypto_px.top_coins_train_data_byday_hourly` as (
select
a.*,
b.* except(id, date, px_pct_3d, px_pct_5d,px_pct_10d),
c.* except(id, date)
from crypto_px.px_returns_byday_hourly a
inner join `crypto_px.top_coins_list` s on a.id = s.id
left join crypto_px.px_technicals_byday_hourly c on a.id = c.id and a.date = c.date 
left join crypto_px.top_coin_marketcap_rank d on a.id = d.id and date(a.date) = d.date
where d.mcap_pct_rank <= {marketcap} and d.volume_pct_rank <= {volume}
)
'''.format(marketcap = .2, volume = .25)
query_job = client.query(query)
query_job.result()


### train_data_ranked
query = '''
create or replace table crypto_px.top_coins_train_data_byday_hourly_ranked as (
select
id
, date
, case when px_avg_pct_1d is not null then rank() over (partition by date order by px_avg_pct_1d desc) end as px_avg_pct_1d_rank
, case when px_avg_pct_2d is not null then rank() over (partition by date order by px_avg_pct_2d desc) end as px_avg_pct_2d_rank
, case when px_avg_pct_3d is not null then rank() over (partition by date order by px_avg_pct_3d desc) end as px_avg_pct_3d_rank
, case when px_avg_pct_5d is not null then rank() over (partition by date order by px_avg_pct_5d desc) end as px_avg_pct_5d_rank
, case when px_avg_pct_10d is not null then rank() over (partition by date order by px_avg_pct_10d desc) end as px_avg_pct_10d_rank
, case when px_avg_pct_28d is not null then rank() over (partition by date order by px_avg_pct_28d desc) end as px_avg_pct_28d_rank
, case when px_avg_pct_56d is not null then rank() over (partition by date order by px_avg_pct_56d desc) end as px_avg_pct_56d_rank
, case when volume_pct_1d is not null then rank() over (partition by date order by volume_pct_1d desc) end as volume_pct_1d_rank
, case when volume_pct_2d is not null then rank() over (partition by date order by volume_pct_2d desc) end as volume_pct_2d_rank
, case when volume_pct_3d is not null then rank() over (partition by date order by volume_pct_3d desc) end as volume_pct_3d_rank
, case when volume_pct_5d is not null then rank() over (partition by date order by volume_pct_5d desc) end as volume_pct_5d_rank
, case when volume_pct_10d is not null then rank() over (partition by date order by volume_pct_10d desc) end as volume_pct_10d_rank
, case when volume_pct_28d is not null then rank() over (partition by date order by volume_pct_28d desc) end as volume_pct_28d_rank
, case when volume_pct_56d is not null then rank() over (partition by date order by volume_pct_56d desc) end as volume_pct_56d_rank

, case when mean_1d_over_5d is not null then rank() over (partition by date order by mean_1d_over_5d desc) end as mean_1d_over_5d_rank
, case when mean_1d_over_10d is not null then rank() over (partition by date order by mean_1d_over_10d desc) end as mean_1d_over_10d_rank
, case when mean_3d_over_28d is not null then rank() over (partition by date order by mean_3d_over_28d desc) end as mean_3d_over_28d_rank
, case when mean_5d_over_56d is not null then rank() over (partition by date order by mean_5d_over_56d desc) end as mean_5d_over_56d_rank
, case when crossover_mean_1d_over_5d is not null then rank() over (partition by date order by crossover_mean_1d_over_5d desc) end as crossover_mean_1d_over_5d_rank
, case when crossover_mean_1d_over_10d is not null then rank() over (partition by date order by crossover_mean_1d_over_10d desc) end as crossover_mean_1d_over_10d_rank
, case when crossover_mean_3d_over_28d is not null then rank() over (partition by date order by crossover_mean_3d_over_28d desc) end as crossover_mean_3d_over_28d_rank
, case when crossover_mean_5d_over_56d is not null then rank() over (partition by date order by crossover_mean_5d_over_56d desc) end as crossover_mean_5d_over_56d_rank
, case when rsi_7d is not null then rank() over (partition by date order by rsi_7d desc) end as rsi_7d_rank
, case when rsi_14d is not null then rank() over (partition by date order by rsi_14d desc) end as rsi_14d_rank
, case when rsi_28d is not null then rank() over (partition by date order by rsi_28d desc) end as rsi_28d_rank
, case when px_pct_1d_over_5d_avg is not null then rank() over (partition by date order by px_pct_1d_over_5d_avg desc) end as px_pct_1d_over_5d_avg_rank
, case when px_pct_1d_over_10d_avg is not null then rank() over (partition by date order by px_pct_1d_over_10d_avg desc) end as px_pct_1d_over_10d_avg_rank
, case when px_pct_1d_over_28d_avg is not null then rank() over (partition by date order by px_pct_1d_over_28d_avg desc) end as px_pct_1d_over_28d_avg_rank
, case when px_pct_3d_over_28d_avg is not null then rank() over (partition by date order by px_pct_3d_over_28d_avg desc) end as px_pct_3d_over_28d_avg_rank
, case when px_pct_3d_over_56d_avg is not null then rank() over (partition by date order by px_pct_3d_over_56d_avg desc) end as px_pct_3d_over_56d_avg_rank
, case when mean_reversion_1d_over_5d_avg is not null then rank() over (partition by date order by mean_reversion_1d_over_5d_avg desc) end as mean_reversion_1d_over_5d_avg_rank
, case when mean_reversion_1d_over_10d_avg is not null then rank() over (partition by date order by mean_reversion_1d_over_10d_avg desc) end as mean_reversion_1d_over_10d_avg_rank
, case when mean_reversion_1d_over_28d_avg is not null then rank() over (partition by date order by mean_reversion_1d_over_28d_avg desc) end as mean_reversion_1d_over_28d_avg_rank
, case when mean_reversion_3d_over_28d_avg is not null then rank() over (partition by date order by mean_reversion_3d_over_28d_avg desc) end as mean_reversion_3d_over_28d_avg_rank
, case when mean_reversion_3d_over_56d_avg is not null then rank() over (partition by date order by mean_reversion_3d_over_56d_avg desc) end as mean_reversion_3d_over_56d_avg_rank
from `crypto_px.top_coins_train_data_byday_hourly`
)
'''
query_job = client.query(query)
query_job.result()

### prod_ranked_combo
query = '''
create or replace table crypto_px.prod_ranked_combo_hourly_top_coins as (
with tb1 as (
select 
  a.id as ticker,
  a.date,
  a.px_avg_pct_1d_rank as var_1,
  a.px_pct_1d_over_5d_avg_rank as var_2,
  a.mean_reversion_1d_over_5d_avg_rank  as var_3
from crypto_px.top_coins_train_data_byday_hourly_ranked a  
),
tb2 as (
select
*,
(var_1 + var_2 + var_3)/3 as ComboRank
from tb1
where var_1 is not NULL and var_2 is not NULL and var_3 is not NULL
),
tb3 as (
select
*,
percent_rank() over (partition by date order by ComboRank) as percentile_rank
from tb2
)
select
t.*,
row_number() over (partition by t.date order by t.percentile_rank) as ptf_count,
p.next_day_return  
from tb3 t
left join crypto_px.top_coins_train_data_byday_hourly p on t.ticker = p.id and t.date = p.date
order by t.date desc, ptf_count
)
'''
query_job = client.query(query)
query_job.result()