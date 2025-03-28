import json
import sys
import time
import requests
import pandas as pd
import os
from datetime import datetime, timedelta, timezone

from google.cloud import bigquery, bigquery_storage
from google.cloud.bigquery_storage import BigQueryReadClient
from google.cloud.bigquery_storage import types

credentials_path = 'gcp-bigquery-privatekey.json'
os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = credentials_path
client = bigquery.Client()

### solana_marketcap_rank
query = '''
create or replace table crypto_px.top_coin_marketcap_rank as (
select 
v.*,
percent_rank() over (partition by v.date order by v.marketcap desc) as mcap_pct_rank,
percent_rank() over (partition by v.date order by v.volume desc) as volume_pct_rank,
from crypto_px.px_daily v
inner join crypto_px.top_coins_list s on v.id=s.id
order by v.date, mcap_pct_rank 
)
'''
query_job = client.query(query)
query_job.result()

### train_data_daily
query = ''' 
create or replace table `crypto_px.top_coins_train_data_daily` as (
select
a.*,
b.* except(id, date, px_pct_3d, px_pct_5d,px_pct_10d),
c.* except(id, date)
from crypto_px.px_returns_daily a
inner join `crypto_px.top_coins_list` s on a.id = s.id
left join crypto_px.px_momentum b on a.id = b.id and a.date = b.date
left join crypto_px.px_technicals c on a.id = c.id and a.date = c.date 
left join crypto_px.top_coin_marketcap_rank d on a.id = d.id and a.date = d.date
where d.mcap_pct_rank <= {marketcap} and d.volume_pct_rank <= {volume}
)
'''.format(marketcap = .2, volume = .25)
query_job = client.query(query)
query_job.result()


### train_data_ranked
query = '''
create or replace table crypto_px.top_coins_train_data_ranked as (
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

, case when px_momentum_3d is not null then rank() over (partition by date order by px_momentum_3d desc) end as px_momentum_3d_rank
, case when px_momentum_5d is not null then rank() over (partition by date order by px_momentum_5d desc) end as px_momentum_5d_rank
, case when px_momentum_10d is not null then rank() over (partition by date order by px_momentum_10d desc) end as px_momentum_10d_rank
, case when px_avg_momentum_3d is not null then rank() over (partition by date order by px_avg_momentum_3d desc) end as px_avg_momentum_3d_rank
, case when px_avg_momentum_5d is not null then rank() over (partition by date order by px_avg_momentum_5d desc) end as px_avg_momentum_5d_rank
, case when px_avg_momentum_10d is not null then rank() over (partition by date order by px_avg_momentum_10d desc) end as px_avg_momentum_10d_rank
, case when px_vol_5d is not null then rank() over (partition by date order by px_vol_5d desc) end as px_vol_5d_rank
, case when px_vol_10d is not null then rank() over (partition by date order by px_vol_10d desc) end as px_vol_10d_rank
, case when px_vol_14d is not null then rank() over (partition by date order by px_vol_14d desc) end as px_vol_14d_rank
, case when px_vol_28d is not null then rank() over (partition by date order by px_vol_28d desc) end as px_vol_28d_rank
, case when px_vol_56d is not null then rank() over (partition by date order by px_vol_56d desc) end as px_vol_56d_rank
, case when px_avg_vol_5d is not null then rank() over (partition by date order by px_avg_vol_5d desc) end as px_avg_vol_5d_rank
, case when px_avg_vol_10d is not null then rank() over (partition by date order by px_avg_vol_10d desc) end as px_avg_vol_10d_rank
, case when px_avg_vol_14d is not null then rank() over (partition by date order by px_avg_vol_14d desc) end as px_avg_vol_14d_rank
, case when px_avg_vol_28d is not null then rank() over (partition by date order by px_avg_vol_28d desc) end as px_avg_vol_28d_rank
, case when px_avg_vol_56d is not null then rank() over (partition by date order by px_avg_vol_56d desc) end as px_avg_vol_56d_rank
, case when px_chg_3d_vol_14d is not null then rank() over (partition by date order by px_chg_3d_vol_14d desc) end as px_chg_3d_vol_14d_rank
, case when px_chg_5d_vol_14d is not null then rank() over (partition by date order by px_chg_5d_vol_14d desc) end as px_chg_5d_vol_14d_rank
, case when px_chg_10d_vol_28d is not null then rank() over (partition by date order by px_chg_10d_vol_28d desc) end as px_chg_10d_vol_28d_rank
, case when px_momentum_3d_vol_28d is not null then rank() over (partition by date order by px_momentum_3d_vol_28d desc) end as px_momentum_3d_vol_28d_rank
, case when px_momentum_5d_vol_28d is not null then rank() over (partition by date order by px_momentum_5d_vol_28d desc) end as px_momentum_5d_vol_28d_rank
, case when px_momentum_10d_vol_56d is not null then rank() over (partition by date order by px_momentum_10d_vol_56d desc) end as px_momentum_10d_vol_56d_rank
, case when px_avg_momentum_3d_vol_28d is not null then rank() over (partition by date order by px_avg_momentum_3d_vol_28d desc) end as px_avg_momentum_3d_vol_28d_rank
, case when px_avg_momentum_5d_vol_28d is not null then rank() over (partition by date order by px_avg_momentum_5d_vol_28d desc) end as px_avg_momentum_5d_vol_28d_rank
, case when px_avg_momentum_10d_vol_56d is not null then rank() over (partition by date order by px_avg_momentum_10d_vol_56d desc) end as px_avg_momentum_10d_vol_56d_rank
, case when px_momentum_3d_l3 is not null then rank() over (partition by date order by px_momentum_3d_l3 desc) end as px_momentum_3d_l3_rank
, case when px_momentum_5d_l3 is not null then rank() over (partition by date order by px_momentum_5d_l3 desc) end as px_momentum_5d_l3_rank
, case when px_momentum_10d_l3 is not null then rank() over (partition by date order by px_momentum_10d_l3 desc) end as px_momentum_10d_l3_rank
, case when px_avg_momentum_3d_l3 is not null then rank() over (partition by date order by px_avg_momentum_3d_l3 desc) end as px_avg_momentum_3d_l3_rank
, case when px_avg_momentum_5d_l3 is not null then rank() over (partition by date order by px_avg_momentum_5d_l3 desc) end as px_avg_momentum_5d_l3_rank
, case when px_avg_momentum_10d_l3 is not null then rank() over (partition by date order by px_avg_momentum_10d_l3 desc) end as px_avg_momentum_10d_l3_rank
, case when px_momentum_3d_chg_3d is not null then rank() over (partition by date order by px_momentum_3d_chg_3d desc) end as px_momentum_3d_chg_3d_rank
, case when px_momentum_5d_chg_3d is not null then rank() over (partition by date order by px_momentum_5d_chg_3d desc) end as px_momentum_5d_chg_3d_rank
, case when px_momentum_10d_chg_3d is not null then rank() over (partition by date order by px_momentum_10d_chg_3d desc) end as px_momentum_10d_chg_3d_rank
, case when px_avg_momentum_3d_chg_3d is not null then rank() over (partition by date order by px_avg_momentum_3d_chg_3d desc) end as px_avg_momentum_3d_chg_3d_rank
, case when px_avg_momentum_5d_chg_3d is not null then rank() over (partition by date order by px_avg_momentum_5d_chg_3d desc) end as px_avg_momentum_5d_chg_3d_rank
, case when px_avg_momentum_10d_chg_3d is not null then rank() over (partition by date order by px_avg_momentum_10d_chg_3d desc) end as px_avg_momentum_10d_chg_3d_rank
, case when px_momentum_3d_vol_28d_chg_3d is not null then rank() over (partition by date order by px_momentum_3d_vol_28d_chg_3d desc) end as px_momentum_3d_vol_28d_chg_3d_rank
, case when px_momentum_5d_vol_28d_chg_3d is not null then rank() over (partition by date order by px_momentum_5d_vol_28d_chg_3d desc) end as px_momentum_5d_vol_28d_chg_3d_rank
, case when px_momentum_10d_vol_56d_chg_3d is not null then rank() over (partition by date order by px_momentum_10d_vol_56d_chg_3d desc) end as px_momentum_10d_vol_56d_chg_3d_rank
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
from `crypto_px.top_coins_train_data_daily`
)
'''
query_job = client.query(query)
query_job.result()

### prod_ranked_combo
query = '''
create or replace table crypto_px.prod_ranked_combo_top_coins as (
with tb1 as (
select 
  a.id as ticker,
  a.date,
  a.px_avg_momentum_10d_rank as var_1,
  a.px_avg_vol_10d_rank as var_2,
  a.crossover_mean_1d_over_10d_rank  as var_3
from crypto_px.top_coins_train_data_ranked a  
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
left join crypto_px.top_coins_train_data_daily p on t.ticker = p.id and t.date = p.date
order by t.date desc, ptf_count
)
'''
query_job = client.query(query)
query_job.result()

yesterday = datetime.now(timezone.utc) - timedelta(days=1)
query = '''
create or replace table crypto_px.coins_max_leverage as (
select 
a.id,
upper(t.symbol) as symbol,
b.marketcap,
a.px_vol_28d,
a.px_vol_28d * 3 as est_max_drawdown,
floor(1 / nullif((a.px_vol_28d * 3), 0)) as max_leverage
from `crypto_px.px_momentum` a
left join crypto_px.px_returns_daily b on a.id = b.id and a.date = b.date
left join crypto_px.top_coins_list t on a.id = t.id
where a.date = '{date}' 
qualify row_number() over (partition by symbol order by marketcap desc) = 1
order by b.marketcap desc
)
'''.format(date = yesterday.strftime("%Y-%m-%d"))
query_job = client.query(query)
query_job.result()