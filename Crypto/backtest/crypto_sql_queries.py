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

# px_daily

query = '''
create or replace table crypto_px.px_daily as (
select
id,
date(ts) as date,
avg(px) as px_avg,
avg(volume) as volume_avg,
ARRAY_AGG(px)[OFFSET(0)] as px_close,
ARRAY_AGG(marketcap)[OFFSET(0)] as marketcap_close
from
crypto_px.px_tbl_fnl
group by 1,2
)
'''

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
  lag(px_close, 30) over (partition by id order by date) as px_close_l30,
  lag(px_close, 60) over (partition by id order by date) as px_close_l60,
  
  lag(px_avg, 1) over (partition by id order by date) as px_avg_l1,
  lag(px_avg, 2) over (partition by id order by date) as px_avg_l2,
  lag(px_avg, 3) over (partition by id order by date) as px_avg_l3,
  lag(px_avg, 5) over (partition by id order by date) as px_avg_l5,
  lag(px_avg, 10) over (partition by id order by date) as px_avg_l10,
  lag(px_avg, 30) over (partition by id order by date) as px_avg_l30,
  lag(px_avg, 60) over (partition by id order by date) as px_avg_l60,

  lag(volume, 1) over (partition by id order by date) as volume_l1,
  lag(volume, 2) over (partition by id order by date) as volume_l2,
  lag(volume, 3) over (partition by id order by date) as volume_l3,
  lag(volume, 5) over (partition by id order by date) as volume_l5,
  lag(volume, 10) over (partition by id order by date) as volume_l10,
  lag(volume, 30) over (partition by id order by date) as volume_l30,
  lag(volume, 60) over (partition by id order by date) as volume_l60,
from crypto_px.px_daily p
)
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
    CASE WHEN row_number() OVER (PARTITION BY id order by date) > 29
      then AVG(px_close) OVER (PARTITION BY id order by date ROWS BETWEEN 29 PRECEDING AND current row) 
      end AS px_close_avg_30d,
    CASE WHEN row_number() OVER (PARTITION BY id order by date) > 59
      then AVG(px_close) OVER (PARTITION BY id order by date ROWS BETWEEN 59 PRECEDING AND current row) 
      end AS px_close_avg_60d,

    round(px_close / nullif(px_close_l1, 0) - 1, 6) as px_pct_1d,
    round(px_close / nullif(px_close_l2, 0) - 1, 6) as px_pct_2d,
    round(px_close / nullif(px_close_l3, 0) - 1, 6) as px_pct_3d,
    round(px_close / nullif(px_close_l5, 0) - 1, 6) as px_pct_5d,
    round(px_close / nullif(px_close_l10,0) - 1, 6) as px_pct_10d,
    round(px_close / nullif(px_close_l30,0) - 1, 6) as px_pct_30d,
    round(px_close / nullif(px_close_l60,0) - 1, 6) as px_pct_60d,

    round(px_avg / nullif(px_avg_l1, 0) - 1, 6) as px_avg_pct_1d,
    round(px_avg / nullif(px_avg_l2, 0) - 1, 6) as px_avg_pct_2d,
    round(px_avg / nullif(px_avg_l3, 0) - 1, 6) as px_avg_pct_3d,
    round(px_avg / nullif(px_avg_l5, 0) - 1, 6) as px_avg_pct_5d,
    round(px_avg / nullif(px_avg_l10,0) - 1, 6) as px_avg_pct_10d,
    round(px_avg / nullif(px_avg_l30,0) - 1, 6) as px_avg_pct_30d,
    round(px_avg / nullif(px_avg_l60,0) - 1, 6) as px_avg_pct_60d,

    round(volume / nullif(volume_l1, 0) - 1, 6) as volume_pct_1d,
    round(volume / nullif(volume_l2, 0) - 1, 6) as volume_pct_2d,
    round(volume / nullif(volume_l3, 0) - 1, 6) as volume_pct_3d,
    round(volume / nullif(volume_l5, 0) - 1, 6) as volume_pct_5d,
    round(volume / nullif(volume_l10,0) - 1, 6) as volume_pct_10d,
    round(volume / nullif(volume_l30,0) - 1, 6) as volume_pct_30d,
    round(volume / nullif(volume_l60,0) - 1, 6) as volume_pct_60d

from tb1 
order by id, date 
)
'''

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
avg(px_avg_pct_1d) over (partition by id order by date rows between 5 preceding and current row) as px_avg_momentum_5d,
avg(px_avg_pct_1d) over (partition by id order by date rows between 8 preceding and current row) as px_avg_momentum_10d,

stddev(px_pct_1d) over (partition by id order by date rows between 5 preceding and current row) as px_vol_5d,
stddev(px_pct_1d) over (partition by id order by date rows between 8 preceding and current row) as px_vol_10d,
stddev(px_pct_1d) over (partition by id order by date rows between 11 preceding and current row) as px_vol_14d,
stddev(px_pct_1d) over (partition by id order by date rows between 17 preceding and current row) as px_vol_28d,
stddev(px_pct_1d) over (partition by id order by date rows between 17 preceding and current row) as px_vol_56d,

stddev(px_avg_pct_1d) over (partition by id order by date rows between 2 preceding and current row) as px_avg_vol_5d,
stddev(px_avg_pct_1d) over (partition by id order by date rows between 5 preceding and current row) as px_avg_vol_10d,
stddev(px_avg_pct_1d) over (partition by id order by date rows between 8 preceding and current row) as px_avg_vol_14d,
stddev(px_avg_pct_1d) over (partition by id order by date rows between 11 preceding and current row) as px_avg_vol_28d,
stddev(px_avg_pct_1d) over (partition by id order by date rows between 17 preceding and current row) as px_avg_vol_56d
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


### train_data_ranked
query = '''
create or replace table crypto_px.train_data_ranked as (
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
from `crypto_px.train_data_daily`
where marketcap > {marketcap} and volume > {volume}
)
'''.format(marketcap = 1000, volume = 20000)

query = '''
create or replace table crypto_px.combo_3var_shortlist as (
(select * from crypto_px.combo_3var_backtest
order by sharp_5y desc
limit 100)

union distinct

(select * from crypto_px.combo_3var_backtest
order by return_5y_latest desc
limit 100)

union distinct

(select * from crypto_px.combo_3var_backtest
order by max_drawdown 
limit 100)
)
'''