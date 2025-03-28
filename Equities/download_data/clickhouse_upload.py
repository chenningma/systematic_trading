import pandas as pd
import clickhouse_connect
import os
from dotenv import load_dotenv

parent_dir = os.path.dirname(os.path.dirname(os.getcwd()))
dotenv_path = parent_dir + '/.env'
load_dotenv(dotenv_path)

client = clickhouse_connect.get_client(
        host='fcnre0n2gm.us-central1.gcp.clickhouse.cloud',
        user='default',
        password=os.getenv('CLICKHOUSE_PASSWORD'),
        secure=True
    )

client.command('''
CREATE TABLE IF NOT EXISTS raw.prices
(
    ticker String,
    date DateTime,
    open Float64,
    high Float64,
    low Float64,
    close Float64,
    volume Float64,
    closeadj Float64,
    closeunadj Float64,
    lastupdated DateTime
)
ENGINE = MergeTree()
ORDER BY (date, ticker)
''')

file_list = ['historical_px_2005','historical_px_2010', 'historical_px_2015','historical_px_2020','historical_px_2025']

for filename in file_list:
	try:
		print(filename)
		file = '/Users/gracema/nasdaq_data_backup/' + filename
		data_df = pd.read_parquet(file)
		data_df = data_df.dropna(subset = ['ticker', 'date'], how = 'any')

		client.insert_df(table='prices', df=data_df)
	except Exception as e:
		print(e)


client.command('''
CREATE TABLE IF NOT EXISTS raw.valuations
(
    ticker String,
    date DateTime,
	lastupdated DateTime,
    ev Float64,
    evebit Float64,
    evebitda Float64,
    marketcap Float64,
    pb Float64,
    pe Float64,
    ps Float64
)
ENGINE = MergeTree()
ORDER BY (date, ticker)
''')



file_list = ['historical_valuations_2005', 'historical_valuations_2010', 'historical_valuations_2015','historical_valuations_2020','historical_valuations_2025']

for filename in file_list:
	try:
		print(filename)
		file = '/Users/gracema/nasdaq_data_backup/' + filename
		data_df = pd.read_parquet(file)
		data_df = data_df.dropna(subset = ['ticker', 'date'], how = 'any')

		client.insert_df(table='raw.valuations', df=data_df)
	except Exception as e:
		print(e)


client.command('''
CREATE TABLE IF NOT EXISTS raw.financials
(
ticker String,
dimension String,
calendardate DateTime,
datekey DateTime,
reportperiod DateTime,
fiscalperiod String,
lastupdated DateTime,
accoci Float64,
assets Float64,
assetsavg Float64,
assetsc Float64,
assetsnc Float64,
assetturnover Float64,
bvps Float64,
capex Float64,
cashneq Float64,
cashnequsd Float64,
cor Float64,
consolinc Float64,
currentratio Float64,
de Float64,
debt Float64,
debtc Float64,
debtnc Float64,
debtusd Float64,
deferredrev Float64,
depamor Float64,
deposits Float64,
divyield Float64,
dps Float64,
ebit Float64,
ebitda Float64,
ebitdamargin Float64,
ebitdausd Float64,
ebitusd Float64,
ebt Float64,
eps Float64,
epsdil Float64,
epsusd Float64,
equity Float64,
equityavg Float64,
equityusd Float64,
ev Float64,
evebit Float64,
evebitda Float64,
fcf Float64,
fcfps Float64,
fxusd Float64,
gp Float64,
grossmargin Float64,
intangibles Float64,
intexp Float64,
invcap Float64,
invcapavg Float64,
inventory Float64,
investments Float64,
investmentsc Float64,
investmentsnc Float64,
liabilities Float64,
liabilitiesc Float64,
liabilitiesnc Float64,
marketcap Float64,
ncf Float64,
ncfbus Float64,
ncfcommon Float64,
ncfdebt Float64,
ncfdiv Float64,
ncff Float64,
ncfi Float64,
ncfinv Float64,
ncfo Float64,
ncfx Float64,
netinc Float64,
netinccmn Float64,
netinccmnusd Float64,
netincdis Float64,
netincnci Float64,
netmargin Float64,
opex Float64,
opinc Float64,
payables Float64,
payoutratio Float64,
pb Float64,
pe Float64,
pe1 Float64,
ppnenet Float64,
prefdivis Float64,
price Float64,
ps Float64,
ps1 Float64,
receivables Float64,
retearn Float64,
revenue Float64,
revenueusd Float64,
rnd Float64,
roa Float64,
roe Float64,
roic Float64,
ros Float64,
sbcomp Float64,
sgna Float64,
sharefactor Float64,
sharesbas Float64,
shareswa Float64,
shareswadil Float64,
sps Float64,
tangibles Float64,
taxassets Float64,
taxexp Float64,
taxliabilities Float64,
tbvps Float64,
workingcapital Float64
)
ENGINE = MergeTree()
ORDER BY (datekey, ticker, dimension)
''')

file_list = ['historical_financials_2005', 'historical_financials_2015','historical_financials_2025']

for filename in file_list:
	try:
		print(filename)
		file = '/Users/gracema/nasdaq_data_backup/' + filename
		data_df = pd.read_parquet(file)
		data_df = data_df.dropna(subset = ['ticker', 'datekey'], how = 'any')

		client.insert_df(table='raw.financials', df=data_df)
	except Exception as e:
		print(e)


