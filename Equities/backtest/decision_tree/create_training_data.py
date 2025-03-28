import pandas as pd
import os 
from google.cloud import bigquery, bigquery_storage
import numpy as np
import math 
from datetime import datetime

from google.cloud.bigquery_storage import BigQueryReadClient
from google.cloud.bigquery_storage import types
from google.cloud import bigquery_storage

credentials_path = '/Users/gracema/equities_models/gcp-bigquery-privatekey.json'
os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = credentials_path

def read_table(project_id, dataset_id, table_id):

    bqstorageclient = bigquery_storage.BigQueryReadClient()
    table = f"projects/{project_id}/datasets/{dataset_id}/tables/{table_id}"

    read_options = types.ReadSession.TableReadOptions(
        selected_fields=["country_name", "region_name"] 
    )

    parent = "projects/{}".format(project_id)

    requested_session = types.ReadSession(
        table=table,
        data_format=types.DataFormat.ARROW,
        #read_options=read_options,
    )
    read_session = bqstorageclient.create_read_session(
        parent=parent,
        read_session=requested_session,
        max_stream_count=1,
    )

    stream = read_session.streams[0] #read every stream from 0 to 3
    reader = bqstorageclient.read_rows(stream.name)

    frames = []
    for message in reader.rows().pages:
        frames.append(message.to_dataframe())
    dataframe = pd.concat(frames)
    print(dataframe.head())

    return dataframe

def create_default_model_variables():
    X_fields = pd.read_csv('train_data_lib.csv')
    X_fields = X_fields.iloc[:, 0:3]
    X_fields['added_dt'] = pd.to_datetime(X_fields['added_dt'])

    client = bigquery.Client()
    table_id = 'boreal-pride-417020.train.default_model_variables'
    job_config = bigquery.LoadJobConfig(
        schema = [ \
        bigquery.SchemaField("added_dt", bigquery.enums.SqlTypeNames.DATE)], \
        write_disposition="WRITE_TRUNCATE")
    job = client.load_table_from_dataframe(
                    X_fields, table_id, job_config=job_config
                )

# read/write config
project_id = "boreal-pride-417020"
dataset_id = "training_data"

read_table_prefix = "raw_training_data_top60pct"
postfix = datetime.today().strftime("%Y%m")
read_table_id = read_table_prefix + '_' + postfix

write_table_id = dataset_id+'.'+ read_table_prefix[4:] + '_' + postfix

raw_data = read_table(project_id, dataset_id, read_table_id)

### create default_model_variables
create_default_model_variables()

# Get default variables
client = bigquery.Client()
query_job = client.query("""
   SELECT distinct variable
   FROM train.default_model_variables
   where incl = 1                     
   """)

incl = query_job.result().to_dataframe()
incl = incl.variable.values

all_data = pd.concat([raw_data.loc[:, ['date','ticker', 'next_month_return']], raw_data.loc[:, incl]],  axis = 1)
if 'px_pct_2m' not in incl:
    all_data = pd.concat([all_data, raw_data.loc[:, 'px_pct_2m']], axis = 1)
if 'px_pct_3m' not in incl:
    all_data = pd.concat([all_data, raw_data.loc[:, 'px_pct_3m']], axis = 1)
if 'px_pct_6m' not in incl:
    all_data = pd.concat([all_data, raw_data.loc[:, 'px_pct_6m']], axis = 1)

all_data = all_data.sort_values(by=['ticker', 'date'])

all_data = all_data.replace([np.inf, -np.inf], np.nan)
all_data = all_data.drop_duplicates(subset = ['ticker', 'date'])

### count NAs
for x in incl:
    this_col = all_data.loc[all_data[x].isnull(), x]
    pct = len(this_col) / len(all_data)
    if pct > .15:
        print(str(x), ': ', str(pct))

all_data.loc[:, 'next_2m_return'] = all_data.groupby('ticker')['px_pct_2m'].shift(-2)
all_data.loc[:, 'next_3m_return'] = all_data.groupby('ticker')['px_pct_3m'].shift(-3)
all_data.loc[:, 'next_6m_return'] = all_data.groupby('ticker')['px_pct_6m'].shift(-6)

all_data.loc[:, 'return_rank'] = all_data.groupby('date')['next_month_return'].rank(ascending = False, method = 'dense')

all_data.loc[:, 'is_top_50_next_month'] = 0
all_data.loc[all_data.return_rank <= 50, 'is_top_50_next_month'] = 1

all_data.loc[:, 'is_top_100_next_month'] = 0
all_data.loc[all_data.return_rank <= 100, 'is_top_100_next_month'] = 1

all_data.loc[:, 'return_rank'] = all_data.groupby('date')['next_2m_return'].rank(ascending = False, method = 'dense')

all_data.loc[:, 'is_top_50_next_2month'] = 0
all_data.loc[all_data.return_rank <= 50, 'is_top_50_next_2month'] = 1

all_data.loc[:, 'is_top_100_next_2month'] = 0
all_data.loc[all_data.return_rank <= 100, 'is_top_100_next_2month'] = 1


all_data.loc[:, 'return_rank'] = all_data.groupby('date')['next_3m_return'].rank(ascending = False, method = 'dense')

all_data.loc[:, 'is_top_50_next_3month'] = 0
all_data.loc[all_data.return_rank <= 50, 'is_top_50_next_3month'] = 1

all_data.loc[:, 'is_top_100_next_3month'] = 0
all_data.loc[all_data.return_rank <= 100, 'is_top_100_next_3month'] = 1

all_data.loc[:, 'return_rank'] = all_data.groupby('date')['next_6m_return'].rank(ascending = False, method = 'dense')

all_data.loc[:, 'is_top_50_next_6month'] = 0
all_data.loc[all_data.return_rank <= 50, 'is_top_50_next_6month'] = 1

all_data.loc[:, 'is_top_100_next_6month'] = 0
all_data.loc[all_data.return_rank <= 100, 'is_top_100_next_6month'] = 1

client = bigquery.Client()
job_config = bigquery.LoadJobConfig(write_disposition="WRITE_TRUNCATE")
job = client.load_table_from_dataframe(
                all_data, write_table_id, job_config=job_config
            )
