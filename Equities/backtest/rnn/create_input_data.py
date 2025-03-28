import pandas as pd
pd.options.mode.chained_assignment = None
import os 
import numpy as np
import math 
from datetime import datetime

from google.cloud import bigquery, bigquery_storage
from google.cloud.bigquery_storage import BigQueryReadClient
from google.cloud.bigquery_storage import types
from google.cloud import bigquery_storage

from dotenv import load_dotenv, find_dotenv

parent_dir = os.path.dirname(os.path.dirname(os.path.dirname(os.getcwd())))
dotenv_path = parent_dir + '/.env'
load_dotenv(dotenv_path)

credentials_path = parent_dir + '/' +os.getenv('TEST_CREDENTIALS_PATH')
os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = credentials_path
client = bigquery.Client()

### Get Training Set
query = '''
select 
ticker,
date_end,
px_close_pct_1w,
volume_avg_pct_1w,
marketcap
from training_data.weekly_training_data_medcap t
order by t.ticker, t.date_end 
'''
query_job = client.query(query)
raw_data = query_job.to_dataframe()

# Create quintile ranks (1-5) for each column
quintile_ranks = raw_data.loc[:, ['ticker', 'date_end']]
for col in raw_data.select_dtypes(['int', 'float']).columns:
    ranks = raw_data.groupby('date_end')[col].transform(
        lambda x: pd.qcut(x, q=5, labels=False, duplicates='drop') + 1)
    quintile_ranks[f'{col}_quintile'] = ranks

check = quintile_ranks.loc[quintile_ranks.date_end == '2025-03-07', ['ticker', 'date_end', 'px_close_pct_1w_quintile']]

# Get Training Data
all_data = quintile_ranks.reset_index(drop=True)

# Create MultiIndex from cross product of unique tickers and dates
tickers = all_data.ticker.unique()
dates = all_data.date_end.unique()
index = pd.MultiIndex.from_product([tickers, dates], names=['ticker', 'date_end'])

# Reindex the dataframe with the new MultiIndex
all_data = all_data.set_index(['ticker', 'date_end']).reindex(index).reset_index()

### Create Windowed Data

window_length = 150
sequence_length = len(all_data.date_end.unique())
training_length = int((sequence_length-window_length)*.8)

x_train = []
y_train = []
x_test = []
y_test = []

# Iterate through the sequences 
for this_ticker in all_data.ticker.unique():

	seq = all_data.loc[all_data.ticker == this_ticker, [ 'px_close_pct_1w_quintile']].values
	
	features = []
	labels = []
	# Create multiple training examples from each sequence
	for i in range(window_length, len(seq)):
		# Extract the features and label
		extract = seq[i - window_length:i + 1]

		# Set the features and label
		features.append(extract[:-1])
		labels.append(extract[-1])
	
	x_train += features[0:training_length]
	y_train += labels[0:training_length]

	x_test += features[training_length:]
	y_test += labels[training_length:]
        
def remove_nas(x_tbl, y_tbl):
  # Find indices where there are no NaN values in x_train
    x_train = np.array(x_tbl)
    y_train = np.array(y_tbl)

    # remove x_nulls
    valid_indices = ~np.isnan(x_train).any(axis=1)
    valid_indices = valid_indices.ravel().tolist()

    # Filter both x_train and y_train using the valid indices
    x_train = x_train[valid_indices, :]
    y_train = y_train[valid_indices, :]

    # remove y_nulls
    valid_indices = ~np.isnan(y_train).any(axis=1)
    valid_indices = valid_indices.ravel().tolist()

    # Filter both x_train and y_train using the valid indices
    x_train = x_train[valid_indices, :]
    y_train = y_train[valid_indices, :]

    return x_train, y_train

x_train, y_train = remove_nas(x_train, y_train)
x_test, y_test = remove_nas(x_test, y_test)