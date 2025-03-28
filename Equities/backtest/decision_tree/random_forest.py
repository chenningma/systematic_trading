from google.cloud import bigquery, bigquery_storage
import numpy as np
import math 
from datetime import datetime

import xgboost as xgb
from sklearn.metrics import confusion_matrix, roc_curve, auc, f1_score
from sklearn.model_selection import train_test_split, GridSearchCV
from sklearn.metrics import mean_squared_error as MSE
from sklearn.ensemble import RandomForestRegressor
import shap 

from google.cloud.bigquery_storage import BigQueryReadClient
from google.cloud.bigquery_storage import types
from google.cloud import bigquery_storage

credentials_path = 'gcp-bigquery-privatekey.json'
os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = credentials_path

bqstorageclient = bigquery_storage.BigQueryReadClient()
project_id_billing = 'boreal-pride-417020'

def read_table():

    bqstorageclient = bigquery_storage.BigQueryReadClient()

    project_id = "boreal-pride-417020"
    dataset_id = "train"
    table_id = "training_data_100vars"
    table = f"projects/{project_id}/datasets/{dataset_id}/tables/{table_id}"

    read_options = types.ReadSession.TableReadOptions(
        selected_fields=["country_name", "region_name"] 
    )

    parent = "projects/{}".format(project_id_billing)

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

raw_data = read_table()

client = bigquery.Client()
query_job = client.query("""
   SELECT distinct variable
   FROM train.model_variables where model_id = 1
   """)

incl = query_job.result().to_dataframe()
incl = incl.variable.values

dep_var = 'next_month_return'
all_data = pd.concat([raw_data.loc[:, ['ticker', 'date']], raw_data.loc[:, dep_var]], axis = 1)
all_data = pd.concat([all_data, raw_data.loc[:, incl]], axis = 1)
all_data = all_data.dropna()

date_list = all_data.loc[:, 'date'].sort_values().unique()
cutoff = len(date_list) - 5*12
train_date = pd.DataFrame(date_list[0:cutoff], columns = ['date'])
test_date = pd.DataFrame(date_list[cutoff:len(date_list)], columns = ['date'])

train = all_data.merge(train_date, how = 'inner', on ='date').reset_index(drop=True)
X_train = train.loc[:, incl]
Y_train = train.loc[:, dep_var]

test = all_data.merge(test_date, how = 'inner', on ='date').reset_index(drop=True)
X_test = test.loc[:, incl]
Y_test = test.loc[:, dep_var]

### Tune Random forest for feature selection
param_dist = {
    "max_depth": [3,4,5,6,9,12],
    "min_samples_split": [20, 40, 100],
    "min_samples_leaf": [20, 40, 100],
    "max_features": ['auto', 'sqrt', 'log2']
}

random_search = GridSearchCV(estimator=rf, param_distributions=param_dist,
                                   n_iter=100, cv=5, random_state=42, n_jobs=-1)

random_search.fit(X_train, Y_train)
print("Best Parameters:", random_search.best_params_)

### Train Random Forest
rf = RandomForestRegressor(n_estimators = 100, max_depth=5, min_samples_leaf = 20, min_samples_split=40, max_features = 'sqrt', verbose = 1)
rf.fit(X_train, Y_train)
pred = rf.predict(X_train) 
    
# RMSE Computation 
rmse = f1_score(Y_test, pred)
print(str(rmse)) 

parameters_tbl = pd.DataFrame([['n_estimators', 'max_depth','min_samples_leaf','min_samples_split', 'max_features'], ['100', '3', '20', '40', 'sqrt']]).transpose()
parameters_tbl.columns = ['parameter', 'value']
parameters_tbl.loc[:, 'model_id'] = 1
parameters_tbl.loc[:, 'model_type'] = 'RandomForestRegressor'
parameters_tbl.loc[:, 'model_run_date'] = datetime.strptime('2024-03-27', "%Y-%m-%d")
parameters_tbl.loc[:, 'rmse'] = rmse

# Get importance
importance = rf.feature_importances_
gain_tbl = pd.DataFrame([importance], columns = X_train.columns).transpose()
gain_tbl = gain_tbl.sort_values(by = 0, ascending = False).reset_index()
gain_tbl.columns = ['variable', 'importance']
gain_tbl.loc[:, 'model_id'] = 3
gain_tbl.to_csv('variables.csv')

client = bigquery.Client()
table_id = 'boreal-pride-417020.train.model_results'
job_config = bigquery.LoadJobConfig(write_disposition="WRITE_APPEND")
job = client.load_table_from_dataframe(
                parameters_tbl, table_id, job_config=job_config
            )
table_id = 'boreal-pride-417020.train.model_variables'
job = client.load_table_from_dataframe(
                gain_tbl, table_id, job_config=job_config
            )
