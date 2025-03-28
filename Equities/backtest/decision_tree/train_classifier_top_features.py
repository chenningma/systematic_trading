import pandas as pd
pd.options.mode.chained_assignment = None
import os 
from google.cloud import bigquery, bigquery_storage
import numpy as np
import math 
from datetime import datetime
import random 

import xgboost as xgb
from sklearn.metrics import confusion_matrix, roc_curve, auc, f1_score
from sklearn.model_selection import train_test_split, GridSearchCV
from sklearn.metrics import mean_squared_error as MSE
from sklearn.ensemble import RandomForestRegressor

from google.cloud.bigquery_storage import BigQueryReadClient
from google.cloud.bigquery_storage import types
from google.cloud import bigquery_storage

credentials_path = 'gcp-bigquery-privatekey.json'
os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = credentials_path

def print_runtime(time_start):
    time_stop = datetime.now()
    runtime = time_stop - time_start
    print(runtime)

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

def backtest_classifier_base(all_data, xgb_params, cutoff): 
    date_list = all_data.loc[:, 'date'].sort_values().unique()
    train_date = pd.DataFrame(date_list[0:cutoff], columns = ['date'])
    train = all_data.merge(train_date, how = 'inner', on ='date').reset_index(drop=True)

    all_portfolio = pd.DataFrame()
    start = cutoff+1
    for k in range(start,len(date_list)):
        try:
            prev_month = date_list[k-1]
            latest_month = date_list[k]

            latest_data = all_data.loc[all_data.date == latest_month, :].reset_index(drop = True)
            
            # match with marketcap
            #if top_mcap_flag == 1: 
            #    latest_data = latest_data.merge(mcap_tbl, how = 'inner', on = ['ticker', 'date'])

            train = pd.concat([train, latest_data])

            X_train = train.loc[:, incl]
            Y_train = train.loc[:, Y_field]

            model = xgb.XGBClassifier(**xgb_params)
            model.fit(X_train, Y_train)

            # make predictions
            
            pred_class = pd.DataFrame(model.predict(latest_data.loc[:, incl]), columns = ['pred_label']) 
            score = f1_score(latest_data.loc[:, Y_field], pred_class)
            #print(str(latest_month), ': ', str(score))

            pred = pd.DataFrame(model.predict_proba(latest_data.loc[:, incl])[:, 1], columns = ['pred']) 
            to_add = latest_data.loc[:, ['ticker', 'next_month_return']]
            pred = pd.concat([pred, to_add], axis = 1)

            to_add = latest_data.loc[:, Y_field]
            to_add.name = 'dependent_var'
            pred = pd.concat([pred, to_add], axis = 1)

            top_predict = pred.sort_values(by = 'pred', ascending = False).reset_index(drop=True)

            ## top 50, full replacement
            top_cutoff = 50
            portfolio = top_predict.iloc[0:top_cutoff, :]

            portfolio.loc[:, 'date'] = latest_month
            portfolio.loc[:, 'score'] = score 

            all_portfolio = pd.concat([all_portfolio, portfolio])
        except Exception as e:
            print("error on model_id", str(model_id), ", date ", str(latest_month))
            print(e)

    return all_portfolio

def write_model_to_db(xgb_params, score, Y_field, importance, model_result_table_id, model_variable_table_id):
    client = bigquery.Client()
    query_job = client.query("""
    SELECT max(model_id) as latest
    FROM {}
    """.format(model_result_table_id))

    last_num = query_job.result().to_dataframe()
    model_id = last_num.iloc[0,0] + 1

    parameters_tbl = pd.DataFrame([xgb_params], columns = xgb_params.keys()).transpose().reset_index()
    parameters_tbl.columns = ['parameter', 'value']
    parameters_tbl['value'] = parameters_tbl['value'].astype(str)
    parameters_tbl.loc[:, 'model_id'] = model_id
    parameters_tbl.loc[:, 'model_type'] = 'XGBClassifier'
    parameters_tbl.loc[:, 'model_run_date'] = datetime.now()
    parameters_tbl.loc[:, 'score'] = score
    parameters_tbl.loc[:, 'score_type'] = 'f1'
    parameters_tbl.loc[:, 'dependent_var'] = Y_field
    
    job_config = bigquery.LoadJobConfig(write_disposition="WRITE_APPEND")
    job = client.load_table_from_dataframe(
                    parameters_tbl, model_result_table_id, job_config=job_config
                )
    
    gain_tbl = pd.DataFrame([importance], columns = importance.keys()).transpose()
    gain_tbl = gain_tbl.reset_index()
    gain_tbl.columns = ['variable', 'importance']
    gain_tbl = gain_tbl.merge(pd.DataFrame(incl, columns=['variable']), how = 'outer', on ='variable').fillna(0)
    gain_tbl.loc[:, 'model_id'] = model_id
    #gain_tbl.to_csv('variables.csv')

    job = client.load_table_from_dataframe(gain_tbl, model_variable_table_id, job_config=job_config)
    return model_id
    
def write_backtest_to_db(all_portfolio, model_id, dependent_var_name, description, train_dataset, write_table_id):
    client = bigquery.Client()
    query_job = client.query("""
    SELECT max(backtest_id) as latest
    FROM {}
    """.format(write_table_id))

    last_num = query_job.result().to_dataframe()
    backtest_id = last_num.iloc[0,0] + 1

    all_portfolio.loc[:, 'backtest_id'] = backtest_id
    all_portfolio.loc[:, 'model_id'] = model_id
    all_portfolio.loc[:, 'dependent_var_name'] = dependent_var_name
    all_portfolio.loc[:, 'description'] = description
    all_portfolio.loc[:, 'train_dataset'] = train_dataset
    all_portfolio.loc[:, 'prediction_type'] = 'probability'
    all_portfolio.loc[:, 'score_type'] = 'f1'
    all_portfolio['dependent_var'] = all_portfolio['dependent_var'].astype(float)

    job_config = bigquery.LoadJobConfig(write_disposition="WRITE_APPEND")
    job = client.load_table_from_dataframe(
                all_portfolio, write_table_id, job_config=job_config
            )
    
    return backtest_id

def create_monthly_return(this_ptf, cost_per_trade):
    this_ptf = this_ptf.sort_values(by = ['date'])

    monthly_return = this_ptf.groupby(['date'])['next_month_return'].mean().reset_index()

    na_list = monthly_return.loc[monthly_return.next_month_return.isnull(), 'date'].tolist()
    na_list = [str(x) for x in na_list]
    if len(na_list) > 0 :
        print("check NAs:", ', '.join(na_list))

    monthly_return['next_month_return'] = monthly_return.next_month_return.fillna(0)
    
    # calculate turnover
    date_list = this_ptf.loc[:, 'date'].sort_values().unique()

    turnover_tbl = list()
    for i in range(1, len(date_list)):
        first_month = date_list[i-1]
        next_month = date_list[i]
        first_ptf = this_ptf.loc[this_ptf.date == first_month, 'ticker'].values
        next_ptf = this_ptf.loc[this_ptf.date == next_month, 'ticker'].values

        idx = np.in1d(next_ptf, first_ptf)
        turnover = len(next_ptf[~idx])

        this_turnover = [first_month, turnover]
        turnover_tbl.append(this_turnover)

    turnover_tbl = pd.DataFrame(turnover_tbl, columns = ['date', 'turnover'])

    monthly_return = monthly_return.merge(turnover_tbl, how = 'left', on = 'date')

    monthly_return.loc[:, 'trade_costs'] = monthly_return.turnover * 1/50 * cost_per_trade
    monthly_return.loc[:, 'total_return'] = monthly_return.next_month_return - monthly_return.trade_costs 

    monthly_return.loc[:, 'total_return_idx'] = 100.0

    for i in range(1, len(monthly_return)):
        monthly_return.loc[i, 'total_return_idx'] =  monthly_return.loc[i-1, 'total_return_idx'] * (1+monthly_return.loc[i, 'total_return'])

    return monthly_return

def create_performance_stats(monthly_return, start, end):

        return_5y = monthly_return.loc[end, 'total_return_idx'] / monthly_return.loc[start, 'total_return_idx'] - 1
        std_5y = monthly_return.loc[start:end, 'total_return'].std() * np.sqrt(12)
        sharpe_5y = monthly_return.loc[start:end, 'total_return'].mean()*12 / (monthly_return.loc[start:end, 'total_return'].std() * np.sqrt(12))
        drawdown_5y = monthly_return.loc[start:end, 'total_return'].min()
        drawdown_5y_dt = monthly_return.loc[monthly_return.total_return == drawdown_5y, 'date'].values[0]

        std = monthly_return.loc[0:end, 'total_return'].std() * np.sqrt(12)
        sharpe = monthly_return.loc[0:end, 'total_return'].mean()*12 / (monthly_return.loc[0:end, 'total_return'].std() * np.sqrt(12))
        drawdown = monthly_return.loc[0:end, 'total_return'].min()
        drawdown_dt = monthly_return.loc[monthly_return.total_return == drawdown, 'date'].values[0]

        this_stat = [return_5y, std_5y, sharpe_5y, drawdown_5y, drawdown_5y_dt, std, sharpe, drawdown, drawdown_dt]
        stat_tbl = pd.DataFrame(this_stat, columns = ['return_5y_latest', 'std_5y', 'sharp_5y', 'max_5y_drawdown', '5y_drawdown_dt', 'std_all', 'sharpe_all', 'max_drawdown', 'drawdown_dt'])

        return stat_tbl

project_id = "boreal-pride-417020"
dataset_id = "train"
read_table_id = "training_xincl_data_top60pct_ipo_delay"
raw_data = read_table(project_id, dataset_id, read_table_id)

# get model parameters
def get_model_tbl_from_id(model_result_table_id, starting_model_id):
    client = bigquery.Client()
    query_job = client.query("""
    SELECT *
    FROM {id1} where model_id = {id2}
    """.format(id1=model_result_table_id, id2=starting_model_id))

    model_tbl = query_job.result().to_dataframe()
    Y_field = model_tbl.loc[0, 'dependent_var']

    params = model_tbl.loc[model_tbl.model_id == starting_model_id, ['parameter', 'value']]
    params['value'] = params['value'].astype(float)
    xgb_params = dict(zip(params.parameter, params.value))
    xgb_params['n_estimators'] = int(xgb_params['n_estimators'])
    xgb_params['max_depth'] = int(xgb_params['max_depth'])

    return Y_field, xgb_params

model_result_table_id = 'boreal-pride-417020.train.model_results_is_top_100_next_month'
starting_model_id = 20
Y_field, xgb_params = get_model_tbl_from_id(model_result_table_id, starting_model_id)

### Get training variables    
client = bigquery.Client()
query_job = client.query("""
   SELECT distinct variable
   FROM train.default_model_variables
   where incl = 1                           
   """)

all_vars = query_job.result().to_dataframe()
all_vars = all_vars.variable.values

# random generate
for t in range(0, 20):
    var_list = list()
    for i in range(0, 2000):
        this_var_list = random.sample(list(all_vars), 50)
        var_list.append(this_var_list)

    #model_id_list = list(model_tbl.model_id.unique())
    #for model_id in model_id_list:
    max_score = 0.135
    for j in range(0, len(var_list)):
        
        time_start = datetime.now()

        incl = var_list[j]

        all_data = pd.concat([raw_data.loc[:, ['ticker', 'date', 'next_month_return']], raw_data.loc[:, Y_field]], axis = 1)
        all_data = pd.concat([all_data, raw_data.loc[:, incl]], axis = 1)
        all_data = all_data.dropna(subset = ['date', 'ticker'])

        date_list = all_data.loc[:, 'date'].sort_values().unique()
        cutoff = len(date_list) - 8*12
        train_date = pd.DataFrame(date_list[0:cutoff], columns = ['date'])
        test_date = pd.DataFrame(date_list[cutoff:len(date_list)], columns = ['date'])

        train = all_data.merge(train_date, how = 'inner', on ='date').reset_index(drop=True)
        X_train = train.loc[:, incl]
        Y_train = train.loc[:, Y_field]

        test = all_data.merge(test_date, how = 'inner', on ='date').reset_index(drop=True)
        X_test = test.loc[:, incl]
        Y_test = test.loc[:, Y_field]
        
        # run classifier
        model = xgb.XGBClassifier(**xgb_params)
        model.fit(X_train, Y_train)
        pred = model.predict(X_test) 

        # Score Computation 
        score = f1_score(Y_test, pred)
        
        ### compare with past performance
        
        if score > max_score:
        # write model
            importance = model.get_booster().get_score(importance_type = 'gain')
            
            model_result_table_id = 'boreal-pride-417020.train.model_results_is_top_100_next_month'
            model_variable_table_id = 'boreal-pride-417020.train.model_variables_is_top_100_next_month'
            model_id = write_model_to_db(xgb_params, score, Y_field, importance, model_result_table_id, model_variable_table_id)

        max_score = max(score, max_score)
