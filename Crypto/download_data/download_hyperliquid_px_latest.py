import json
import sys
import time
import requests
import pandas as pd
import numpy as np
import os
import logging
from datetime import datetime, timedelta, timezone
import argparse

import eth_account
from eth_account.signers.local import LocalAccount
from hyperliquid.exchange import Exchange
from hyperliquid.info import Info
from hyperliquid.utils import constants
from google.cloud import bigquery

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def init_bigquery():
    try:
        credentials_path = 'gcp-bigquery-privatekey.json'
        os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = credentials_path
        return bigquery.Client()
    except Exception as e:
        logger.error(f"Failed to initialize BigQuery client: {str(e)}")
        raise

def setup(base_url=None, skip_ws=False):
    try:
        config_path = os.path.join(os.path.dirname(__file__), "config.json")
        with open(config_path) as f:
            config = json.load(f)
        
        account: LocalAccount = eth_account.Account.from_key(config["secret_key"])
        address = config["account_address"] or account.address
        
        logger.info(f"Running with account address: {address}")
        if address != account.address:
            logger.info(f"Running with agent address: {account.address}")
            
        info = Info(base_url, skip_ws)
        user_state = info.user_state(address)
        spot_user_state = info.spot_user_state(address)
        margin_summary = user_state["marginSummary"]
        
        if float(margin_summary["accountValue"]) == 0 and len(spot_user_state["balances"]) == 0:
            url = info.base_url.split(".", 1)[1]
            error_string = f"No accountValue:\nIf you think this is a mistake, make sure that {address} has a balance on {url}."
            raise Exception(error_string)
            
        exchange = Exchange(account, base_url, account_address=address)
        return address, info, exchange
    except Exception as e:
        logger.error(f"Setup failed: {str(e)}")
        raise

def get_last_timestamp(client):
    """Get the latest timestamp from the main price table"""
    try:
        query = """
        SELECT MAX(time_close) as last_timestamp
        FROM `crypto_px.hyperliquid_px_5m`
        """
        query_job = client.query(query)
        results = query_job.result()
        
        for row in results:
            # If table is empty, return None
            if row.last_timestamp is None:
                return None
            return row.last_timestamp.replace(tzinfo=timezone.utc)
    except Exception as e:
        logger.error(f"Failed to get last timestamp: {str(e)}")
        raise

def download_by_name(info, symbol, start_date, end_date):
    """Download price data for a specific symbol and time range"""
    try:
        # Convert timestamps to milliseconds as required by API
        start_ts = int(start_date.timestamp() * 1000)
        end_ts = int(end_date.timestamp() * 1000)
        
        logger.debug(f"Downloading data for {symbol} from {start_date} to {end_date}")
        candle_data = info.candles_snapshot(name=symbol, interval='5m', startTime=start_ts, endTime=end_ts)

        if len(candle_data) == 0:
            return pd.DataFrame()

        df = pd.DataFrame(candle_data)
        df.columns = ['time_open', 'time_close', 'symbol', 'interval', 'open', 'close', 'high', 'low', 'volume', 'num']
        
        # Convert types
        df["time_open"] = pd.to_datetime(df["time_open"], unit="ms")
        df["time_close"] = pd.to_datetime(df["time_close"], unit="ms")
        df['open'] = df['open'].astype(float)
        df['close'] = df['close'].astype(float)
        df['high'] = df['high'].astype(float)
        df['low'] = df['low'].astype(float)
        df['volume'] = df['volume'].astype(float)

        df = df.sort_values(by='time_close')
        df["time_close_rd"] = df["time_close"].dt.round('min')

        return df
    except Exception as e:
        logger.error(f"Failed to download data for {symbol}: {str(e)}")
        raise

def main():
    parser = argparse.ArgumentParser(description='Download latest Hyperliquid price data')
    parser.add_argument('--days', type=int, default=30,
                      help='Number of days to download if no existing data (default: 30)')
    args = parser.parse_args()

    try:
        client = init_bigquery()
        address, info, exchange = setup(base_url=constants.MAINNET_API_URL, skip_ws=True)

        # Get coin list and update BigQuery table
        json_data = info.meta()
        coins_df = pd.DataFrame(json_data['universe'])
        
        table_id = 'crypto_px.hyperliquid_coin_list'
        job_config = bigquery.LoadJobConfig(write_disposition="WRITE_TRUNCATE")
        job = client.load_table_from_dataframe(coins_df, table_id, job_config=job_config)
        job.result()
        logger.info("Updated coin list in BigQuery")

        write_table_id = 'crypto_px.hyperliquid_px_5m_update'
        end_date = datetime.now(timezone.utc)

        last_timestamp = get_last_timestamp(client)
        if last_timestamp is None:
            start_date = end_date - timedelta(days=args.days)
            logger.info(f"No existing data found. Downloading last {args.days} days")
        else:
            start_date = last_timestamp
            logger.info(f"Continuing from last available timestamp: {start_date}")

        collect_df = pd.DataFrame()

        for id in coins_df.name:
            logger.info(f"Downloading data for {id} from {start_date} to {end_date}")
            try:
                df = download_by_name(info, id, start_date, end_date)

                if len(df) > 0:
                    collect_df = pd.concat([collect_df, df])

                if len(collect_df) > 15000:
                    job_config = bigquery.LoadJobConfig(write_disposition="WRITE_APPEND")
                    job = client.load_table_from_dataframe(collect_df, write_table_id, job_config=job_config)
                    job.result()
                    collect_df = pd.DataFrame()
                    logger.info("Batch uploaded to update table")
            except Exception as e:
                logger.error(f"Error downloading {id}: {str(e)}")

        if len(collect_df) > 0:
            job_config = bigquery.LoadJobConfig(write_disposition="WRITE_APPEND")
            job = client.load_table_from_dataframe(collect_df, write_table_id, job_config=job_config)
            job.result()
            logger.info("Final batch uploaded to update table")

        # Merge data into main table
        merge_query = """
        MERGE `crypto_px.hyperliquid_px_5m` T
        USING `crypto_px.hyperliquid_px_5m_update` S
        ON T.time_close = S.time_close AND T.symbol = S.symbol
        WHEN NOT MATCHED THEN
          INSERT (time_open, time_close, symbol, `interval`, open, close, high, low, volume, num, time_close_rd)
          VALUES (time_open, time_close, symbol, `interval`, open, close, high, low, volume, num, time_close_rd)
        """

        merge_job = client.query(merge_query)
        merge_job.result()
        logger.info("Data merged into main table")

        clear_query = "TRUNCATE TABLE `crypto_px.hyperliquid_px_5m_update`"
        clear_job = client.query(clear_query)
        clear_job.result()
        logger.info("Update table cleared")

    except Exception as e:
        logger.error(f"Script failed: {str(e)}")
        sys.exit(1)

if __name__ == "__main__":
    main()
