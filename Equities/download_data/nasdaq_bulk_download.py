import json
import sys
import time
import os
from dotenv import load_dotenv
parent_dir = os.path.dirname(os.path.dirname(os.getcwd()))
dotenv_path = parent_dir + '/.env'
load_dotenv(dotenv_path)

api_key = os.getenv('NASDAQ_API_KEY') # enter your api key, it can be found in your Quandl account here: https://www.quandl.com/account/profile
#table = 'SF1' # enter the Sharadar table you would like to retrieve 
destFileRef = 'SF1_tickers.csv.zip' # enter the destination that you would like the retrieved data to be saved to
url = f'https://data.nasdaq.com/api/v3/datatables/SHARADAR/TICKERS?table=SF1&api_key={api_key}&qopts.export=true'

def bulk_fetch(url=url, destFileRef=destFileRef):
    version = sys.version.split(' ')[0]
    if version < '3':
        import urllib2
        fn = urllib2.urlopen
    else:
        import urllib
        fn = urllib.request.urlopen

    valid = ['fresh','regenerating']
    invalid = ['generating']
    status = ''

    while status not in valid:
        Dict = json.loads(fn(url).read())
        last_refreshed_time = Dict['datatable_bulk_download']['datatable']['last_refreshed_time']
        status = Dict['datatable_bulk_download']['file']['status']
        link = Dict['datatable_bulk_download']['file']['link']
        print(status)
    if status not in valid:
        time.sleep(60)

    print('fetching from %s' % link)
    zipString = fn(link).read()
    f = open(destFileRef, 'wb')
    f.write(zipString)
    f.close()
    print('fetched')

bulk_fetch()