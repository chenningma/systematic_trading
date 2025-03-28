from brel import Filing
from brel.utils import pprint


filing = Filing.open("/Users/grace/Downloads/0001083301-22-000023-xbrl/wulf-20220630_lab.xml")
#filing = Filing.open("https://www.sec.gov/Archives/edgar/data/320193/000032019323000077/aapl-20230701_htm.xml")
alldata = filing.get_all_facts()
pprint(alldata)