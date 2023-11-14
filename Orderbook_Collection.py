# 모듈 불러오기
from requests.adapters import HTTPAdapter
from requests.packages.urllib3.util.retry import Retry

import sys
import datetime
import json
import csv
import os
import requests
import time
import pandas as pd
import argparse
import warnings

import timeit
import urllib3
import numpy as np 
import collections

# 1. init_session() def init_session() 을 정의해야 → 필수 변수 중 session 을 실행할 수 있음.
def init_session():
    session = requests.Session()
    retry = Retry (connect=1, backoff_factor=0.1)
    adapter = HTTPAdapter (max_retries=retry)
    session.mount('http://', adapter)
    session.mount('https://', adapter)
    
    return session

# 2. 필수 변수들은 맨 처음에 선언해야. 어디에 쓰일지를 모름.
_list_ex = ['bithumb']
# csv_dir = './raw/'
csv_dir = os.getcwd()
_dict_url = {}
currency = ''

session = init_session()

# 3. def bithumb_live_book() 을 정의해야 → pull_csv_book_trade() 함수를 실행할 수 있음.
def bithumb_live_book(data, req_timestamp):
    # timestamp,price,type,quantity
    print("data: ", data, "\n")

    warnings.filterwarnings("ignore", category=FutureWarning)

    data = data['data']
    bids = (pd.DataFrame(data['bids'])).apply(pd.to_numeric,errors='ignore')

    bids.sort_values('price', ascending=False, inplace=True)
    bids = bids.reset_index(); del bids['index']
    bids['type'] = 0
    
    asks = (pd.DataFrame(data['asks'])).apply(pd.to_numeric,errors='ignore')

    asks.sort_values('price', ascending=True, inplace=True)
    asks['type'] = 1 

    # df = bids.append(asks)
    df = pd.concat([bids, asks], ignore_index=True)
    df['quantity'] = df['quantity'].round(decimals=4)
    df['timestamp'] = req_timestamp
        
    return df

# 4. def get_book_trade(ex, url, req_timestamp) 를 정의해야 → pull_csv_book_trade() 함수를 실행할 수 있음.

def get_book_trade(ex, url, req_timestamp):
    
    book = trade = {}
    try:
        book = (session.get(url[0], headers={ 'User-Agent': 'Mozilla/5.0' }, verify=False, timeout=1)).json()
        trade = (session.get(url[1], headers={ 'User-Agent': 'Mozilla/5.0' }, verify=False, timeout=1)).json()
    except:
        return None, None
    
    print(book)
    
    return book, trade

# 5. def write_csv(fn, df) 함수를 정의해야 → pull_csv_book_trade()  를 실행할 수 있음.
def write_csv(fn, df):
    should_write_header = os.path.exists(fn)
    if should_write_header == False:
        df.to_csv(fn, index=False, header=True, mode = 'a')
    else:
        df.to_csv(fn, index=False, header=False, mode = 'a')

# 6. def pull_csv_book_trade() 가 정의되어야 → def main() 을 실행가능
def pull_csv_book_trade():
    
    start_day = datetime.datetime(2023, 11, 11, 0, 0, 0) # 지정한 일자
    next_day = start_day + datetime.timedelta(days=1) # 24시간 후 # test : minutes=1, seconds=10
    
    timestamp = last_update_time = datetime.datetime.now()

    
    while datetime.datetime.now() <= next_day:
        if datetime.datetime.now() < start_day: 
            continue    
    
        timestamp = datetime.datetime.now()
        if ((timestamp-last_update_time).total_seconds() < 1.0):
            continue
        last_update_time = timestamp

        req_timestamp = timestamp.strftime('%Y-%m-%d %H:%M:%S.%f')
        req_time = req_timestamp.split(' ')[0]

        _dict_book_trade = {}
        #start_time = timeit.default_timer()
        _err = False
        for ex in _list_ex:
            book, trade = get_book_trade (ex, _dict_url[ex], req_timestamp)
            if book is None or trade is None:
                _err = True
                break
            if not book or not trade:
                _err = True
                break
            _dict_book_trade.update ({ex: [book, trade]})

        #delay = timeit.default_timer() - start_time
        #print 'fetch delay: %.2fs' % delay

        if _err == True:
            continue

        for ex in _list_ex:
            book_fn = '%s/%s-only-%s-%s-book.csv'% (csv_dir, req_time, ex, currency)
            # trade_fn = '%s/%s-only-%s-%s-trade.csv'% (csv_dir, req_time, ex, currency)

            book_df = bithumb_live_book (book, req_timestamp)
            # trade_df, raw_trade_df = bithumb_live_trade (trade, req_timestamp)

            '''if trade_df is None:
                continue'''

            write_csv(book_fn, book_df) 
            # write_csv(trade_fn, trade_df)

# 7. def parse_args() 가 정의되어야 → main() 을 실행가능
def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument('--currency', help="choose crypto-currency", choices = ('BTC','ETH'), dest='currency', action="store")

    return parser.parse_args()

# 8. def main() 를 정의해야 → if 조건으로 실행가능.
def main():

    global _dict_url
    global currency

    urllib3.disable_warnings()    
    # args = parse_args()
    # currency = args.currency
    currency = "BTC"

    print("currency: ", currency, "\n")

    _dict_url = {'bithumb': ['https://api.bithumb.com/public/orderbook/%s_KRW/?count=5' % currency, 
    'https://api.bithumb.com/public/transaction_history/%s_KRW/?count=10' % currency]}
    
    pull_csv_book_trade()
    

if __name__ == '__main__': # "Orderbook_Collection":
    main()
