
# coding: utf-8
from __future__ import unicode_literals
import numpy as np
import pandas as pd
import datetime as dt
import time
from sqlalchemy import create_engine
from sqlalchemy.types import Date
from sqlalchemy.types import Integer
import re

def get_quote_yahoojp(code, start=None, end=None, interval='d'): # start = '2017-01-01'
    base = 'http://info.finance.yahoo.co.jp/history/?code={0}.T&{1}&{2}&tm={3}&p={4}'
    
    start = pd.to_datetime(start) # Timestamp('2017-01-01 00:00:00')

    if end == None:
        end = pd.to_datetime(pd.datetime.now())
    else :
        end = pd.to_datetime(end)
    start = 'sy={0}&sm={1}&sd={2}'.format(start.year, start.month, start.day) # 'sy=2017&sm=1&sd=1'
    end = 'ey={0}&em={1}&ed={2}'.format(end.year, end.month, end.day)
    p = 1
    results = []

    if interval not in ['d', 'w', 'm', 'v']:
        raise ValueError("Invalid interval: valid values are 'd', 'w', 'm' and 'v'")

    while True:
        url = base.format(code, start, end, interval, p)
        # https://info.finance.yahoo.co.jp/history/?code=7203.T&sy=2000&sm=1&sd=1&ey=2017&em=10&ed=13&tm=d&p=1
        tables = pd.read_html(url, header=0) # header引数で0行目をヘッダーに指定。データフレーム型
        if len(tables) < 2 or len(tables[1]) == 0:
            break
        results.append(tables[1]) # ページ内の3つのテーブルのうち2番目のテーブルを連結
        p += 1
        time.sleep(1)
        
    result = pd.concat(results, ignore_index=True) # インデックスをゼロから振り直す

    result.columns = ['Date', 'Open', 'High', 'Low', 'Close', 'Volume', 'AdjClose'] # 列名を変更
    if interval == 'm':
        result['Date'] = pd.to_datetime(result['Date'], format='%Y年%m月')
    else:
        result['Date'] = pd.to_datetime(result['Date'], format='%Y年%m月%d日') # 日付の表記を変更
    result = result.set_index('Date') # インデックスを日付に変更
    result = result.sort_index()
    
    company = tables[0].columns[0]
    # print(tables[0].columns[0])
    
    return [result, company]


def to_mysql(code, df):
    table_name = 't_{0}'.format(code)
    db_settings = {
        "host": 'localhost',
        "database": 'StockPrice_Yahoo_1',
        "user": 'user',
        "password": 'password',
        "port":'3306'
    }
    engine = create_engine('mysql://{user}:{password}@{host}:{port}/{database}'.format(**db_settings))
    # sqlalchemy.typesで定義されたデータ型を辞書形式で設定
    dtype = {'Date': Date(), 'Open': Integer(), 'High': Integer(), 'Low': Integer(), 'Close': Integer(), 'Volume': Integer(), 'AdjClose': Integer()}

    df.to_sql(table_name, engine, if_exists='replace', dtype=dtype)
    # 主キーを設定
    # 参考 https://stackoverflow.com/questions/30867390/python-pandas-to-sql-how-to-create-a-table-with-a-primary-key
    with engine.connect() as con:
        con.execute('ALTER TABLE `{0}` ADD PRIMARY KEY (`Date`);'.format(table_name))