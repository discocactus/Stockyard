
# coding: utf-8
from __future__ import unicode_literals
import numpy as np
import pandas as pd
import pandas.tseries.offsets as offsets
import datetime as dt
import time
import importlib
import traceback
from retrying import retry
from sqlalchemy import create_engine
from sqlalchemy.types import Date
from sqlalchemy.types import Integer
from sqlalchemy.types import Text


class sql:
    db_settings = {
        "host": 'localhost',
        "database": 'StockPrice_Yahoo_1',
        "user": 'user',
        "password": 'password',
        "port":'3306'
    }
    engine = create_engine('mysql://{user}:{password}@{host}:{port}/{database}'.format(**db_settings))

    
    def write_quote(self, code, quote):
        table_name = 't_{0}'.format(code)
        # sqlalchemy.typesで定義されたデータ型を辞書形式で設定
        dtype = {
            'Date': Date(),
            'Open': Integer(),
            'High': Integer(),
            'Low': Integer(),
            'Close': Integer(),
            'Volume': Integer(),
            'AdjClose': Integer()
        }
        quote.to_sql(table_name, sql.engine, if_exists='replace', dtype=dtype)
        # 主キーを設定
        # 参考 https://stackoverflow.com/questions/30867390/python-pandas-to-sql-how-to-create-a-table-with-a-primary-key
        with sql.engine.connect() as con:
            con.execute('ALTER TABLE `{0}` ADD PRIMARY KEY (`Date`);'.format(table_name))
        

    def get_quote(self, code):
        table_name = 't_{0}'.format(code)
        result = pd.read_sql_table(table_name, sql.engine, index_col='Date')#.drop('index', axis=1)
        
        return result
    
    
    def write_info(self, table_name, info):
        # sqlalchemy.typesで定義されたデータ型を辞書形式で設定
        dtype = {
            'Code': Integer(),
            'StockName': Text(),
            'Date': Date(),
            'Open': Text()}
        
        info.to_sql(table_name, sql.engine, if_exists='replace', dtype=dtype)
        
        
    def get_info(self):
        result = pd.read_sql_table('info', sql.engine, index_col=None).drop('index', axis=1)
        result['Date'] = pd.to_datetime(result['Date'])
        
        return result
        

    def get_domestic_stock_code(self, start_index=0, end_index=None):
        domestic_stock_table = pd.read_sql_table('domestic_stock_table', sql.engine, index_col=None).drop('index', axis=1)
        
        if end_index == None:
            end_index = len(domestic_stock_table)

        result = list(domestic_stock_table['code'][start_index : end_index])
        
        return result


    def statement_query(self, statement):
        result = pd.read_sql_query(statement, sql.engine, index_col=None)
        # ex. df = sql.statement_query('SELECT code, name FROM domestic_stock_table')
        # テーブル全体ではなく抽出の場合、インデックスは無いらしく下記ではエラーになる
        #result = pd.read_sql_query(statement, sql.engine, index_col=None).drop('index', axis=1)
        
        return result
    
    
# 関数にretryデコレーターを付ける
# stop_max_attempt_numberは最大リトライ関数を指定する
# wait_exponential_multiplierは指数関数的なウェイトを取る場合の初回のウェイトをミリ秒単位で指定する
@retry(stop_max_attempt_number=5, wait_exponential_multiplier=1000)
def get_quote_yahoojp(code, start=None, end=None, interval='d'): # start = '2017-01-01'
    # http://sinhrks.hatenablog.com/entry/2015/02/04/002258
    # http://jbclub.xii.jp/?p=598
    base = 'http://info.finance.yahoo.co.jp/history/?code={0}.T&{1}&{2}&tm={3}&p={4}'
    
    start = pd.to_datetime(start) # Timestamp('2017-01-01 00:00:00')

    if end == None:
        end = pd.to_datetime(pd.datetime.now())
    else :
        end = pd.to_datetime(end)
    start = 'sy={0}&sm={1}&sd={2}'.format(start.year, start.month, start.day) # 'sy=2017&sm=1&sd=1'
    end = 'ey={0}&em={1}&ed={2}'.format(end.year, end.month, end.day)
    p = 1
    tmp_result = []

    if interval not in ['d', 'w', 'm', 'v']:
        raise ValueError("Invalid interval: valid values are 'd', 'w', 'm' and 'v'")

    while True:
        url = base.format(code, start, end, interval, p)
        # print(url)
        # https://info.finance.yahoo.co.jp/history/?code=7203.T&sy=2000&sm=1&sd=1&ey=2017&em=10&ed=13&tm=d&p=1
        tables = pd.read_html(url, header=0) # header引数で0行目をヘッダーに指定。データフレーム型
        if len(tables) < 2 or len(tables[1]) == 0:
            # print('break')
            break
        tmp_result.append(tables[1]) # ページ内の3つのテーブルのうち2番目のテーブルを連結
        p += 1
        # print(p)
        
    result = pd.concat(tmp_result, ignore_index=True) # インデックスをゼロから振り直す

    result.columns = ['Date', 'Open', 'High', 'Low', 'Close', 'Volume', 'AdjClose'] # 列名を変更
    if interval == 'm':
        result['Date'] = pd.to_datetime(result['Date'], format='%Y年%m月')
    else:
        result['Date'] = pd.to_datetime(result['Date'], format='%Y年%m月%d日') # 日付の表記を変更
    result = result.set_index('Date') # インデックスを日付に変更
    result = result.sort_index()
    
    stock_name = tables[0].columns[0]
    # print([code, stock_name])
    
    return [result, stock_name]


def extract_quote(tmp_quote):
    # null が存在する行を取り除いて価格データとする 参考 https://qiita.com/u1and0/items/fd2780813b690a40c197
    result = tmp_quote[~tmp_quote.isnull().any(axis=1)].astype(float).astype(int) # この場合、"~"は "== False" とするのと同じこと
    # なぜか日付が重複した行が入る場合があるので確認、削除
    if(result.index.duplicated().any()):
        result = result[~result.index.duplicated()]
        
    return result


def reform_info(tmp_info, code, stock_name):
    # 単列の場合、代入と同時に列を生成できるが、複数列の場合は存在しないとエラーになるので先に列を追加しなければいけない
    result = tmp_info.ix[:, ['Code', 'StockName', 'Date', 'Open', 'High', 'Low', 'Close', 'Volume', 'AdjClose']] # 列を追加、並べ替え
    result[['Code', 'StockName']] = [code, stock_name] # 複数列に値を代入する場合は列名をリスト形式で記述
    result['Code'] = result['Code'].astype(int) # float型になってしまうので変換
            
    return result