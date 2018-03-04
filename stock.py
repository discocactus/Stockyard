# coding: utf-8

# from __future__ import unicode_literals
import numpy as np
import pandas as pd
import pandas.tseries.offsets as offsets
import datetime as dt
import time
import importlib
import logging
from retry import retry
# import traceback
# from retrying import retry


# パスの設定
csv_path = 'D:\stockyard\_csv'
price_path = 'D:\stockyard\_yahoo_csv'
# csv_path = '/home/hideshi_honma/stockyard/_csv'
# price_path = '/home/hideshi_honma/stockyard/_yahoo_csv'

    
def get_jpx_expro_code(start_index=0, end_index=None, csv_path=csv_path):
    jpx_expro = pd.read_csv('{0}/jpx_expro.csv'.format(csv_path), index_col=0)

    if end_index == None:
        end_index = len(jpx_expro)

    result = list(jpx_expro['code'][start_index : end_index])

    return result
    
    
def get_jpx_new_added_code(start_index=0, end_index=None, csv_path=csv_path):
    jpx_new_added = pd.read_csv('{0}/jpx_new_added.csv'.format(csv_path), index_col=0)

    if end_index == None:
        end_index = len(jpx_new_added)

    result = list(jpx_new_added['code'][start_index : end_index])

    return result
    
    
def get_yahoo_code(start_index=0, end_index=None, csv_path=csv_path):
    yahoo_code = pd.read_csv('{0}/yahoo_stock_table.csv'.format(csv_path), index_col=0)

    if (end_index == None) or (end_index > len(yahoo_code)):
        end_index = len(yahoo_code)

    result = list(yahoo_code['code'][start_index : end_index])

    return result


def get_yahoo_info(csv_path=csv_path):
    result = pd.read_csv('{0}/yahoo_info.csv'.format(csv_path), index_col=0)
    result['Date'] = pd.to_datetime(result['Date'])

    return result

    
def get_yahoo_price(code, price_path=price_path):
    result = pd.read_csv('{0}/t_{1}.csv'.format(price_path, code), index_col=0)
    result.index = pd.to_datetime(result.index)

    return result

    
# 関数にretryデコレーターを付ける
@retry(tries=5, delay=1, backoff=2)
def get_table(url):
    result = pd.read_html(url, header=0) # header引数で0行目をヘッダーに指定。データフレーム型
    
    return result


def get_stock_table_yahoojp():
    result = []
    p = 1
    base = 'http://stocks.finance.yahoo.co.jp/stocks/qi/?&p={0}'

    while True:
        url = base.format(p)
        # 'https://stocks.finance.yahoo.co.jp/stocks/qi/?&p=1' # 2018-03-03 p=187まで
        print('{0}: {1}'.format(p, url))
        tables = get_table(url)
        if len(tables[2]) == 0:
            break
        result.append(tables[2])
        p += 1
        
    result = pd.concat(result, ignore_index=True)
        
    return result

  
def get_price_yahoojp(code, start=None, end=None, interval='d'): # start = '2017-01-01'
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
        tables = get_table(url)
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


def extract_price(tmp_price):
    # null が存在する行を取り除いて価格データとする 参考 https://qiita.com/u1and0/items/fd2780813b690a40c197
    result = tmp_price[~tmp_price.isnull().any(axis=1)].astype(float).astype(int) # この場合、"~"は "== False" とするのと同じこと
    # なぜか日付が重複した行が入る場合があるので確認、削除
    if(result.index.duplicated().any()):
        result = result[~result.index.duplicated()]
        
    return result


def reform_info(tmp_info, code, stock_name):
    # 単列の場合、代入と同時に列を生成できるが、複数列の場合は存在しないとエラーになるので先に列を追加しなければいけない
    # result = tmp_info.loc[:, ['Code', 'StockName', 'Date', 'Open', 'High', 'Low', 'Close', 'Volume', 'AdjClose']] # 列を追加、並べ替え
    # result[['Code', 'StockName']] = [code, stock_name] # 複数列に値を代入する場合は列名をリスト形式で記述
    tmp_info['Code'] = code # float型になってしまうので変換
    tmp_info['StockName'] = stock_name
    tmp_info = tmp_info[['Code', 'StockName', 'Date', 'Open', 'High', 'Low', 'Close', 'Volume', 'AdjClose']] # 列を並べ替え
            
    return tmp_info


def complement_price(price_table):
    # 出来高ゼロの日はインデックスごと欠落しているので、ビジネスデイ(freq='B')のdatetime型インデックスにデータをあてはめる
    # TODO 休日はどうするか要検討、japandas使ってみる
    result = pd.DataFrame(price_table, index=pd.date_range(price_table.index[0], price_table.index[-1], freq='B'))
    
    # 欠損データを補完する
    # Volumeをゼロで補完
    result.Volume = result.Volume.fillna(0)
    
    # Close,AdjCloseは前日データで補完
    result[['Close', 'AdjClose']] = result[['Close', 'AdjClose']].fillna(method='ffill') 
    
    # 欠損Openを前日補完済み前日Closeで補完
    result['Open'] = result['Open'].fillna(result['Close'].shift(1))
    
    # High,Low,CloseはOpenで補完
    result[['Open', 'High', 'Low']] = result[['Open', 'High', 'Low']].fillna(method='ffill', axis=1)
    
    return result


def add_processed_price(price_table):
    # 差分系列の作成

    # 原系列
    # 当日Close-前日Close
    price_table['diff_close'] = price_table['AdjClose'].diff(1)
    # Close-Open
    price_table['open_close'] = price_table['Close'] - price_table['Open']
    # High-Low
    price_table['range'] = price_table['High'] - price_table['Low']
    # 収益率 当日Close-前日Close
    price_table['return_dc'] = price_table['diff_close'] / price_table['AdjClose'].shift(1) * 100
    # 収益率 Close-Open
    price_table['return_oc'] = price_table['open_close'] / price_table['Open'] * 100
    # 比率 range/Open
    price_table['ratio_range'] = price_table['range'] / price_table['Open'] * 100
    
    # 対数系列
    # 対数化 AdjClose
    price_table['log_price'] = np.log(price_table['AdjClose'])
    # 対数差収益率 当日Close-前日Close
    price_table['log_return_dc'] = price_table['log_price'].diff(1) * 100
    # 対数差収益率 Close-Open
    price_table['log_return_oc'] = (np.log(price_table['Close']) - np.log(price_table['Open'])) * 100
    # 対数化 High-Low
    # price_table['log_range'] = np.log(price_table['High']) - np.log(price_table['Low'])
    # 対数化比率 range / Open
    # price_table['log_ratio_range'] = (np.log(price_table['High']) - np.log(price_table['Low'])) / np.log(price_table['Open']) * 100
    
    return price_table