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


def get_stooq_ticker():
    result = []
    p = 1
    base = 'https://stooq.com/t/tr/?l={0}'

    while True:
    # while p < 4: # テスト用
        url = base.format(p)
        # 'https://stooq.com/t/tr/?o=4&l=1' # 2018-04-01 l=1195まで
        print('{0}: {1}'.format(p, url))
        tables = get_table(url)
        if len(tables[25]) == 0:
            break
        result.append(tables[25])
        p += 1
        
    result = pd.concat(result, ignore_index=True)
    result = result[['Ticker', 'Market', 'Price change 1D']]
    result.columns = ['ticker', 'name', 'market']
    # result['market'] = result['market'].fillna("")
    result = result.fillna('none')
    result = result.sort_values(by=['market', 'name'])
    result = result.drop_duplicates().reset_index(drop=True)
    
    return result
    
    
def get_yahoo_code(start_index=0, end_index=None, csv_path=csv_path):
    table = pd.read_csv('{0}/yahoo_stock_table.csv'.format(csv_path), index_col=0)
    table = table.append(pd.read_csv('{0}/yahoo_etf_table.csv'.format(csv_path), index_col=0))
    table = table.drop_duplicates('code')
    table = table.sort_values(by=['code']).reset_index(drop=True)

    if (end_index == None) or (end_index > len(table)):
        end_index = len(table)

    result = list(table['code'][start_index : end_index])
    # result = table.loc[start_index:end_index, ['code', 'market_code']]

    return result


def get_yahoo_info(csv_path=csv_path):
    result = pd.read_csv('{0}/yahoo_info.csv'.format(csv_path), index_col=0)
    result['Date'] = pd.to_datetime(result['Date'])

    return result

    
def get_yahoo_price(code, price_path=price_path):
    result = pd.read_csv('{0}/y_{1}.csv'.format(price_path, code), index_col=0)
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
    
    result.columns = ['code', 'market', 'name', 'price', 'extra']
    
    result['code'] = result['code'].astype(int)
    
    result['market_code'] = 'T'
    result.loc[result['market'].fillna("").str.contains('名'), 'market_code'] = 'N'
    result.loc[result['market'].fillna("").str.contains('大'), 'market_code'] = 'O'
    result.loc[result['market'].fillna("").str.contains('札'), 'market_code'] = 'S'
    result.loc[result['market'].fillna("").str.contains('福'), 'market_code'] = 'F'
        
    return result


def get_etf_table_yahoojp():
    result = []
    p = 1
    base = 'https://stocks.finance.yahoo.co.jp/etf/list/?p={0}'

    while True:
        url = base.format(p)
        # 'https://stocks.finance.yahoo.co.jp/etf/list/?p=1' # 2018-03-09 p=5まで
        print('{0}: {1}'.format(p, url))
        tables = get_table(url)
        if len(tables[0]) == 0:
            break
        result.append(tables[0].iloc[:-1, :])
        p += 1
        
    result = pd.concat(result, ignore_index=True)
    
    result.columns = ['code', 'market', 'name', '連動対象', '価格更新日時','price',
                      '前日比', '前日比率', '売買単位','運用会社', '信託報酬（税抜）']
    
    result['code'] = result['code'].astype(int)
    
    result['market_code'] = 'T'
    result.loc[result['market'].fillna("").str.contains('名'), 'market_code'] = 'N'
    result.loc[result['market'].fillna("").str.contains('大'), 'market_code'] = 'O'
    result.loc[result['market'].fillna("").str.contains('札'), 'market_code'] = 'S'
    result.loc[result['market'].fillna("").str.contains('福'), 'market_code'] = 'F'
        
    return result

  
def get_price_yahoojp(code, market_code, start=None, end=None, interval='d'):
    # 参考: http://sinhrks.hatenablog.com/entry/2015/02/04/002258
    # 参考: http://jbclub.xii.jp/?p=598
    
    # urlを準備
    base = 'http://info.finance.yahoo.co.jp/history/?code={0}.{1}&{2}&{3}&tm={4}&p={5}'

    # 取得期間の設定
    # start
    if start == None:
        start = pd.to_datetime('1980-01-01')
    else :
        start = pd.to_datetime(start)
    start = 'sy={0}&sm={1}&sd={2}'.format(start.year, start.month, start.day) # 'sy=2017&sm=1&sd=1'
    
    # end
    if end == None:
        end = pd.to_datetime(pd.datetime.now())
    else :
        end = pd.to_datetime(end)
    end = 'ey={0}&em={1}&ed={2}'.format(end.year, end.month, end.day)
    
    # 変数の設定
    p = 1
    tmp_result = []

    # インターバルの設定
    if interval not in ['d', 'w', 'm', 'v']:
        raise ValueError("Invalid interval: valid values are 'd', 'w', 'm' and 'v'")

    # webサイトから価格テーブルを取得
    while True:
        url = base.format(code, market_code, start, end, interval, p)
        # print(url)
        # https://info.finance.yahoo.co.jp/history/?code=7203.T&sy=2000&sm=1&sd=1&ey=2017&em=10&ed=13&tm=d&p=1
        tables = get_table(url)
        if len(tables) < 2 or len(tables[1]) == 0:
            # print('break')
            break
        tmp_result.append(tables[1]) # ページ内の3つのテーブルのうち2番目のテーブルを連結
        p += 1
        # print(p)
        
    # 整形処理
    result = pd.concat(tmp_result, ignore_index=True) # インデックスをゼロから振り直す
    result.columns = ['Date', 'Open', 'High', 'Low', 'Close', 'Volume', 'AdjClose'] # 列名を変更
    if interval == 'm':
        result['Date'] = pd.to_datetime(result['Date'], format='%Y年%m月')
    else:
        result['Date'] = pd.to_datetime(result['Date'], format='%Y年%m月%d日') # 日付の表記を変更
    result = result.set_index('Date') # インデックスを日付に変更
    result = result.sort_index()
    
    # 銘柄名を変数に格納
    stock_name = tables[0].columns[0]
    # print([code, stock_name])
    
    return [result, stock_name]


def extract_price(tmp_price):
    tmp_price = tmp_price[~tmp_price.isnull().any(axis=1)]
        
    # なぜか日付が重複した行が入る場合があるので確認、削除
    tmp_price = tmp_price[~tmp_price.index.duplicated()]
        
    # 数値の列の数値以外の欠損値文字列 ('---' 等) を NaN に置換
    for col in tmp_price:
        if tmp_price[col].dtypes == object:
            tmp_price.loc[tmp_price[col].str.isnumeric() == False, col] = np.nan
            
    # NaN が入る場合があるので価格列は float で統一
    tmp_price = tmp_price.astype(float)
        
    return tmp_price


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


def calc_adj_close(code):
    info = get_yahoo_info()
    price = get_yahoo_price(code)
    
    if info['Code'].isin([code]).any():
        info['s_rate'] = info['Open'].str.extract('-> ([0-9.]*)株', expand=True).astype(float)
        info = info[info['Code'] == code].set_index('Date')

        price['s_rate'] = info['s_rate']
        price['s_rate'] = price['s_rate'].fillna(1)
        price['a_rate'] = 1.0
        # yahoo の正確な計算式は不明。誤差が発生する銘柄もある。桁数だけの問題ではなさそう。
        for i in reversed(range(len(price) - 1)):
            price['a_rate'][i] = price['a_rate'][i + 1] / price['s_rate'][i + 1]
        price['AdjClose'] = np.round(price['Close'] * price['a_rate'], 2)
        price = price[['Open', 'High', 'Low', 'Close', 'Volume', 'AdjClose']]
    else:
        print('code {0} has no info.'.format(code))
    
    return price