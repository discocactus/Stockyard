
# coding: utf-8

#%%
# Import
import numpy as np
import pandas as pd
import pandas.tseries.offsets as offsets
import matplotlib.pyplot as plt
import datetime as dt
import time
import importlib
import logging
from retry import retry
from sqlalchemy import create_engine
from sqlalchemy.types import Date, Integer, Text

import stock

#%%
importlib.reload(stock)

#%%
# 'Date'列をインデックスに指定してCSVの読み込み、インデックスをdatetime型に変換
df_quote = pd.read_csv('/Users/Really/Stockyard/_csv/t_1758.csv', index_col='Date')
df_quote.index = pd.to_datetime(df_quote.index)
df_quote

df_quote.dtypes
df_quote.index

df_quote1 = pd.read_csv('/Users/Really/Stockyard/_csv/t_9983.csv', index_col='Date')
df_quote1.index = pd.to_datetime(df_quote1.index)
df_quote1

df_quote1.dtypes
df_quote1.index

df_9983 = stock.complement_price(df_quote1)

#%%
# 出来高ゼロの日はインデックスごと欠落しているので、ビジネスデイ(freq='B')のdatetime型インデックスにデータをあてはめる
# TODO 休日はどうするか要検討、japandas使ってみる
df_quote_fill = pd.DataFrame(df_quote, index=pd.date_range(df_quote.index[0], df_quote.index[-1], freq='B'))

# 欠損データを補完する
# Volumeをゼロで補完
df_quote_fill.Volume = df_quote_fill.Volume.fillna(0)

# Close,AdjCloseは前日データで補完
df_quote_fill[['Close', 'AdjClose']] = df_quote_fill[['Close', 'AdjClose']].fillna(method='ffill') 

# 欠損Openを前日補完済み前日Closeで補完 (下の2つはやり方の違い、結果は同じ)
df_quote_fill['Open'] = df_quote_fill['Open'].fillna(df_quote_fill['Close'].shift(1))
# df_quote_fill['Open'][1:] = df_quote_fill['Open'][1:].where(~df_quote_fill['Open'][1:].isnull(),
#                   np.array(df_quote_fill['Close'][:-1]))

# High,Low,CloseはOpenで補完
df_quote_fill[['Open', 'High', 'Low']] = df_quote_fill[['Open', 'High', 'Low']].fillna(method='ffill', axis=1)

df_quote_fill
df_quote_fill.isnull().any() # 欠損値の有無を列単位で確認

#%%
# 関数化
df_quote_fill = stock.complement_price(df_quote)

#%%
# 差分系列の作成

# 原系列
# 当日Close-前日Close
df_quote_fill['diff_close'] = df_quote_fill['AdjClose'].diff(1)
# Close-Open
df_quote_fill['open_close'] = df_quote_fill['Close'] - df_quote_fill['Open']
# High-Low
df_quote_fill['range'] = df_quote_fill['High'] - df_quote_fill['Low']
# 収益率 当日Close-前日Close
df_quote_fill['return_dc'] = df_quote_fill['diff_close'] / df_quote_fill['AdjClose'].shift(1) * 100
# 収益率 Close-Open
df_quote_fill['return_oc'] = df_quote_fill['open_close'] / df_quote_fill['Open'] * 100
# 比率 range/Open
df_quote_fill['ratio_range'] = df_quote_fill['range'] / df_quote_fill['Open'] * 100

# 対数系列
# 対数化 AdjClose
df_quote_fill['log_price'] = np.log(df_quote_fill['AdjClose'])
# 対数差収益率 当日Close-前日Close
df_quote_fill['log_return_dc'] = df_quote_fill['log_price'].diff(1) * 100
# 対数差収益率 Close-Open
df_quote_fill['log_return_oc'] = (np.log(df_quote_fill['Close']) - np.log(df_quote_fill['Open'])) * 100
# 対数化 High-Low
# df_quote_fill['log_range'] = np.log(df_quote_fill['High']) - np.log(df_quote_fill['Low'])
# 対数化比率 range / Open
# df_quote_fill['log_ratio_range'] = (np.log(df_quote_fill['High']) - np.log(df_quote_fill['Low'])) / np.log(df_quote_fill['Open']) * 100

#%%
# 関数化
df_quote_fill = stock.add_processed_price(df_quote_fill)

#%%
# plot
# %matplotlib inline
plt.style.use('ggplot')

# Line
df_quote_fill['AdjClose'].plot()
df_quote_fill[['return_dc', 'return_oc']].plot()
df_quote_fill['range'].plot()
df_quote_fill['ratio_range'].plot()
df_quote_fill['log_return_oc'].plot()
df_quote_fill[['return_oc', 'log_return_oc']].plot()

#%%
# 最大値とか
df_9983['Volume'].max()
df_9983['Volume'].idxmax()
df_9983['Volume'].ix[df_9983['Volume'].idxmax()]
np.where(df_9983['Volume'] == df_9983['Volume'].max())

#%%
# datetime型インデックスの作成例
dtidx = pd.date_range('2000-01-01', '2017-12-01', freq='B') # freq='B'はBusiness Day
dtidx

# all_stock_tableにあってdomestic_stock_tableにない'code'を持つ行を抽出
all_stock_table[~all_stock_table['code'].isin(domestic_stock_table['code'])]

# marketの種別で集計
all_stock_table.groupby('market').count()

# 指定した値をNaNに置換、NaNはfloat型
all_stock_table.replace('-', np.NaN)

#%%
# MySQLに接続
sql = stock.sql()

help(sql)

#%%
# MySQLに接続 (クラス不使用)
db_settings = {
    "host": 'localhost',
    "database": 'StockPrice_Yahoo_1',
    "user": 'user',
    "password": 'password',
    "port":'3306'
}
engine = create_engine('mysql://{user}:{password}@{host}:{port}/{database}'.format(**db_settings))

#%%
# - Memo - MySQL クエリ
# mysql>
"""
CREATE DATABASE StockPrice_Yahoo_1 DEFAULT CHARACTER SET utf8mb4;
GRANT ALL ON StockPrice_Yahoo_1.* TO user@localhost IDENTIFIED BY 'password';
show databases;
use StockPrice_Yahoo_1;
show tables;
drop tables
drop database
select*from
show columns from # テーブルの中に含まれるカラムの情報を取得する
select*from t_1382 where Date between '2013-12-01' and '2013-12-31';
select * from t_8848 order by Date desc limit 5; # 最後の5行
"""

