
# coding: utf-8

# # import

# In[ ]:


import numpy as np
import pandas as pd
import pandas.tseries.offsets as offsets
import datetime as dt
import time
import importlib
import logging
from retry import retry
#import traceback
#from retrying import retry
from sqlalchemy import create_engine
from sqlalchemy.types import Date, Integer, Float, Text
# from sqlalchemy.types import Integer
# from sqlalchemy.types import Text

import stock


# In[ ]:


importlib.reload(stock)


# In[ ]:


# pandas の最大表示列数を設定 (max_rows で表示行数の設定も可能)
pd.set_option('display.max_columns', 30)


# # MySQLに接続

# In[ ]:


sql = stock.sql()


# In[ ]:


help(sql)


# # 上場一覧から各種テーブルを作成 (メインはyahoo_stock_table)

# In[ ]:


file_month = 1712


# In[ ]:


# 東証のエクセルファイルを読み込む # http://www.jpx.co.jp/markets/statistics-equities/misc/01.html
all_stock_table = pd.read_excel('/Users/Really/Stockyard/_dl_data/data_j_{0}.xls'.format(file_month))
all_stock_table.columns = ['date', 'code', 'name', 'market', 'code_33', 'category_33', 'code_17', 'category_17', 'code_scale', 'scale'] # 列名を変更


# In[ ]:


all_stock_table


# In[ ]:


# marketの種別で集計
all_stock_table.groupby('market').count()


# In[ ]:


# 上場一覧のテーブル保存
all_stock_table.to_csv('/Users/Really/Stockyard/_csv/all_stock_table.csv')
sql.write_table('all_stock_table', all_stock_table)
# all_stock_table.to_sql('all_stock_table', engine, if_exists='replace')


# In[ ]:


# PRO Marketを除いたテーブルの作成 (YahooにはPRO Marketのデータはない)
yahoo_stock_table = all_stock_table.ix[~all_stock_table['market'].str.contains('PRO Market')].reset_index(drop=True)


# In[ ]:


yahoo_stock_table


# In[ ]:


# PRO Marketを除いたテーブルの保存
yahoo_stock_table.to_csv('/Users/Really/Stockyard/_csv/yahoo_stock_table.csv')
sql.write_table('yahoo_stock_table', yahoo_stock_table)
# yahoo_stock_table.to_sql('yahoo_stock_table', engine, if_exists='replace')


# In[ ]:


# 内国株のテーブル作成
domestic_stock_table = all_stock_table.ix[all_stock_table['market'].str.contains('内国株')].reset_index(drop=True)


# In[ ]:


domestic_stock_table


# In[ ]:


# 内国株のテーブル保存
domestic_stock_table.to_csv('/Users/Really/Stockyard/_csv/domestic_stock_table.csv')
sql.write_table('domestic_stock_table', domestic_stock_table)
# domestic_stock_table.to_sql('domestic_stock_table', engine, if_exists='replace')


# In[ ]:


# 内国株, PRO Market 以外のテーブル作成
ex_stock_table = yahoo_stock_table[~yahoo_stock_table['code'].isin(domestic_stock_table['code'])].reset_index(drop=True)

# 正規表現を使った書き方の例。
# ex_stock_table = all_stock_table.ix[all_stock_table['market'].str.contains('[^内国株）PRO Market]...$')].reset_index(drop=True)
# 文字列末尾の合致検索では、'$' の前の '.' の数で検索する文字数 ('.' * n + '$') が決定されているっぽい。
# つまりこの場合だと 'PRO Market' で実際に合致が確認されているのは末尾4文字の 'rket' 。
# '内国株）', 'PRO Market' をそれぞれグループ化するために' ()' で括る必要はないみたい。(ただし括っても同じ結果になる) 

# 下の書き方だと最後の1文字しか見ないことになるので、外国株も除外されてしまう。
# ex_stock_table = all_stock_table.ix[all_stock_table['market'].str.contains('[^内国株）PRO Market]$')].reset_index(drop=True)
# 上はつまり下の書き方と同じこと。
# ex_stock_table = all_stock_table.ix[all_stock_table['market'].str.contains('[^）t]$')].reset_index(drop=True)


# In[ ]:


ex_stock_table


# In[ ]:


# marketの種別で集計
ex_stock_table.groupby('market').count()


# In[ ]:


# 内国株, PRO Market 以外のテーブル保存
ex_stock_table.to_csv('/Users/Really/Stockyard/_csv/ex_stock_table.csv')
sql.write_table('ex_stock_table', ex_stock_table)
# ex_stock_table.to_sql('ex_stock_table', engine, if_exists='replace')


# In[ ]:


# 外国株のテーブル作成
foreign_stock_table = all_stock_table.ix[all_stock_table['market'].str.contains('外国株')].reset_index(drop=True)


# In[ ]:


foreign_stock_table


# In[ ]:


# 外国株のテーブル保存
foreign_stock_table.to_csv('/Users/Really/Stockyard/_csv/foreign_stock_table.csv')
sql.write_table('foreign_stock_table', foreign_stock_table)


# In[ ]:


# 型の確認
pd.DataFrame([
    sql.read_table('all_stock_table').dtypes,
    sql.read_table('yahoo_stock_table').dtypes,
    sql.read_table('domestic_stock_table').dtypes,
    sql.read_table('ex_stock_table').dtypes,
    sql.read_table('foreign_stock_table').dtypes],
    index=['all', 'yahoo', 'domestic', 'ex', 'foreign'])


# # 上場一覧の更新

# ## 新旧ファイルの読み込み

# In[ ]:


file_month = 1712


# In[ ]:


# 東証のエクセルファイルを読み込む # http://www.jpx.co.jp/markets/statistics-equities/misc/01.html
new_stock_table = pd.read_excel('/Users/Really/Stockyard/_dl_data/data_j_{0}.xls'.format(file_month))
new_stock_table.columns = ['date', 'code', 'name', 'market', 'code_33', 'category_33', 'code_17', 'category_17', 'code_scale', 'scale'] # 列名を変更


# In[ ]:


new_stock_table


# In[ ]:


new_stock_table.dtypes


# In[ ]:


# 旧テーブルの読み込み # http://www.jpx.co.jp/markets/statistics-equities/misc/01.html
old_stock_table = pd.read_excel('/Users/Really/Stockyard/_dl_data/data_j_{0}.xls'.format(file_month - 1))
old_stock_table.columns = ['date', 'code', 'name', 'market', 'code_33', 'category_33', 'code_17', 'category_17', 'code_scale', 'scale'] # 列名を変更


# In[ ]:


old_stock_table


# In[ ]:


old_stock_table.dtypes


# ## 新規上場銘柄

# In[ ]:


# 新規上場銘柄のテーブル作成
new_added = new_stock_table[~new_stock_table['code'].isin(old_stock_table['code'])].reset_index(drop=True)


# In[ ]:


new_added


# In[ ]:


# 新規上場銘柄のテーブル保存
new_added.to_csv('/Users/Really/Stockyard/_csv/new_added_stock_table.csv')
sql.write_table('new_added_stock_table', new_added)
# new_added.to_sql('new_added', engine, if_exists='replace')


# In[ ]:


# 新規上場銘柄の履歴テーブルの読み込み
saved_added = sql.read_table('added_stock_table')


# In[ ]:


saved_added


# In[ ]:


saved_added.dtypes


# In[ ]:


# 新旧テーブルの連結
added_stock_table = saved_added.append(new_added).reset_index(drop=True)


# In[ ]:


added_stock_table


# In[ ]:


added_stock_table.dtypes


# In[ ]:


# 新規上場銘柄の履歴テーブル保存
added_stock_table.to_csv('/Users/Really/Stockyard/_csv/added_stock_table.csv')
sql.write_table('added_stock_table', added_stock_table)
# added_stock_table.to_sql('added_stock_table', engine, if_exists='replace')


# ## 上場廃止銘柄

# In[ ]:


# 上場廃止銘柄のテーブル作成
new_discontinued = old_stock_table[~old_stock_table['code'].isin(new_stock_table['code'])].reset_index(drop=True)


# In[ ]:


new_discontinued


# In[ ]:


# 上場廃止銘柄の履歴テーブルの読み込み
saved_discontinued = sql.read_table('discontinued_stock_table')


# In[ ]:


saved_discontinued


# In[ ]:


saved_discontinued.dtypes


# In[ ]:


# 新旧テーブルの連結
discontinued_stock_table = saved_discontinued.append(new_discontinued).reset_index(drop=True)


# In[ ]:


discontinued_stock_table


# In[ ]:


discontinued_stock_table.dtypes


# In[ ]:


# 上場廃止銘柄のテーブル保存
discontinued_stock_table.to_csv('/Users/Really/Stockyard/_csv/discontinued_stock_table.csv')
sql.write_table('discontinued_stock_table', discontinued_stock_table)
# discontinued_stock_table.to_sql('discontinued_stock_table', engine, if_exists='replace')


# # 連続読み込み用コードリストの作成例

# ## yahoo 連続読み込み用コードリスト作成

# In[ ]:


start_index = 3100
increase_number = 100
end_index = start_index + increase_number

reading_code = sql.get_yahoo_stock_code(start_index, end_index)
print(reading_code[-10:])
print('Next start from {0}'.format(start_index + increase_number))


# ## 新規銘柄読み込み用コードリスト作成

# In[ ]:


reading_code = sql.get_new_added_stock_code()
reading_code


# # クラス不使用版コード

# ## MySQLに接続

# In[ ]:


db_settings = {
    "host": 'localhost',
    # "database": 'StockPrice_Yahoo_1',
    "database": 'stockyard',
    "user": 'user',
    "password": 'password',
    "port":'3306'
}
engine = create_engine('mysql://{user}:{password}@{host}:{port}/{database}'.format(**db_settings))


# ## 読み込む内国株のコードリスト作成 (クラス不使用版)

# In[ ]:


# 内国株だけにする
# MySQLに保存済みの内国株テーブルから作成。今後はこちらを使用する
yahoo_stock_table = pd.read_sql_table('yahoo_stock_table', engine, index_col=None).drop('index', axis=1)

start_index = 2810
increase_number = 10
end_index = start_index + increase_number

reading_code = list(yahoo_stock_table['code'][start_index : end_index])
print(reading_code[-10:])
print('Next start from {0}'.format(start_index + increase_number))

