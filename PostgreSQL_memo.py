
# coding: utf-8

# # memo

# 半角円記号(Shift-JIS / CHCP932) = バックスラッシュ(UTF-8 / CHCP65001?)  
# ターミナルの文字コードをCP932に設定しないと文字化けする  
# PostgreSQLではユーザーのことをロールと呼ぶらしい  
#   
# psql -U postgres # Login  
# 
# CREATE ROLE python WITH LOGIN PASSWORD 'password'; # ユーザー(ロール)pythonの作成  
# ALTER ROLE python with SUPERUSER; # pythonにSUPERUSER権限を付与  
# GRANT 権限 ON 対象 TO 誰に # 広範な権限を付与するための記述法がまだよくわからない  
# \du # ロールの詳細リスト  
# \x # 列を縦に展開表示(再入力で横に復帰)  
# SELECT * from pg_stat_activity; # プロセスIDの確認  
# SELECT pg_cancel_backend(プロセスID); # 実行中のプロセスの停止  
# Ctrl + C # sql文の中断  
# show data_directory; # データディレクトリを表示
# 

# __サービスの起動と停止__  
# pg_ctl start -D "D:\PostgreSQL\9.6\data"  
# pg_ctl stop -D "D:\PostgreSQL\9.6\data"  
# ※環境変数 PGDATA が指定してあればデータディレクトリ -D は不要かも  
#   
# __ターミナルを起動__  
# psql -d postgres # デフォルトのテーブルに接続  
# psql -d テーブル名 # 直接テーブルに接続  
#   
# __データベース__  
# \l # データベース一覧の表示  
# \c # データベース名  データベースの選択  
#   
# __テーブル__  
# \dt # テーブル一覧の表示  
# \d テーブル名; # テーブル構造の表示  
#   
# select * from テーブル名; # テーブル内のデータを一覧  
# select * from テーブル名 order by カラム; # 指定したカラムの内容を小さい順に表示  
# select * from テーブル名 order by カラム desc; # 指定したカラムの内容を大きい順に表示  
# select * from テーブル名 limit 数; # 表示数指定  
# select * from テーブル名 offset 数; # 表示の開始位置指定  
# select distinct カラム名 from テーブル名; # カラム内の任意の文字を表示  
# select sum(カラム名) from テーブル名; # カラム内の合計値  
# select max(カラム名) from テーブル名; # カラム内の最大値  
# select min(カラム名) from テーブル名; # カラム内の最小値  
# select avg(カラム名) from テーブル名; # カラム内の平均値  
#   
# drop テーブル名; # テーブルの削除  
# update テーブル名 set 更新内容; # データの更新  
# delete from テーブル名 where 条件; # データの削除  
# alter table テーブル名 owner to オーナー名; # テーブルのオーナーの変更  
#   
# __テーブル構造の変更__  
# alter table テーブル名 add カラム名 データ型; # カラムの追加  
# alter table テーブル名 drop カラム名; # カラムの削除  
# alter table テーブル名 rename カラム名 to 新カラム名; # カラム名の変更  
# alter table テーブル名 alter カラム名 type データ型; # カラムのデータ型を変更する  
#   
# __インデックス__  
# create index インデックス名 on テーブル名(カラム名); # インデックス追加  
# drop index インデックス名; # インデックス削除  
# 
# __view__  
# create view ビュー名 as viewに指定するコマンド; # viewの作成  
# \dv; # view一覧の確認  
# select * from ビュー名; # viewの使用方法  
# drop view ビュー名; # viewの削除  
#   
# __外部ファイルの読み込み__  
# \i # ファイル名SQL文を外部ファイルに書いて実行する時に使う  
#   
# __関数__  
# select length(カラム名) from テーブル名;文字数  
# select concat(文字列, 文字列, ...) from テーブル名;文字列連結  
# ロック中のプロセスの一覧表示を行うクエリ
SELECT l.pid, db.datname, c.relname, l.locktype, l.mode
FROM pg_locks l
        LEFT JOIN pg_class c ON l.relation=c.relfilenode
        LEFT JOIN pg_database db ON l.database = db.oid
ORDER BY l.pid;
# # import

# In[ ]:


import numpy as np
import pandas as pd
import pandas.tseries.offsets as offsets
import datetime as dt
import time
import importlib
# import logging
# from retry import retry
# import traceback
# from retrying import retry
import sqlalchemy
import sqlalchemy.orm
from sqlalchemy import create_engine
from sqlalchemy.types import Date, Integer, Float, Text

import stock


# In[ ]:


importlib.reload(stock)


# In[ ]:


# pandas の最大表示列数を設定 (max_rows で表示行数の設定も可能)
pd.set_option('display.max_columns', 30)


# # 接続

# In[ ]:


# SQLAlchemy
db_settings = {
    "db": 'postgresql', # デフォルトドライバーは psycopg2 になる。
    "user": 'python',
    "password": 'password',
    "host": 'localhost',
    "port": '5432',
    "database": 'stockyard'
}
engine = create_engine('{db}://{user}:{password}@{host}:{port}/{database}'.format(**db_settings))


# In[ ]:


engine


# In[ ]:


engine.url.get_driver_name()


# In[ ]:


# stock.psql(sql)クラス
psql = stock.psql()


# In[ ]:


stock.psql.engine


# In[ ]:


help(psql)


# # テスト用にMySQLからデータを読み込み

# In[ ]:


msql = stock.msql()


# In[ ]:


stock.msql.engine


# In[ ]:


help(msql)


# In[ ]:


myprice = msql.get_price(7203)
myprice


# In[ ]:


msql.write_price(1001, myprice)


# In[ ]:


price = msql.get_price(1001)
price


# # ちなみにcsvの方が断然速い。。。

# In[ ]:


# SSD
myprice.to_csv('t_test.csv')


# In[ ]:


# HDD
myprice.to_csv(r'D:\stockyard\t_test.csv')


# In[ ]:


csvprice = pd.read_csv(r'D:\stockyard\t_test.csv', index_col='Date')
csvprice.index = pd.to_datetime(price.index)
csvprice


# In[ ]:


# 日付インデックスの型の確認
type(csvprice.index[0])


# In[ ]:


# 日付による行指定も機能している
csvprice.loc['2000-01-04']


# In[ ]:


# ちゃんと日付の演算もできる形で読み書きできているらしい
print(csvprice.index[-1])
print((csvprice.index[-1] + offsets.Day()).date())


# # 読み書き

# In[ ]:


# Dateの型を明示しないと日付が日時になってしまう
table_name = 't_1001'
myprice.to_sql(table_name, engine, if_exists='replace')


# In[ ]:


price = pd.read_sql_query("select*from t_1001", engine, index_col='Date')
price.index = pd.to_datetime(price.index)
price


# In[ ]:


table_name = 't_1003'
dtype = {
    'Date': Date(),
    'Open': Integer(),
    'High': Integer(),
    'Low': Integer(),
    'Close': Integer(),
    'Volume': Integer(),
    'AdjClose': Float()
}
myprice.to_sql(table_name, engine, if_exists='replace', dtype=dtype)


# In[ ]:


price = pd.read_sql_query("select*from t_1003", engine, index_col='Date')
price.index = pd.to_datetime(price.index)
price


# In[ ]:


# 日付インデックスの型の確認
type(price.index[0])


# In[ ]:


# 日付による行指定も機能している
price.loc['2000-01-04']


# In[ ]:


# ちゃんと日付の演算もできる形で読み書きできているらしい
print(price.index[-1])
print((price.index[-1] + offsets.Day()).date())


# In[ ]:


help(psql)


# In[ ]:


price = psql.get_price(1003)
price


# In[ ]:


psql.write_price(1005, myprice)


# In[ ]:


price = psql.get_price(1005)
price

