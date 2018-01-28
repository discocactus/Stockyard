
# coding: utf-8

# # - Memo - MySQL クエリ
# mysql>
CREATE DATABASE StockPrice_Yahoo_1 DEFAULT CHARACTER SET utf8mb4;
GRANT ALL ON StockPrice_Yahoo_1.* TO user@localhost IDENTIFIED BY 'password';
GRANT ALL ON stockyard.* TO user@localhost IDENTIFIED BY 'password';
show databases;
use StockPrice_Yahoo_1;
show tables;
drop tables
drop database
select*from
show columns from # テーブルの中に含まれるカラムの情報を取得する
select*from t_1382 where Date between '2013-12-01' and '2013-12-31';
select * from t_8848 order by Date desc limit 5; # 最後の5行
show variables like 'datadir';
show variables like 'char%';show variables like 'char%';

character_set_client : クライアント側で発行したsql文はこの文字コードになる
character_set_connection : クライアントから受け取った文字をこの文字コードへ変換する
character_set_database : 現在参照しているDBの文字コード
character_set_results : クライアントへ送信する検索結果はこの文字コードになる
character_set_server : DB作成時のデフォルトの文字コード
character_set_system : システムの使用する文字セットで常にutf8が使用されている

# # Import

# In[ ]:


from __future__ import unicode_literals
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
import sqlalchemy
import sqlalchemy.orm
from sqlalchemy import create_engine
from sqlalchemy.types import Date
from sqlalchemy.types import Integer
from sqlalchemy.types import Text

import stock


# In[ ]:


get_ipython().run_line_magic('load_ext', 'line_profiler')


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


# # MySQLに接続 (クラス不使用)

# In[ ]:


db_settings = {
    "db": 'mysql', # ドライバーは mysqldb になる。mysqlclient のこと？
    # "db": 'mysql+mysqlconnector',
    # "db": 'mysql+pymysql',
    # "host": 'localhost',
    "host": '127.0.0.1',
    # "host": 'MyCon',
    # "database": 'StockPrice_Yahoo_1',
    "database": 'stockyard',
    "user": 'user',
    "password": 'password',
    "port": '3306',
    "charset": '?charset=utf8mb4'
}
# engine = create_engine('mysql://{user}:{password}@{host}:{port}/{database}'.format(**db_settings))
engine = create_engine('{db}://{user}:{password}@{host}:{port}/{database}{charset}'.format(**db_settings))


# In[ ]:


engine.url.get_driver_name()


# # pandasへデータの読み込み

# In[ ]:


pd.read_sql_query("select*from t_1301", engine, index_col='Date')


# In[ ]:


# なぜかとても時間がかかるので使用不可
pd.read_sql_table('t_1301', engine, index_col='Date')


# In[ ]:


# This function is a convenience wrapper around read_sql_table and read_sql_query.
# クエリが与えられた場合は read_sql_query として動作する
pd.read_sql("select*from t_1301", engine, index_col='Date')


# In[ ]:


# テーブル名が与えられた場合は read_sql_table として動作する
pd.read_sql("t_1301", engine, index_col='Date')


# In[ ]:


# DB の index 列をインデックスおよびラベル名として使用する。(インデックスとして適正な値かどうか注意が必要)
pd.read_sql('select*from yahoo_info', engine, index_col='index')


# In[ ]:


# DB の index 列を使用せず破棄する。インデックスはリセットされる。ラベル名なし。
pd.read_sql('select*from yahoo_info', engine, index_col=None).drop('index', axis=1)


# In[ ]:


# DB の id 列が 1 から振ってあるのでインデックス 0 のラベル名は 1 になる。
yf = pd.read_sql('select*from yahoo_fundamental', engine, index_col='id')
yf


# In[ ]:


# ラベル名に int は使用できないのかも？この場合ラベル名による行指定はエラーになる。
yf.loc['1']


# In[ ]:


yf.iloc[0:1]


# In[ ]:


sql.get_info('yahoo_info')


# In[ ]:


sql.get_new_added_stock_code()


# In[ ]:


sql.get_price(1301)


# In[ ]:


sql.get_yahoo_info()


# In[ ]:


sql.get_yahoo_stock_code()


# In[ ]:


sql.read_table('domestic_stock_table', index_col='index')


# In[ ]:


sql.read_table('kt_1301', 'index')


# In[ ]:


sql.read_table('yahoo_fundamental', 'id')


# In[ ]:


sql.statement_query('select*from yahoo_fundamental')


# # pandasから書き込み

# __全体的に遅く処理速度が不安定。要調査、修正。__

# In[ ]:


price = sql.get_price(1301)
price


# In[ ]:


# 遅い。5～6秒。何に時間がかかっているの要調査。
code = 1000
sql.write_price(code, price)


# In[ ]:


sql.get_price(1000)


# In[ ]:


# なぜか2秒以下だったこともあるが、ほとんどの場合4秒以上かかる。
# 型は問題なさそう。ただし別途 Date を Primary Key に設定する必要がある。
table_name = 't_1001'
price.to_sql(table_name, engine, if_exists='replace')


# In[ ]:


t_1001 = sql.get_price(1001)
t_1001


# In[ ]:


t_1001.dtypes


# In[ ]:


info = sql.get_yahoo_info()
info


# In[ ]:


table_name = 'test_info'
sql.write_info(table_name, info)


# In[ ]:


sql.get_info('test_info')


# In[ ]:


df = sql.read_table('kt_1301', 'index')
df


# In[ ]:


table_name = 'test_table'
table = df
sql.write_table(table_name, table)


# In[ ]:


sql.read_table('test_table', 'index')


# # 処理遅延の原因究明のための各種動作確認

# In[ ]:


pd.read_csv(r"C:\Users\Really\GitHub\Stockyard\_csv\info.csv")


# In[ ]:


import MySQLdb

conn = MySQLdb.connect(db='stockyard', user='user', passwd='password', charset='utf8mb4')

c = conn.cursor()

c.execute('SELECT * FROM t_1301')
for row in c.fetchall():
    print(row)
    
conn.close()


# In[ ]:


pd.read_sql_table('t_1301', conn, index_col='Date')


# In[ ]:


# セッションを作成
Session = sqlalchemy.orm.sessionmaker(bind=engine)
session = Session()


# In[ ]:


sql_table = session.execute("SELECT * FROM t_1301")
for v in sql_table:
   print(v)


# In[ ]:


get_ipython().run_cell_magic('writefile', 'sqlalchemy_test.py', '\nimport pandas as pd\nfrom sqlalchemy import create_engine\n\ndb_settings = {\n    # "host": \'localhost\',\n    # "host": \'127.0.0.1\',\n    "host": \'MyCon\',\n    # "database": \'StockPrice_Yahoo_1\',\n    "database": \'stockyard\',\n    "user": \'user\',\n    "password": \'password\',\n    "port":\'3306\'\n}\n# engine = create_engine(\'mysql://{user}:{password}@{host}:{port}/{database}\'.format(**db_settings))\n# engine = create_engine(\'mysql://{user}:{password}@{host}:{port}/{database}?charset=utf8mb4\'.format(**db_settings))\n# engine = create_engine(\'mysql+mysqlconnector://{user}:{password}@{host}:{port}/{database}?charset=utf8mb4\'.format(**db_settings))\nengine = create_engine(\'mysql+pymysql://{user}:{password}@{host}:{port}/{database}?charset=utf8mb4\'.format(**db_settings))\n\nsql_table = pd.read_sql_table(\'t_1301\', engine, index_col=\'Date\')\nprint(sql_table)')


# In[ ]:


get_ipython().run_cell_magic('writefile', 'mysql_test.py', '\nimport pandas as pd\nfrom sqlalchemy import create_engine\n\ndb_settings = {\n    # "host": \'localhost\',\n    "host": \'127.0.0.1\',\n    # "database": \'StockPrice_Yahoo_1\',\n    "database": \'stockyard\',\n    "user": \'user\',\n    "password": \'password\',\n    "port":\'3306\'\n}\nengine = create_engine(\'mysql://{user}:{password}@{host}:{port}/{database}\'.format(**db_settings))\n# engine = create_engine(\'mysql://{user}:{password}@{host}:{port}/{database}?charset=utf8mb4\'.format(**db_settings))\n\ndef read_sql():\n    x = pd.read_sql_table(\'t_1301\', engine, index_col=\'Date\')\n    return x')


# In[ ]:


import mysql_test


# In[ ]:


importlib.reload(mysql_test)


# In[ ]:


get_ipython().run_line_magic('lprun', '-T lprofo -f mysql_test.read_sql  mysql_test.read_sql()')


# In[ ]:


get_ipython().run_line_magic('lprun', '-T lprofo -f sql.get_price sql.get_price(1301)')


# In[ ]:


print(open('lprofo', 'r').read())

