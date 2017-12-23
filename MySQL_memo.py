
# coding: utf-8

# # Import

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
from sqlalchemy.types import Date
from sqlalchemy.types import Integer
from sqlalchemy.types import Text

import stock


# In[ ]:

importlib.reload(stock)


# # MySQLに接続

# In[ ]:

sql = stock.sql()


# In[ ]:

help(sql)


# # MySQLに接続 (クラス不使用)

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


# # - Memo - MySQL クエリ

# In[ ]:

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

