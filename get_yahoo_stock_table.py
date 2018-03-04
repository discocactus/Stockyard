
# coding: utf-8

# # import

import numpy as np
import pandas as pd

import stock


# # データパスの設定

# csv_path = '/Users/Really/Stockyard/_csv'
# csv_path = 'D:\stockyard\_csv'
csv_path = '/home/hideshi_honma/stockyard/_csv'


# # Yahooの銘柄一覧テーブルを取得

yahoo_stock_table = stock.get_stock_table_yahoojp()
yahoo_stock_table.columns = ['code', 'market', 'data', 'price', 'extra']
yahoo_stock_table = yahoo_stock_table[['code', 'market', 'data', 'price']]
yahoo_stock_table.to_csv('{0}/yahoo_stock_table.csv'.format(csv_path))