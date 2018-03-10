
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

# 株式
yahoo_stock_table = stock.get_stock_table_yahoojp()
yahoo_stock_table.columns = ['code', 'market', 'name', 'price', 'extra']
yahoo_stock_table.to_csv('{0}/yahoo_stock_table.csv'.format(csv_path))

# ETF
yahoo_etf_table = stock.get_etf_table_yahoojp()
yahoo_etf_table.columns = ['code', 'market', 'name', '連動対象', '価格更新日時','price',
                           '前日比', '前日比率', '売買単位','運用会社', '信託報酬（税抜）']
yahoo_etf_table.to_csv('{0}/yahoo_etf_table.csv'.format(csv_path))