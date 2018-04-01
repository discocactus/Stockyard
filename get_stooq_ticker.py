
# coding: utf-8

# # import

import numpy as np
import pandas as pd

import stock


# # データパスの設定

# csv_path = '/Users/Really/Stockyard/_csv'
# csv_path = 'D:\stockyard\_csv'
csv_path = '/home/hideshi_honma/stockyard/_csv'


# # Stooq の Ticker 一覧テーブルを取得
stooq_ticker = stock.get_stooq_ticker()
stooq_ticker.to_csv('{0}/stooq_ticker.csv'.format(csv_path))