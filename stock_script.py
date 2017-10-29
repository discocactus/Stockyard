
# coding: utf-8
from __future__ import unicode_literals
import numpy as np
import pandas as pd
import datetime as dt
import time
import importlib
import traceback
from sqlalchemy import create_engine
from sqlalchemy.types import Date
from sqlalchemy.types import Integer
from sqlalchemy.types import Text

import stock


# TODO クラス化する？
#  MySQLとの接続設定
db_settings = {
    "host": 'localhost',
    "database": 'StockPrice_Yahoo_1',
    "user": 'user',
    "password": 'password',
    "port": '3306'
}
engine = create_engine('mysql://{user}:{password}@{host}:{port}/{database}'.format(**db_settings))


# TODO この辺の一覧作成は関数化するかファイルからの読み込みにするか
# 上場一覧の作成
stock_list_all = pd.read_excel('data_j.xls')
stock_list_all.columns = ['date', 'code', 'name', 'market', 'code_33', 'category_33', 'code_17', 'category_17', 'code_scale', 'scale']

# 上場一覧をMySQLに書き込み
# TODO 型を合わせる
# stock_list_all.to_sql('stock_list_all', engine, if_exists='replace')


# 内国株だけのリスト作成
domestic_stock_code = list(stock_list_all.ix[stock_list_all['market'].str.contains('内国株'), 1])[160:170] #
domestic_stock_code
# TODO この辺まで

# 連続読み込み書き込み
start = '2000-01-01'
end = None
failed = []

for seq in range(len(domestic_stock_code)):
    code = domestic_stock_code[seq]
    try:
        result, test_result, stock_name = stock.get_quote_yahoojp(code, start=start, end=end)

        quote = result[result.isnull().any(axis=1) == False].astype(float).astype(int)
        # https://qiita.com/u1and0/items/fd2780813b690a40c197

        tmp_info = result[result.isnull().any(axis=1)].reset_index()
        if len(tmp_info) > 0:
            tmp_row = {'Code': code, 'StockName': stock_name}
            for event in range(len(tmp_info)):
                tmp_row.update(dict(tmp_info.iloc[event]))
                info = info.append(tmp_row, ignore_index=True)

        try:
            # TODO SQLへの書き込みは関数化orクラス化する?
            stock.to_mysql(code, quote)
            # sqlalchemy.typesで定義されたデータ型を辞書形式で設定
            dtype = {
                'Code': Integer(),
                'Company': Text(),
                'Date': Date(),
                'Open': Text()
            }
            info.to_sql('info', engine, if_exists='replace', dtype=dtype)

            quote.to_csv("".join(["/Users/Really/Stock/_csv/t_", str(code), ".csv"]))
            info.to_csv("".join(["/Users/Really/Stock/_csv/info.csv"]))

            print('{0}: Success {1}'.format(seq, code))

        except:
            traceback.print_exc()
            failed.append(code)

            print('{0}: Failed in {1} at Save Data'.format(seq, code))

    except:
        traceback.print_exc()
        failed.append(code)

        print('{0}: Failed in {1} at get_quote'.format(seq, code))

    time.sleep(10)

print('Failed in {0} stocks:'.format(len(failed)))
print(failed)