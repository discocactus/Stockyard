
# coding: utf-8

# # Memo
2939 8287 マックスバリュ西日本 までyahooから銘柄名を取得
2940 から 2959 までは東証のエクセル由来のテーブルから取得に変更
2960 8345 岩手銀行 からyahooからの取得に戻した

3318 メガネスーパー 10/26 上場廃止
Pro Market銘柄はYahooにはないらしい
# # TODO
11/10以前の読み込み文はAdjCloseがInt型で保存されているので、Float型に再計算
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


# # MySQLに接続

# In[ ]:

sql = stock.sql()


# In[ ]:

help(sql)


# # ヒストリカルデータの初回連続読み込み

# ## TODO 価格データ読み込み済みリストの作成、次に読み込む銘柄コードの自動取得化

# In[ ]:

done = yahoo_stock_code
done


# ## 連続読み込み用内国株のコードリスト作成

# In[ ]:

start_index = 3100
increase_number = 100
end_index = start_index + increase_number

reading_code = sql.get_yahoo_stock_code(start_index, end_index)
print(reading_code[-10:])
print('Next start from {0}'.format(start_index + increase_number))


# ## 新規上場銘柄読み込み用コードリスト作成

# In[ ]:

reading_code = sql.get_new_added_stock_code()
reading_code


# ## 任意選択読み込み用コードリスト作成

# In[ ]:

read_start = 71
reading_code = keep_failed[read_start : ]#read_start + 8]
reading_code += list(yahoo_stock_table['code'][1600 : 1700])
reading_code, len(reading_code), 


# ## 失敗分読み込み用コードリスト作成

# In[ ]:

reading_code = failed
reading_code, len(reading_code)


# ## 連続読み込み書き込み

# In[ ]:

# 読み込み期間の設定
start = '2000-01-01'
end = None

# ロガー設定
start_time = dt.datetime.now()
logging.basicConfig(filename='get_price_{0}.log'.format(start_time.strftime('%Y-%m-%d')), filemode='w', level=logging.INFO)
logging.info('{0} get_price Started'.format(start_time.strftime('%Y-%m-%d %H:%M:%S')))

sql = stock.sql() # MySQLに接続するクラスインスタンスを作成

info = sql.get_info('yahoo_info') # 保存済み info の読み込み
failed = [] # 読み込みに失敗した銘柄のコードを書き込むリストを作成
save_failed = [] # 保存のみ失敗した分

# 連続読み込み書き込み
for index in range(len(reading_code)):
    code = reading_code[index]
    
    try:
        time.sleep(5)
        
        # Yahooファイナスンスから時系列情報と銘柄名を取得
        tmp_price, stock_name = stock.get_price_yahoojp(code, start=start, end=end)
        
        # 価格と価格以外の情報を分離
        tmp_info = tmp_price[tmp_price.isnull().any(axis=1)].reset_index()
        if len(tmp_info) > 0:
            new_info = stock.reform_info(tmp_info, code, stock_name)
            info = info.append(new_info, ignore_index=True)
        
            price = stock.extract_price(tmp_price)
            
        else:
            price = tmp_price # 価格以外の情報がなければそのまま
            
        try:
            # CSVで保存
            price.to_csv('/Users/Really/Stockyard/_yahoo_csv/t_{0}.csv'.format(code))
            info.to_csv('/Users/Really/Stockyard/_csv/yahoo_info.csv')
            # MySQLに保存
            sql.write_price(code, price)
            sql.write_info('yahoo_info', info)
          
            print('{0}: Success {1}'.format(index, code))
            
        except Exception as e:
            logging.warning('{0} {1}: {2}'.format(dt.datetime.now().strftime('%Y-%m-%d %H:%M:%S'), code, e))
            save_failed.append(code)
            print('{0}: Failed in {1} at Save Data'.format(index, code))
            print(e)
            
    except Exception as e:
        logging.warning('{0} {1}: {2}'.format(dt.datetime.now().strftime('%Y-%m-%d %H:%M:%S'), code, e))
        failed.append(code)
        print('{0}: Failed in {1} at get_price'.format(index, code))
        print(e)

print('Failed in {0} stocks at get:'.format(len(failed)))
print(failed)
print('Failed in {0} stocks at save:'.format(len(save_failed)))
print(save_failed)

# 最後にinfoの重複と順序を整理してから再度保存
info = info.drop_duplicates()
info = info.sort_values(by=['Code', 'Date'])
info.to_csv('/Users/Really/Stockyard/_csv/yahoo_info.csv')
sql.write_info('yahoo_info', info)

logging.info('{0} get_price Finished'.format(dt.datetime.now().strftime('%Y-%m-%d %H:%M:%S')))


# ## 変数を別名でキープ

# In[ ]:

keep_info = info
keep_info


# In[ ]:

keep_failed = failed
keep_failed


# ## キープ分の銘柄コードをcsvに保存 (履歴なし随時処理)

# In[ ]:

keep_info.to_csv('/Users/Really/Stockyard/_csv/keep_info.csv')
sql = stock.sql()
sql.write_info('keep_info', keep_info)


# In[ ]:

pd.Series(keep_failed).to_csv('/Users/Really/Stockyard/_csv/keep_failed.csv')


# # 更新

# ## 更新する銘柄コードリスト作成

# In[ ]:

start_index = 0


# In[ ]:

start_index = end_index
end_index


# In[ ]:

increase_number = 10
end_index = None
# end_index = start_index + increase_number

# reading_code = sql.get_yahoo_stock_code()
reading_code = sql.get_yahoo_stock_code(start_index, end_index)
print(reading_code[-10:])
print(len(reading_code))
print('Next start from {0}'.format(end_index))


# ## 連続更新

# __TODO 3542銘柄一括更新で3時間以上かかる。要検討 (うち、銘柄間1秒スリープ分合計1時間弱)__

# In[ ]:

end = None  # 読み込み終了日

# ロガー設定
start_time = dt.datetime.now()
logging.basicConfig(filename='get_price_{0}.log'.format(start_time.strftime('%Y-%m-%d')), filemode='w', level=logging.INFO)
logging.info('{0} add_new_price Started'.format(start_time.strftime('%Y-%m-%d %H:%M:%S')))

sql = stock.sql() # MySQLに接続するクラスインスタンスを作成

info = sql.get_info('yahoo_info') # 保存済み info の読み込み
failed = [] # 読み込みに失敗した銘柄のコードを書き込むリストを作成
save_failed = [] # 保存のみ失敗した分

# 連続読み込み書き込み
for index in range(len(reading_code)):
    code = reading_code[index]
    
    try:
        time.sleep(0)
        
        # 何か問題があるようなら最終更新日以降のデータがない場合にパスする処理を考える
        # 現状ではYahooにデータが無い場合、"No objects to concatenate" が帰って来る
        price = sql.get_price(code)
        last_date = price.index[-1]
        start = str((price.index[-1] + offsets.Day()).date())
        
        # Yahooファイナスンスから時系列情報と銘柄名を取得
        tmp_price, stock_name = stock.get_price_yahoojp(code, start=start, end=end)
        
        # 価格と価格以外の情報を分離
        tmp_info = tmp_price[tmp_price.isnull().any(axis=1)].reset_index()
        if len(tmp_info) > 0:
            new_info = stock.reform_info(tmp_info, code, stock_name)
            info = info.append(new_info, ignore_index=True)
        
            new_price = stock.extract_price(tmp_price)
            
        else:
            new_price = tmp_price # 価格以外の情報がなければそのまま
            
        price = price.append(new_price)
        
        try:
            # CSVで保存
            price.to_csv('/Users/Really/Stockyard/_yahoo_csv/t_{0}.csv'.format(code))
            info.to_csv('/Users/Really/Stockyard/_csv/yahoo_info.csv')
            # MySQLに保存
            sql.write_price(code, price)
            sql.write_info('yahoo_info', info)
          
            print('{0}: Success {1}'.format(index, code))
            
        except Exception as e:
            logging.warning('{0} {1}: {2}'.format(dt.datetime.now().strftime('%Y-%m-%d %H:%M:%S'), code, e))
            save_failed.append(code)
            print('{0}: Failed in {1} at Save Data'.format(index, code))
            print(e)
            
    except Exception as e:
        logging.warning('{0} {1}: {2}'.format(dt.datetime.now().strftime('%Y-%m-%d %H:%M:%S'), code, e))
        failed.append(code)
        print('{0}: Failed in {1} at get_price'.format(index, code))        
        print(e)

print('Failed in {0} stocks at get:'.format(len(failed)))
print(failed)
print('Failed in {0} stocks at save:'.format(len(save_failed)))
print(save_failed)

# 最後にinfoの重複と順序を整理してから再度保存
info = info.drop_duplicates()
info = info.sort_values(by=['Code', 'Date'])
info.to_csv('/Users/Really/Stockyard/_csv/yahoo_info.csv')
sql.write_info('yahoo_info', info)

logging.info('{0} add_new_price Finished'.format(dt.datetime.now().strftime('%Y-%m-%d %H:%M:%S')))


# In[ ]:

for i in range(increase_number):
    print('[{0}]'.format(reading_code[i]))
    print(sql.statement_query('select * from t_{0} order by Date desc limit 3'.format(reading_code[i])))


# # 単一銘柄、保存なし版

# ## 単一銘柄の読み込み

# In[ ]:

code = 9284 # 銘柄コード

# 読み込み期間の設定
start = '2017-10-01'
end = None

# ロガー設定
start_time = dt.datetime.now()
logging.basicConfig(filename='get_price_{0}.log'.format(start_time.strftime('%Y-%m-%d')), filemode='w', level=logging.INFO)

sql = stock.sql() # MySQLに接続するクラスインスタンスを作成

info = sql.get_info('yahoo_info') # 保存済み info の読み込み
failed = [] # 読み込みに失敗した銘柄のコードを書き込むリストを作成

# 読み込み
try:
    # Yahooファイナスンスから時系列情報と銘柄名を取得
    tmp_price, stock_name = stock.get_price_yahoojp(code, start=start, end=end)

    # 価格と価格以外の情報を分離
    tmp_info = tmp_price[tmp_price.isnull().any(axis=1)].reset_index()
    if len(tmp_info) > 0:
        new_info = stock.reform_info(tmp_info, code, stock_name)
        info = info.append(new_info, ignore_index=True)

        price = stock.extract_price(tmp_price)

    else:
        price = tmp_price # 価格以外の情報がなければそのまま

except Exception as e:
    logging.warning('{0} {1}: {2}'.format(dt.datetime.now().strftime('%Y-%m-%d %H:%M:%S'), code, e))
    failed.append(code)
    print('Failed in {0} at get_price'.format(code))
    print(e)

print('Failed in {0} stocks at get:'.format(len(failed)))
print(failed)


# In[ ]:

price


# In[ ]:

info


# ## 単一銘柄の更新

# In[ ]:

code = 1301 # 銘柄コード
end = None # 読み込み終了日

# ロガー設定
start_time = dt.datetime.now()
logging.basicConfig(filename='get_price_{0}.log'.format(start_time.strftime('%Y-%m-%d')), filemode='w', level=logging.INFO)

sql = stock.sql() # MySQLに接続するクラスインスタンスを作成

info = sql.get_info('yahoo_info') # 保存済み info の読み込み
failed = [] # 読み込みに失敗した銘柄のコードを書き込むリストを作成

try:
    price = sql.get_price(code)
    last_date = price.index[-1]
    start = str((price.index[-1] + offsets.Day()).date())
    
    # Yahooファイナスンスから時系列情報と銘柄名を取得
    tmp_price, stock_name = stock.get_price_yahoojp(code, start=start, end=end)

    # 価格と価格以外の情報を分離
    tmp_info = tmp_price[tmp_price.isnull().any(axis=1)].reset_index()
    if len(tmp_info) > 0:
        new_info = stock.reform_info(tmp_info, code, stock_name)
        info = info.append(new_info, ignore_index=True)

        new_price = stock.extract_price(tmp_price)

    else:
        new_price = tmp_price # 価格以外の情報がなければそのまま
        
    price = price.append(new_price)

except Exception as e:
    logging.warning('{0} {1}: {2}'.format(dt.datetime.now().strftime('%Y-%m-%d %H:%M:%S'), code, e))
    failed.append(code)
    print('Failed in {0} at get_price'.format(code))
    print(e)

print('Failed in {0} stocks at get:'.format(len(failed)))
print(failed)


# # CSVからSQLへ(SQL書き込みに失敗した分用)

# In[ ]:

tmp_failed = save_failed


# In[ ]:

sql = stock.sql()

for tmp_index in range(len(tmp_failed)):
    tmp_code = tmp_failed[tmp_index]
    tmp_price = pd.read_csv('/Users/Really/Stockyard/_yahoo_csv/t_{0}.csv'.format(tmp_code), index_col='Date')
    tmp_price = tmp_price[~tmp_price.index.duplicated()] # 重複している値が True となっているため ~ で論理否定をとって行選択する
    sql.write_price(tmp_code, tmp_price)
    print(tmp_code)


# # 読み込み済み価格データのCSVが保存されているかどうかの確認

# In[ ]:

# original table
yahoo_stock_table = pd.read_sql_table('yahoo_stock_table', engine, index_col=None).drop('index', axis=1)

table_index = list(yahoo_stock_table['code'][ : 2200])
len(table_index), end_index, table_index[-10:]


# In[ ]:

# _yahoo_csvフォルダ内の価格データ一覧をリスト化

import os

csv_table = os.listdir('/Users/Really/Stockyard/_yahoo_csv')[4:] # TODO スキップするファイル数を指定するのではなく正規表現で書き直す
print(csv_table)


# In[ ]:

type(list(map(int, csv_table[0]))), type(csv_table) #, type(table_index[0])


# In[ ]:

# 銘柄コードのみ抽出
# TODO 4桁以上のコードもあるので正規表現で書き直す
for t in range(len(csv_table)):
    csv_table[t] = csv_table[t][2:6]


# In[ ]:

# オブジェクト、データの型の変換例
list(map(int, csv_table))[:5]


# In[ ]:

len(csv_table), len(table_index)


# In[ ]:

# 内容が同一かを確認するためデータフレーム化
#TODO もっといいやり方があるはず
table_df = pd.DataFrame([table_index, csv_table])
table_df


# In[ ]:

table_df.ix[0].astype(str) == table_df.ix[1]


# # 価格データ以外の情報を格納するinfoテーブルを作成、読み込み

# ## (最初だけ。2回目以降は作成不要。上書き注意)

# In[ ]:

info = pd.DataFrame(index=[], columns=['Code', 'StockName', 'Date', 'Open', 'High', 'Low', 'Close', 'Volume', 'AdjClose'])
info = info.astype({'Code': int}) # int型を代入してもなぜかfloat型になってしまうので、あらかじめ明示しておく
info


# ## MySQLに保存済みのinfoテーブルの読み込み (2回目以降、現行使用版)

# In[ ]:

info = sql.get_info('yahoo_info')
info


# ## 価格データの読み込み前にMySQLに保存済みのinfoテーブルとの結合が必要な場合

# In[ ]:

info_sql = sql.get_info('yahoo_info')
info = info.append(info_sql).sort_values(by=['Code', 'Date']).reset_index(drop=True)
info['Date'] = pd.to_datetime(info['Date'])


# ## infoの内容を確認、修正

# In[ ]:

info


# In[ ]:

info = info.drop_duplicates()


# In[ ]:

info = info.sort_values(by=['Code', 'Date'])


# In[ ]:

info.duplicated().any()


# In[ ]:

info[info.duplicated()]


# In[ ]:

info.to_csv('/Users/Really/Stockyard/_csv/yahoo_info.csv')


# In[ ]:

sql.write_info('yahoo_info', info)


# # MySQLのみ保存済み、csvに書き出していない価格データを処理 (処理済み)

# In[ ]:

sql_to_csv_list = domestic_stock_table.ix[:99, 1]

for i in range(len(sql_to_csv_list)):
    price_sql = pd.read_sql_table('t_{0}'.format(sql_to_csv_list[i]), engine, index_col='Date')
    price_sql.to_csv('/Users/Really/Stockyard/_yahoo_csv/t_{0}.csv'.format(sql_to_csv_list[i]))


# #  クラス不使用版コード

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


# ## MySQLに保存済みのinfoテーブルの読み込み

# In[ ]:

# クラスを使わない場合
# テーブル全体を選択しているので read_sql_table を使用するのと同じこと
statement = "SELECT * FROM info"
info = pd.read_sql_query(statement, engine, index_col=None).drop('index', axis=1)
info['Date'] = pd.to_datetime(info['Date'])
info


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


# # いらなくなったコードの保管場所

# In[ ]:

# 列単位で個別に名称を変更する場合
all_stock_table = all_stock_table.rename(columns={'市場・商品区分': 'market'})

# marketの値を指定して選択
len(all_stock_table.query("market == '市場第一部（内国株）' | market == '市場第二部（内国株）'"))

