
# coding: utf-8

import numpy as np
import pandas as pd
import pandas.tseries.offsets as offsets
import datetime as dt
import time
import importlib
import logging
from retry import retry

import stock


# csv_path = '/Users/Really/Stockyard/_csv'
# price_path = '/Users/Really/Stockyard/_yahoo_csv'
# csv_path = 'D:\stockyard\_csv'
# price_path = 'D:\stockyard\_yahoo_csv'
csv_path = '/home/hideshi_honma/stockyard/_csv'
price_path = '/home/hideshi_honma/stockyard/_yahoo_csv'


# # ヒストリカルデータの初回連続読み込み

# ## 連続読み込み用内国株のコードリスト作成
start_index = input('start_index=')
if start_index == 'failed':
    reading_code = list(
        pd.read_csv('{0}/failed.csv'.format(csv_path), header=None, index_col=0).values.flatten())
elif start_index == 'save_failed':
    reading_code = list(
        pd.read_csv('{0}/save_failed.csv'.format(csv_path), header=None, index_col=0).values.flatten())
else:
    start_index = int(start_index)
    increase_number = int(input('increase_number='))
    end_index = start_index + increase_number
    reading_code = stock.get_yahoo_code(start_index, end_index)
    print('tail 10: {0}'.format(reading_code[-10:]))
    print('Next start from {0}'.format(start_index + increase_number))


# ## 連続読み込み書き込み

# 読み込み期間の設定
start = input('start_date (yyyy-mm-dd) =')
end = input('end_date (1999-12-31) =')

# ロガー設定
start_time = dt.datetime.now()
logging.basicConfig(filename='get_price_{0}.log'.format(start_time.strftime('%Y-%m-%d')), filemode='w', level=logging.INFO)
logging.info('{0} get_price Started'.format(start_time.strftime('%Y-%m-%d %H:%M:%S')))

yahoo_table = pd.read_csv('{0}/yahoo_table.csv'.format(csv_path), index_col=0)
info = stock.get_yahoo_info() # 保存済み info の読み込み
failed = [] # 読み込みに失敗した銘柄のコードを書き込むリストを作成
save_failed = [] # 保存のみ失敗した分

# 連続読み込み書き込み
for index in range(len(reading_code)):
    code = reading_code[index]
    market_code = yahoo_table.loc[yahoo_table['code'] == code, 'market_code'].values[0]
    
    try:
        time.sleep(1)
        
        # Yahooファイナスンスから時系列情報と銘柄名を取得
        tmp_price, stock_name = stock.get_price_yahoojp(code, market_code, start=start, end=end)

        # 価格以外の情報を抽出
        tmp_info = tmp_price[tmp_price.isnull().any(axis=1)].reset_index()
        if len(tmp_info) > 0:
            new_info = stock.reform_info(tmp_info, code, stock_name)
            info = info.append(new_info, ignore_index=True)

        # 価格情報の整形と代入
        price = stock.extract_price(tmp_price)
            
        try:
            # CSVで保存
            price.to_csv('{0}/t_{1}.csv'.format(price_path, code))
            info.to_csv('{0}/yahoo_info.csv'.format(csv_path))
          
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
info = info.sort_values(by=['Code', 'Date']).reset_index(drop=True)
info.to_csv('{0}/yahoo_info.csv'.format(csv_path))

logging.info('{0} get_price Finished'.format(dt.datetime.now().strftime('%Y-%m-%d %H:%M:%S')))


# ## 失敗分の銘柄コードをcsvに保存 (履歴なし随時処理)
pd.Series(failed).to_csv('{0}/failed.csv'.format(csv_path))
pd.Series(save_failed).to_csv('{0}/save_failed.csv'.format(csv_path))
