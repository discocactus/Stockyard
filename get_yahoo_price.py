
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
# import traceback
# from retrying import retry

import stock


# In[ ]:


importlib.reload(stock)


# # データパスの設定

# In[ ]:


# csv_path = '/Users/Really/Stockyard/_csv'
# price_path = '/Users/Really/Stockyard/_yahoo_csv'
csv_path = 'D:\stockyard\_csv'
price_path = 'D:\stockyard\_yahoo_csv'


# # Yahooの銘柄一覧ページから銘柄コードを取得

# In[ ]:


reading_code = []


# In[ ]:


yahoo_stock_table = stock.get_stock_table_yahoojp()


# In[ ]:


len(yahoo_stock_table)


# In[ ]:


display(yahoo_stock_table)


# In[ ]:


yahoo_stock_table.columns = ['code', 'market', 'data', 'price', 'extra']


# In[ ]:


yahoo_stock_table = yahoo_stock_table[['code', 'market', 'data', 'price']]


# In[ ]:


yahoo_stock_table.to_csv('{0}/yahoo_stock_table.csv'.format(csv_path))


# In[ ]:


pd.read_csv('{0}/yahoo_stock_table.csv'.format(csv_path), index_col=0)


# In[ ]:


reading_code[3700:]


# In[ ]:


p = 1
base = 'http://stocks.finance.yahoo.co.jp/stocks/qi/?&p={0}'
url = base.format(p)


# In[ ]:


tables = pd.read_html(url, header=0)


# In[ ]:


tables[2].iloc[0, 3]


# In[ ]:


tables[2]


# In[ ]:


while True:
    url = base.format(p)
    # 'https://stocks.finance.yahoo.co.jp/stocks/qi/?&p=1' # 2018-03-03 p=187まで
    tables = pd.read_html(url, header=0)
    if len(tables[2]) == 0:
        break
    reading_code.extend(list(tables[2]['コード']))
    p += 1


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

# reading_code = stock.get_jpx_expro_code(start_index, end_index)
reading_code = stock.get_yahoo_code(start_index, end_index)
# reading_code = stock.get_yahoo_code(start_index)
print(reading_code[-10:])
print('Next start from {0}'.format(start_index + increase_number))


# ## 新規上場銘柄読み込み用コードリスト作成

# In[ ]:


reading_code = stock.get_jpx_new_added_code()
reading_code


# ## 任意選択読み込み用コードリスト作成

# In[ ]:


read_start = 2
reading_code = keep_failed[read_start : ]#read_start + 8]
# reading_code += list(yahoo_stock_table['code'][1600 : 1700])
reading_code, len(reading_code), 


# In[ ]:


reading_code = [3540, ]
reading_code


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

info = stock.get_yahoo_info() # 保存済み info の読み込み
failed = [] # 読み込みに失敗した銘柄のコードを書き込むリストを作成
save_failed = [] # 保存のみ失敗した分

# 連続読み込み書き込み
for index in range(len(reading_code)):
    code = reading_code[index]
    
    try:
        time.sleep(1)
        
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
info = info.sort_values(by=['Code', 'Date'])
info.to_csv('{0}/yahoo_info.csv'.format(csv_path))

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


keep_info.to_csv('{0}/keep_info.csv'.format(csv_path))


# In[ ]:


pd.Series(keep_failed).to_csv('{0}/keep_failed.csv'.format(csv_path))


# # 更新

# ## 更新する銘柄コードリスト作成

# In[ ]:


start_index = 0


# In[ ]:


start_index = end_index
start_index


# In[ ]:


increase_number = 100
end_index = None
# end_index = start_index + increase_number

reading_code = stock.get_yahoo_stock_code(start_index, end_index)
print(reading_code[-10:])
print(len(reading_code))
print('Next start from {0}'.format(end_index))


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


# ## 連続更新

# __TODO 3542銘柄一括更新で3時間以上かかる。要検討 (うち、銘柄間1秒スリープ分合計1時間弱)__

# In[ ]:


end = None  # 読み込み終了日

# ロガー設定
start_time = dt.datetime.now()
logging.basicConfig(filename='get_price_{0}.log'.format(start_time.strftime('%Y-%m-%d')), filemode='w', level=logging.INFO)
logging.info('{0} add_new_price Started'.format(start_time.strftime('%Y-%m-%d %H:%M:%S')))

info = stock.get_yahoo_info() # 保存済み info の読み込み
failed = [] # 読み込みに失敗した銘柄のコードを書き込むリストを作成
save_failed = [] # 保存のみ失敗した分

# 連続読み込み書き込み
for index in range(len(reading_code)):
    code = reading_code[index]
    
    try:
        time.sleep(1)
        
        # 何か問題があるようなら最終更新日以降のデータがない場合にパスする処理を考える
        # 現状ではYahooにデータが無い場合、"No objects to concatenate" が帰って来る
        price = stock.get_yahoo_price(code)
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
info = info.sort_values(by=['Code', 'Date'])
info.to_csv('{0}/yahoo_info.csv'.format(csv_path))

logging.info('{0} add_new_price Finished'.format(dt.datetime.now().strftime('%Y-%m-%d %H:%M:%S')))


# In[ ]:


# TODO これをcsvで実現する方法を考える
for i in range(increase_number):
    print('\n[{0}]'.format(reading_code[i]))
    print(stock.get_yahoo_price(reading_code[i])[-3:])


# In[ ]:


# TODO これをcsvで実現する方法を考える
for i in range(increase_number):
    print('[{0}]'.format(reading_code[i]))
    print(sql.statement_query('select * from t_{0} order by Date desc limit 3'.format(reading_code[i])))


# In[ ]:


yahoo_stock_table = pd.read_csv('{0}/yahoo_stock_table.csv'.format(csv_path), index_col=0)
yahoo_stock_table[yahoo_stock_table['code'].isin(failed)]


# ## 変数を別名でキープ

# In[ ]:


keep_info = info
keep_info


# In[ ]:


keep_failed = failed
keep_failed


# ## キープ分の銘柄コードをcsvに保存 (履歴なし随時処理)

# In[ ]:


keep_info.to_csv('{0}/keep_info.csv'.format(csv_path))


# In[ ]:


pd.Series(keep_failed).to_csv('{0}/keep_failed.csv'.format(csv_path))


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

info = stock.get_yahoo_info() # 保存済み info の読み込み
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

info = stock.get_yahoo_info() # 保存済み info の読み込み
failed = [] # 読み込みに失敗した銘柄のコードを書き込むリストを作成

try:
    price = stock.get_yahoo_price(code)
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


# # 価格データ以外の情報を格納するinfoテーブルを作成、読み込み

# ## (最初だけ。2回目以降は作成不要。上書き注意)

# In[ ]:


info = pd.DataFrame(index=[], columns=['Code', 'StockName', 'Date', 'Open', 'High', 'Low', 'Close', 'Volume', 'AdjClose'])
info = info.astype({'Code': int}) # int型を代入してもなぜかfloat型になってしまうので、あらかじめ明示しておく
info


# ## 価格データの読み込み前に保存済みのinfoテーブルとの結合が必要な場合

# In[ ]:


info_csv = stock.get_yahoo_info() # 保存済み info の読み込み
info = info.append(info_csv).sort_values(by=['Code', 'Date']).reset_index(drop=True)
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


info.to_csv('{0}/yahoo_info.csv'.format(csv_path))

