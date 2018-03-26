
# coding: utf-8

# # Memo

# In[ ]:


# Yahoo の銘柄一覧から作成した取得銘柄リストを使用
# 2018-03-16 取得済み分についてファイル名接頭辞を t_ から y_ に変更
# 取得スクリプト内もファイル名接頭辞を変更
# 初回取得と更新取得の統合 -> スクリプトファイル作成済み


# # TODO

# In[ ]:


# 価格更新取得用2018年分ファイルと全期間ファイルの結合
# 株式分割・併合が行われた場合の調整後終値の再取得または再計算
# 取得時エラー表示: 更新するデータがないだけなのか、それ以外のエラーなのか区別できるとよいのだけど
# インデックス等、列数が異なる銘柄の取得


# # import

# In[ ]:


import numpy as np
import pandas as pd
import pandas.tseries.offsets as offsets
import datetime as dt
import time
import importlib
import logging
import pathlib
from retry import retry
# import traceback
# from retrying import retry

import stock


# In[ ]:


importlib.reload(stock)


# In[ ]:


# pandas の最大表示列数を設定 (max_rows で表示行数の設定も可能)
pd.set_option('display.max_columns', 30)


# # データパスの設定

# In[ ]:


# csv_path = '/Users/Really/Stockyard/_csv'
# price_path = '/Users/Really/Stockyard/_yahoo_csv'
csv_path = 'D:\stockyard\_csv'
price_path = 'D:\stockyard\_yahoo_csv'


# # ヒストリカルデータの初回連続読み込み

# ## TODO 価格データ読み込み済みリストの作成、次に読み込む銘柄コードの自動取得化

# In[ ]:


done = yahoo_stock_code
done


# ## 連続読み込み用内国株のコードリスト作成

# In[ ]:


start_index = 0
increase_number = 50
end_index = start_index + increase_number

# reading_code = stock.get_jpx_expro_code(start_index, end_index)
reading_code = stock.get_yahoo_code(start_index, end_index)
# reading_code = stock.get_yahoo_code(start_index)
print(reading_code[-10:])
print('Next start from {0}'.format(start_index + increase_number))


# In[ ]:


len(reading_code)


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
# reading_code = list(pd.read_csv('{0}/keep_failed.csv'.format(csv_path), header=None, index_col=0).values.flatten())
reading_code, len(reading_code)


# In[ ]:


# 保存したファイルから読み込む場合
reading_code = list(pd.read_csv('{0}/keep_failed.csv'.format(csv_path), header=None, index_col=0).values.flatten())
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

yahoo_table = pd.read_csv('{0}/yahoo_table.csv'.format(csv_path), index_col=0)
info = stock.get_yahoo_info() # 保存済み info の読み込み
failed = [] # 読み込みに失敗した銘柄のコードを書き込むリストを作成
save_failed = [] # 保存のみ失敗した分

# 連続読み込み書き込み
for index in range(len(reading_code)):
    code = reading_code[index]
    market_code = yahoo_table.loc[yahoo_table['code']==code, 'market_code'].values[0]
    
    try:
        time.sleep(1)
        
        # Yahooファイナスンスから時系列情報と銘柄名を取得
        tmp_price, stock_name = stock.get_price_yahoojp(code, market_code, start=start, end=end)
        
        # 価格以外の情報を抽出
        tmp_info = tmp_price[tmp_price.isnull().any(axis=1)].reset_index()
        if len(tmp_info) > 0:
            new_info = stock.reform_info(tmp_info, code, stock_name)
            info = info.append(new_info, ignore_index=True)
        
            price = stock.extract_price(tmp_price)
            
        # 価格情報の整形と代入
        price = stock.extract_price(tmp_price)
            
        try:
            # CSVで保存
            price.to_csv('{0}/y_{1}.csv'.format(price_path, code))
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


# In[ ]:


a = stock.get_yahoo_info()
a


# In[ ]:


a = a.drop_duplicates()
a = a.sort_values(by=['Code', 'Date']).reset_index(drop=True)
a


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

yahoo_table = pd.read_csv('{0}/yahoo_table.csv'.format(csv_path), index_col=0)
info = stock.get_yahoo_info() # 保存済み info の読み込み
failed = [] # 読み込みに失敗した銘柄のコードを書き込むリストを作成
save_failed = [] # 保存のみ失敗した分

# 連続読み込み書き込み
for index in range(len(reading_code)):
    code = reading_code[index]
    market_code = yahoo_table.loc[yahoo_table['code']==code, 'market_code'].values[0]
    
    try:
        time.sleep(1)
        
        # 何か問題があるようなら最終更新日以降のデータがない場合にパスする処理を考える
        # 現状ではYahooにデータが無い場合、"No objects to concatenate" が帰って来る
        price = stock.get_yahoo_price(code)
        start = str((price.index[-1] + offsets.Day()).date())
        
        # Yahooファイナスンスから時系列情報と銘柄名を取得
        tmp_price, stock_name = stock.get_price_yahoojp(code, market_code, start=start, end=end)
        
        # 価格以外の情報を抽出
        tmp_info = tmp_price[tmp_price.isnull().any(axis=1)].reset_index()
        if len(tmp_info) > 0:
            new_info = stock.reform_info(tmp_info, code, stock_name)
            info = info.append(new_info, ignore_index=True)
        
        # 価格情報の整形と代入
        new_price = stock.extract_price(tmp_price)

        # 取得済み分と更新分を結合
        price = price.append(new_price)
        
        try:
            # CSVで保存
            price.to_csv('{0}/y_{1}.csv'.format(price_path, code))
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
    print(sql.statement_query('select * from y_{0} order by Date desc limit 3'.format(reading_code[i])))


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

# ## 各種実験

# In[ ]:


url = 'https://info.finance.yahoo.co.jp/history/?code=38109.T&sy=1983&sm=2&sd=16&ey=2018&em=3&ed=18&tm=d&p=1'


# In[ ]:


tbls = pd.read_html(url, header=0, keep_default_na=False)


# In[ ]:


tbls[1]


# In[ ]:


tbls[1].dtypes


# In[ ]:


importlib.reload(stock)


# ## 単一銘柄の読み込み

# In[ ]:


code = 38109 # 銘柄コード

# 読み込み期間の設定
start = '1980-01-01'
# end = '2018-03-09'
end = input('end=')

# ロガー設定
start_time = dt.datetime.now()
logging.basicConfig(filename='get_price_{0}.log'.format(start_time.strftime('%Y-%m-%d')), filemode='w', level=logging.INFO)

yahoo_table = pd.read_csv('{0}/yahoo_table.csv'.format(csv_path), index_col=0)
info = stock.get_yahoo_info() # 保存済み info の読み込み
failed = [] # 読み込みに失敗した銘柄のコードを書き込むリストを作成

market_code = yahoo_table.loc[yahoo_table['code']==code, 'market_code'].values[0]

# 読み込み
try:
    # Yahooファイナスンスから時系列情報と銘柄名を取得
    tmp_price, stock_name = stock.get_price_yahoojp(code, market_code, start=start, end=end)

    # 価格以外の情報を抽出
    tmp_info = tmp_price[tmp_price.isnull().any(axis=1)].reset_index()
    if len(tmp_info) > 0:
        new_info = stock.reform_info(tmp_info, code, stock_name)
        info = info.append(new_info, ignore_index=True)
        
    # 価格情報の整形と代入
    price = stock.extract_price(tmp_price)

except Exception as e:
    logging.warning('{0} {1}: {2}'.format(dt.datetime.now().strftime('%Y-%m-%d %H:%M:%S'), code, e))
    failed.append(code)
    print('Failed in {0} at get_price'.format(code))
    print(e)

print('Failed in {0} stocks at get:'.format(len(failed)))
print(failed)


# In[ ]:


tmp_price


# In[ ]:


tmp_price[tmp_price.isnull().any(axis=1)].reset_index()


# In[ ]:


price


# In[ ]:


info


# ## 重複行があったことがあるので削除

# In[ ]:


result = tmp_price[~tmp_price.isnull().any(axis=1)]


# In[ ]:


result = result.append(result.iloc[-1:, :])


# In[ ]:


result = result[~result.index.duplicated()]


# In[ ]:


result


# ## 情報行を分割後の価格行に混入している文字列の特定と置換

# 分割情報行の空値の列は np.nan として取得されるため数値は float になる  
# 分割情報行が無ければそのまま int として取得される(調整後終値以外)  
# 価格行の欠損値には --- が入っているため数値は str になる  
# np.nan を代入できるのは float 型のみ

# In[ ]:


tbl = price.copy()


# In[ ]:


tbl


# In[ ]:


tbl.dtypes


# In[ ]:


tbl.loc['1991-10-28', 'Open']


# In[ ]:


type(tbl.loc['1991-10-28', 'Open'])


# In[ ]:


# 数値の列の数値以外の文字列 ('－' 等) を NaN に置換
for col in tbl:
    if tbl[col].dtypes == object:
        tbl.loc[tbl[col].str.isnumeric() == False, col] = np.nan


# In[ ]:


# 型変換
# np.nan は float 型のみ
tbl = tbl.astype(float)


# In[ ]:


# csvに保存するまでならば np.nan ではなく "" に変換して型は気にせずに保存しておく方が良いかも?


# In[ ]:


tbl.to_csv('{0}/tbl.csv'.format(price_path))


# In[ ]:


tbl_csv = pd.read_csv('{0}/tbl.csv'.format(price_path), index_col=0)


# In[ ]:


tbl_csv


# In[ ]:


tbl_csv.dtypes


# ## 単一銘柄の更新

# In[ ]:


code = 1301 # 銘柄コード
end = None # 読み込み終了日

# ロガー設定
start_time = dt.datetime.now()
logging.basicConfig(filename='get_price_{0}.log'.format(start_time.strftime('%Y-%m-%d')), filemode='w', level=logging.INFO)

yahoo_table = pd.read_csv('{0}/yahoo_table.csv'.format(csv_path), index_col=0)
info = stock.get_yahoo_info() # 保存済み info の読み込み
failed = [] # 読み込みに失敗した銘柄のコードを書き込むリストを作成

market_code = yahoo_table.loc[yahoo_table['code']==code, 'market_code'].values[0]

try:
    price = stock.get_yahoo_price(code)
    last_date = price.index[-1]
    start = str((price.index[-1] + offsets.Day()).date())
    
    # Yahooファイナスンスから時系列情報と銘柄名を取得
    tmp_price, stock_name = stock.get_price_yahoojp(code, market_code, start=start, end=end)

    # 価格以外の情報を抽出
    tmp_info = tmp_price[tmp_price.isnull().any(axis=1)].reset_index()
    if len(tmp_info) > 0:
        new_info = stock.reform_info(tmp_info, code, stock_name)
        info = info.append(new_info, ignore_index=True)

    # 価格情報の整形と代入
    new_price = stock.extract_price(tmp_price)
    
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


# # ファイル名の接頭辞 t\_ を y\_ に変更

# ## pathlib について

# In[ ]:


# root = pathlib.Path(__file__) # 実行スクリプトのファイル名のPathオブジェクト
root = pathlib.Path()
root


# In[ ]:


# 絶対パスの取得、シンボリックリンクも解決
abs_root = root.resolve()
abs_root


# In[ ]:


# 親ディレクトリ
abs_root.parent


# In[ ]:


# 親ディレクトリに指定したパスが存在するかどうか
abs_root.parent.joinpath('tax_2017').exists()


# In[ ]:


# 文字列からPathオブジェクトを作成
csv_dir = pathlib.Path('D:\stockyard\_csv')
csv_dir


# ## パスの設定

# In[ ]:


# csv_path = '/Users/Really/Stockyard/_csv'
# price_path = '/Users/Really/Stockyard/_yahoo_csv'
csv_path = 'D:\stockyard\_csv'
price_path = 'D:\stockyard\_yahoo_csv'


# In[ ]:


csv_dir = pathlib.Path(csv_path)
csv_dir


# In[ ]:


price_dir = pathlib.Path(price_path)
price_dir


# ## 置換

# In[ ]:


for price_file in price_dir.iterdir():
    # pathlibのパスは文字列ではない。かつpathlibにも.replaceメソッドがあるので注意。
    replaced = str(price_file).replace('t_', 'y_')
    price_file.rename(replaced)


# # GCE環境調整用

# ## GCE環境での新規読み込み用に新規infoファイルの作成

# In[ ]:


info = stock.get_yahoo_info()


# In[ ]:


a = pd.DataFrame(index=[], columns=['Code', 'StockName', 'Date', 'Open', 'High', 'Low', 'Close', 'Volume', 'AdjClose'])
a


# In[ ]:


a = a.append(info.iloc[1,1:], ignore_index = True)
a


# In[ ]:


# 出力後、ファイル名は適宜変更
a.to_csv('{0}/yahoo_info_gce.csv'.format(csv_path))


# ## infoファイルに不要なインデックス列が追加されてしまったので整形

# In[ ]:


info = pd.read_csv("D:\downloads\yahoo_info(1).csv", index_col=0)


# In[ ]:


info['Date'] = pd.to_datetime(info['Date'])


# In[ ]:


info


# In[ ]:


info = info[['Code', 'StockName', 'Date', 'Open', 'High', 'Low', 'Close', 'Volume', 'AdjClose']]


# In[ ]:


info = info.drop_duplicates()


# In[ ]:


info = info.sort_values(by=['Code', 'Date']).reset_index(drop=True)


# In[ ]:


info.to_csv('D:\downloads\yahoo_info.csv')


# In[ ]:


info_csv = pd.read_csv("D:\downloads\yahoo_info.csv", index_col=0)
info_csv


# ## 定期更新用に2018年分のみの価格ファイルを作成

# In[ ]:


price_dir = pathlib.Path(price_path)
price_dir


# In[ ]:


new_dir = pathlib.Path('D:\stockyard\_yahoo_csv_2018')
new_dir


# In[ ]:


for price_file in price_dir.iterdir():
    # print(new_dir / price_file.name)
    price = pd.read_csv(price_file, index_col=0)
    price.index = pd.to_datetime(price.index)
    price_2018 = price['2018-01-01':]
    price_2018.to_csv(new_dir / price_file.name)


# In[ ]:


pd.read_csv(new_dir / price_file.name, index_col=0)


# # 新規取得と更新の統合スクリプト

# In[ ]:


get_ipython().run_cell_magic('writefile', 'get_yahoo_price_script.py', "\n\n# coding: utf-8\n\nimport numpy as np\nimport pandas as pd\nimport pandas.tseries.offsets as offsets\nimport datetime as dt\nimport time\nimport importlib\nimport logging\nfrom retry import retry\n\nimport stock\n\n\n# パスの設定\n# csv_path = '/Users/Really/Stockyard/_csv'\n# price_path = '/Users/Really/Stockyard/_yahoo_csv'\n# csv_path = 'D:\\stockyard\\_csv'\n# price_path = 'D:\\stockyard\\_yahoo_csv'\ncsv_path = '/home/hideshi_honma/stockyard/_csv'\nprice_path = '/home/hideshi_honma/stockyard/_yahoo_csv'\n\n\n# 読み込むコードのリストを作成\nstart_index = input('start_index=')\nif start_index == 'failed':\n    reading_code = list(\n        pd.read_csv('{0}/failed.csv'.format(csv_path), header=None, index_col=0).values.flatten())\nelif start_index == 'save_failed':\n    reading_code = list(\n        pd.read_csv('{0}/save_failed.csv'.format(csv_path), header=None, index_col=0).values.flatten())\nelse:\n    start_index = int(start_index)\n    increase_number = int(input('increase_number='))\n    end_index = start_index + increase_number\n    reading_code = stock.get_yahoo_code(start_index, end_index)\n    print('tail 10: {0}'.format(reading_code[-10:]))\n    print('Next start from {0}'.format(start_index + increase_number))\n\n\n# 読み込み期間の設定\nstart = input('start_date (none -> 1980-01-01 or update) =')\nend = input('end_date (none -> today) =')\n\n# ロガー設定\nstart_time = dt.datetime.now()\nlogging.basicConfig(filename='get_price_{0}.log'.format(start_time.strftime('%Y-%m-%d')), filemode='w', level=logging.INFO)\nlogging.info('{0} get_price Started'.format(start_time.strftime('%Y-%m-%d %H:%M:%S')))\n\n# 既存ファイルの読み込み\nyahoo_table = pd.read_csv('{0}/yahoo_table.csv'.format(csv_path), index_col=0)\ninfo = stock.get_yahoo_info() # 保存済み info の読み込み\n\n# 変数の設定\nfailed = [] # 読み込みに失敗した銘柄のコードを書き込むリストを作成\nsave_failed = [] # 保存のみ失敗した分\n\n\n# 連続取得保存ループ\nfor index in range(len(reading_code)):\n    code = reading_code[index]\n    market_code = yahoo_table.loc[yahoo_table['code'] == code, 'market_code'].values[0]\n\n    # 既存価格ファイルの読み込み\n    try:\n        price = stock.get_yahoo_price(code)\n        start = str((price.index[-1] + offsets.Day()).date())\n    except Exception as e:\n        logging.warning('{0} {1}: {2}'.format(dt.datetime.now().strftime('%Y-%m-%d %H:%M:%S'), code, e))\n        print('{0}: File does not exist in {1}'.format(index, code))\n        price = pd.DataFrame()\n    \n    # 取得、整形、保存\n    try:\n        time.sleep(1)\n        \n        # Yahooファイナスンスから時系列情報と銘柄名を取得\n        tmp_price, stock_name = stock.get_price_yahoojp(code, market_code, start=start, end=end)\n\n        # 価格以外の情報を抽出\n        tmp_info = tmp_price[tmp_price.isnull().any(axis=1)].reset_index()\n        if len(tmp_info) > 0:\n            new_info = stock.reform_info(tmp_info, code, stock_name)\n            info = info.append(new_info, ignore_index=True)\n\n        # 価格情報の整形と代入\n        new_price = stock.extract_price(tmp_price)\n\n        # 既存分と更新分を結合\n        price = price.append(new_price)\n            \n        # 保存\n        try:\n            price.to_csv('{0}/y_{1}.csv'.format(price_path, code))\n            info.to_csv('{0}/yahoo_info.csv'.format(csv_path))\n          \n            print('{0}: Success {1}'.format(index, code))\n            \n        # 保存に失敗した場合の処理\n        except Exception as e:\n            logging.warning('{0} {1}: {2}'.format(dt.datetime.now().strftime('%Y-%m-%d %H:%M:%S'), code, e))\n            save_failed.append(code)\n            print('{0}: Failed in {1} at Save Data'.format(index, code))\n            print(e)\n            \n    # 読み込みまたは整形に失敗した場合の処理\n    except Exception as e:\n        logging.warning('{0} {1}: {2}'.format(dt.datetime.now().strftime('%Y-%m-%d %H:%M:%S'), code, e))\n        failed.append(code)\n        print('{0}: Failed in {1} at get_price'.format(index, code))\n        print(e)\n# ループ終了\n\n\n# 最後にinfoの重複と順序を整理してから再度保存\ninfo = info.drop_duplicates()\ninfo = info.sort_values(by=['Code', 'Date']).reset_index(drop=True)\ninfo.to_csv('{0}/yahoo_info.csv'.format(csv_path))\n\n# 失敗情報の表示\nprint('Failed in {0} stocks at get:'.format(len(failed)))\nprint(failed)\nprint('Failed in {0} stocks at save:'.format(len(save_failed)))\nprint(save_failed)\n\n# 失敗分の銘柄コードをcsvに保存 (履歴なし随時処理)\npd.Series(failed).to_csv('{0}/failed.csv'.format(csv_path))\npd.Series(save_failed).to_csv('{0}/save_failed.csv'.format(csv_path))\n\n# 終了ログ\nlogging.info('{0} get_price Finished'.format(dt.datetime.now().strftime('%Y-%m-%d %H:%M:%S')))")

