
# coding: utf-8

# # Memo
2939 8287 マックスバリュ西日本 までyahooから銘柄名を取得
2940 から 2959 までは東証のエクセル由来のテーブルから取得に変更
2960 8345 岩手銀行 からyahooからの取得に戻した

3318 メガネスーパー 10/26 上場廃止
Pro Market銘柄はYahooにはないらしい
# # ライブラリファイル作成

# In[15]:

get_ipython().run_cell_magic('writefile', 'stock.py', '\n# coding: utf-8\nfrom __future__ import unicode_literals\nimport numpy as np\nimport pandas as pd\nimport pandas.tseries.offsets as offsets\nimport datetime as dt\nimport time\nimport importlib\nimport logging\nfrom retry import retry\n# import traceback\n# from retrying import retry\nfrom sqlalchemy import create_engine\nfrom sqlalchemy.types import Date, Integer, Text\n# from sqlalchemy.types import Integer\n# from sqlalchemy.types import Text\n\n\nclass sql:\n    db_settings = {\n        "host": \'localhost\',\n        "database": \'StockPrice_Yahoo_1\',\n        "user": \'user\',\n        "password": \'password\',\n        "port":\'3306\'\n    }\n    engine = create_engine(\'mysql://{user}:{password}@{host}:{port}/{database}\'.format(**db_settings))\n\n    \n    def write_quote(self, code, quote):\n        table_name = \'t_{0}\'.format(code)\n        # sqlalchemy.typesで定義されたデータ型を辞書形式で設定\n        dtype = {\n            \'Date\': Date(),\n            \'Open\': Integer(),\n            \'High\': Integer(),\n            \'Low\': Integer(),\n            \'Close\': Integer(),\n            \'Volume\': Integer(),\n            \'AdjClose\': Integer()\n        }\n        quote.to_sql(table_name, sql.engine, if_exists=\'replace\', dtype=dtype)\n        # 主キーを設定\n        # 参考 https://stackoverflow.com/questions/30867390/python-pandas-to-sql-how-to-create-a-table-with-a-primary-key\n        with sql.engine.connect() as con:\n            con.execute(\'ALTER TABLE `{0}` ADD PRIMARY KEY (`Date`);\'.format(table_name))\n        \n\n    def get_quote(self, code):\n        table_name = \'t_{0}\'.format(code)\n        result = pd.read_sql_table(table_name, sql.engine, index_col=\'Date\')#.drop(\'index\', axis=1)\n        \n        return result\n    \n    \n    def write_info(self, table_name, info):\n        # sqlalchemy.typesで定義されたデータ型を辞書形式で設定\n        dtype = {\n            \'Code\': Integer(),\n            \'StockName\': Text(),\n            \'Date\': Date(),\n            \'Open\': Text()}\n        \n        info.to_sql(table_name, sql.engine, if_exists=\'replace\', dtype=dtype)\n        \n        \n    def get_info(self):\n        result = pd.read_sql_table(\'info\', sql.engine, index_col=None).drop(\'index\', axis=1)\n        result[\'Date\'] = pd.to_datetime(result[\'Date\'])\n        \n        return result\n        \n\n    def get_domestic_stock_code(self, start_index=0, end_index=None):\n        domestic_stock_table = pd.read_sql_table(\'domestic_stock_table\', sql.engine, index_col=None).drop(\'index\', axis=1)\n        \n        if end_index == None:\n            end_index = len(domestic_stock_table)\n\n        result = list(domestic_stock_table[\'code\'][start_index : end_index])\n        \n        return result\n\n    \n\n    def statement_query(self, statement):\n        result = pd.read_sql_query(statement, sql.engine, index_col=None)\n        # ex. df = sql.statement_query(\'SELECT code, name FROM domestic_stock_table\')\n        # テーブル全体ではなく抽出の場合、インデックスは無いらしく下記ではエラーになる\n        #result = pd.read_sql_query(statement, sql.engine, index_col=None).drop(\'index\', axis=1)\n        \n        return result\n    \n    \n    def write_table(self, table_name, table):\n        table.to_sql(table_name, sql.engine, if_exists=\'replace\')\n        \n    \n    def read_table(self, table_name):\n        result = pd.read_sql_table(table_name, sql.engine, index_col=None).drop(\'index\', axis=1)\n        \n        return result\n    \n    \n# 関数にretryデコレーターを付ける\n@retry(tries=5, delay=1, backoff=2)\ndef get_table(url):\n    result = pd.read_html(url, header=0) # header引数で0行目をヘッダーに指定。データフレーム型\n    \n    return result\n\n\ndef get_quote_yahoojp(code, start=None, end=None, interval=\'d\'): # start = \'2017-01-01\'\n    # http://sinhrks.hatenablog.com/entry/2015/02/04/002258\n    # http://jbclub.xii.jp/?p=598\n    base = \'http://info.finance.yahoo.co.jp/history/?code={0}.T&{1}&{2}&tm={3}&p={4}\'\n    \n    start = pd.to_datetime(start) # Timestamp(\'2017-01-01 00:00:00\')\n\n    if end == None:\n        end = pd.to_datetime(pd.datetime.now())\n    else :\n        end = pd.to_datetime(end)\n    start = \'sy={0}&sm={1}&sd={2}\'.format(start.year, start.month, start.day) # \'sy=2017&sm=1&sd=1\'\n    end = \'ey={0}&em={1}&ed={2}\'.format(end.year, end.month, end.day)\n    p = 1\n    tmp_result = []\n\n    if interval not in [\'d\', \'w\', \'m\', \'v\']:\n        raise ValueError("Invalid interval: valid values are \'d\', \'w\', \'m\' and \'v\'")\n\n    while True:\n        url = base.format(code, start, end, interval, p)\n        # print(url)\n        # https://info.finance.yahoo.co.jp/history/?code=7203.T&sy=2000&sm=1&sd=1&ey=2017&em=10&ed=13&tm=d&p=1\n        tables = get_table(url)\n        if len(tables) < 2 or len(tables[1]) == 0:\n            # print(\'break\')\n            break\n        tmp_result.append(tables[1]) # ページ内の3つのテーブルのうち2番目のテーブルを連結\n        p += 1\n        # print(p)\n        \n    result = pd.concat(tmp_result, ignore_index=True) # インデックスをゼロから振り直す\n\n    result.columns = [\'Date\', \'Open\', \'High\', \'Low\', \'Close\', \'Volume\', \'AdjClose\'] # 列名を変更\n    if interval == \'m\':\n        result[\'Date\'] = pd.to_datetime(result[\'Date\'], format=\'%Y年%m月\')\n    else:\n        result[\'Date\'] = pd.to_datetime(result[\'Date\'], format=\'%Y年%m月%d日\') # 日付の表記を変更\n    result = result.set_index(\'Date\') # インデックスを日付に変更\n    result = result.sort_index()\n    \n    stock_name = tables[0].columns[0]\n    # print([code, stock_name])\n    \n    return [result, stock_name]\n\n\ndef extract_quote(tmp_quote):\n    # null が存在する行を取り除いて価格データとする 参考 https://qiita.com/u1and0/items/fd2780813b690a40c197\n    result = tmp_quote[~tmp_quote.isnull().any(axis=1)].astype(float).astype(int) # この場合、"~"は "== False" とするのと同じこと\n    # なぜか日付が重複した行が入る場合があるので確認、削除\n    if(result.index.duplicated().any()):\n        result = result[~result.index.duplicated()]\n        \n    return result\n\n\ndef reform_info(tmp_info, code, stock_name):\n    # 単列の場合、代入と同時に列を生成できるが、複数列の場合は存在しないとエラーになるので先に列を追加しなければいけない\n    result = tmp_info.ix[:, [\'Code\', \'StockName\', \'Date\', \'Open\', \'High\', \'Low\', \'Close\', \'Volume\', \'AdjClose\']] # 列を追加、並べ替え\n    result[[\'Code\', \'StockName\']] = [code, stock_name] # 複数列に値を代入する場合は列名をリスト形式で記述\n    result[\'Code\'] = result[\'Code\'].astype(int) # float型になってしまうので変換\n            \n    return result')


# # import

# In[1]:

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


# In[16]:

importlib.reload(stock)


# # MySQLに接続

# In[26]:

sql = stock.sql()


# In[260]:

help(sql)


# # ヒストリカルデータの初回連続読み込み

# ### TODO 価格データ読み込み済みリストの作成、次に読み込む銘柄コードの自動取得化

# In[192]:

done = domestic_stock_code
done


# ### 連続読み込み用内国株のコードリスト作成

# In[91]:

start_index = 3100
increase_number = 100
end_index = start_index + increase_number

reading_code = sql.get_domestic_stock_code(start_index, end_index)
print(reading_code[-10:])
print('Next start from {0}'.format(start_index + increase_number))


# In[4]:

est = sql.read_table('ex_stock_table')


# In[22]:

reading_code = list(est.code[14:])
reading_code


# ### 任意選択読み込み用 コードリスト作成

# In[89]:

read_start = 71
reading_code = keep_failed[read_start : ]#read_start + 8]
reading_code += list(domestic_stock_table['code'][1600 : 1700])
reading_code, len(reading_code), 


# ### 失敗分読み込み用コードリスト作成

# In[24]:

reading_code = failed
reading_code, len(reading_code)


# ### 連続読み込み書き込み

# In[27]:

# 読み込み期間の設定
start = '2000-01-01'
end = None

# ロガー設定
start_time = dt.datetime.now()
logging.basicConfig(filename='get_quote_{0}.log'.format(start_time.strftime('%Y-%m-%d')), filemode='w', level=logging.INFO)
logging.info('{0} get_quote Started'.format(start_time.strftime('%Y-%m-%d %H:%M:%S')))

sql = stock.sql() # MySQLに接続するクラスインスタンスを作成

info = sql.get_info() # 保存済み info の読み込み
failed = [] # 読み込みに失敗した銘柄のコードを書き込むリストを作成
save_failed = [] # 保存のみ失敗した分

# 連続読み込み書き込み
for index in range(len(reading_code)):
    code = reading_code[index]
    
    try:
        time.sleep(5)
        
        # Yahooファイナスンスから時系列情報と銘柄名を取得
        tmp_quote, stock_name = stock.get_quote_yahoojp(code, start=start, end=end)
        
        # 価格と価格以外の情報を分離
        tmp_info = tmp_quote[tmp_quote.isnull().any(axis=1)].reset_index()
        if len(tmp_info) > 0:
            new_info = stock.reform_info(tmp_info, code, stock_name)
            info = info.append(new_info, ignore_index=True)
        
            quote = stock.extract_quote(tmp_quote)
            
        else:
            quote = tmp_quote # 価格以外の情報がなければそのまま
            
        try:
            # CSVで保存
            quote.to_csv('/Users/Really/Stockyard/_csv/t_{0}.csv'.format(code))
            info.to_csv('/Users/Really/Stockyard/_csv/info.csv')
            # MySQLに保存
            sql.write_quote(code, quote)
            sql.write_info('info', info)
          
            print('{0}: Success {1}'.format(index, code))
            
        except Exception as e:
            logging.warning('{0} {1}: {2}'.format(dt.datetime.now().strftime('%Y-%m-%d %H:%M:%S'), code, e))
            save_failed.append(code)
            print('{0}: Failed in {1} at Save Data'.format(index, code))
            print(e)
            
    except Exception as e:
        logging.warning('{0} {1}: {2}'.format(dt.datetime.now().strftime('%Y-%m-%d %H:%M:%S'), code, e))
        failed.append(code)
        print('{0}: Failed in {1} at get_quote'.format(index, code))
        print(e)

print('Failed in {0} stocks at get:'.format(len(failed)))
print(failed)
print('Failed in {0} stocks at save:'.format(len(save_failed)))
print(save_failed)

# 最後にinfoの重複と順序を整理してから再度保存
info = info.drop_duplicates()
info = info.sort_values(by=['Code', 'Date'])
info.to_csv('/Users/Really/Stockyard/_csv/info.csv')
sql.write_info('info', info)

logging.info('{0} get_quote Finished'.format(dt.datetime.now().strftime('%Y-%m-%d %H:%M:%S')))


# ### 変数を別名でキープ

# In[369]:

keep_info = info
keep_info


# In[28]:

keep_failed = failed
keep_failed


# ### キープ分の銘柄コードをcsvに保存 (履歴なし随時処理)

# In[195]:

keep_info.to_csv('/Users/Really/Stockyard/_csv/keep_info.csv')
sql = stock.sql()
sql.write_info('keep_info', keep_info)


# In[29]:

pd.Series(keep_failed).to_csv('/Users/Really/Stockyard/_csv/keep_failed.csv')


# # 更新

# ### 更新する銘柄コードリスト作成

# In[4]:

start_index = 0


# In[22]:

start_index = end_index
end_index


# In[7]:

increase_number = 3542
end_index = start_index + increase_number

# reading_code = sql.get_domestic_stock_code()
reading_code = sql.get_domestic_stock_code(start_index, end_index)
print(reading_code[-10:])
print(len(reading_code))
print('Next start from {0}'.format(end_index))


# ### 連続更新

# ### TODO 3542銘柄一括更新で3時間以上かかる。要検討 (うち、銘柄間1秒スリープ分合計1時間弱)

# In[8]:

end = None  # 読み込み終了日

# ロガー設定
start_time = dt.datetime.now()
logging.basicConfig(filename='get_quote_{0}.log'.format(start_time.strftime('%Y-%m-%d')), filemode='w', level=logging.INFO)
logging.info('{0} add_new_quote Started'.format(start_time.strftime('%Y-%m-%d %H:%M:%S')))

sql = stock.sql() # MySQLに接続するクラスインスタンスを作成

info = sql.get_info() # 保存済み info の読み込み
failed = [] # 読み込みに失敗した銘柄のコードを書き込むリストを作成
save_failed = [] # 保存のみ失敗した分

# 連続読み込み書き込み
for index in range(len(reading_code)):
    code = reading_code[index]
    
    try:
        time.sleep(1)
        
        # 何か問題があるようなら最終更新日以降のデータがない場合にパスする処理を考える
        # 現状ではYahooにデータが無い場合、"No objects to concatenate" が帰って来る
        quote = sql.get_quote(code)
        last_date = quote.index[-1]
        start = str((quote.index[-1] + offsets.Day()).date())
        
        # Yahooファイナスンスから時系列情報と銘柄名を取得
        tmp_quote, stock_name = stock.get_quote_yahoojp(code, start=start, end=end)
        
        # 価格と価格以外の情報を分離
        tmp_info = tmp_quote[tmp_quote.isnull().any(axis=1)].reset_index()
        if len(tmp_info) > 0:
            new_info = stock.reform_info(tmp_info, code, stock_name)
            info = info.append(new_info, ignore_index=True)
        
            new_quote = stock.extract_quote(tmp_quote)
            
        else:
            new_quote = tmp_quote # 価格以外の情報がなければそのまま
            
        quote = quote.append(new_quote)
        
        try:
            # CSVで保存
            quote.to_csv('/Users/Really/Stockyard/_csv/t_{0}.csv'.format(code))
            info.to_csv('/Users/Really/Stockyard/_csv/info.csv')
            # MySQLに保存
            sql.write_quote(code, quote)
            sql.write_info('info', info)
          
            print('{0}: Success {1}'.format(index, code))
            
        except Exception as e:
            logging.warning('{0} {1}: {2}'.format(dt.datetime.now().strftime('%Y-%m-%d %H:%M:%S'), code, e))
            save_failed.append(code)
            print('{0}: Failed in {1} at Save Data'.format(index, code))
            print(e)
            
    except Exception as e:
        logging.warning('{0} {1}: {2}'.format(dt.datetime.now().strftime('%Y-%m-%d %H:%M:%S'), code, e))
        failed.append(code)
        print('{0}: Failed in {1} at get_quote'.format(index, code))        
        print(e)

print('Failed in {0} stocks at get:'.format(len(failed)))
print(failed)
print('Failed in {0} stocks at save:'.format(len(save_failed)))
print(save_failed)

# 最後にinfoの重複と順序を整理してから再度保存
info = info.drop_duplicates()
info = info.sort_values(by=['Code', 'Date'])
info.to_csv('/Users/Really/Stockyard/_csv/info.csv')
sql.write_info('info', info)

logging.info('{0} add_new_quote Finished'.format(dt.datetime.now().strftime('%Y-%m-%d %H:%M:%S')))


# In[18]:

for i in range(increase_number):
    print('[{0}]'.format(reading_code[i]))
    print(sql.statement_query('select * from t_{0} order by Date desc limit 3'.format(reading_code[i])))


# # 単一銘柄、保存なし版

# ### 単一銘柄の読み込み

# In[ ]:

code = 1000 # 銘柄コード

# 読み込み期間の設定
start = '2017-10-01'
end = None

# ロガー設定
start_time = dt.datetime.now()
logging.basicConfig(filename='get_quote_{0}.log'.format(start_time.strftime('%Y-%m-%d')), filemode='w', level=logging.INFO)

sql = stock.sql() # MySQLに接続するクラスインスタンスを作成

info = sql.get_info() # 保存済み info の読み込み
failed = [] # 読み込みに失敗した銘柄のコードを書き込むリストを作成

# 読み込み
try:
    # Yahooファイナスンスから時系列情報と銘柄名を取得
    tmp_quote, stock_name = stock.get_quote_yahoojp(code, start=start, end=end)

    # 価格と価格以外の情報を分離
    tmp_info = tmp_quote[tmp_quote.isnull().any(axis=1)].reset_index()
    if len(tmp_info) > 0:
        new_info = stock.reform_info(tmp_info, code, stock_name)
        info = info.append(new_info, ignore_index=True)

        quote = stock.extract_quote(tmp_quote)

    else:
        quote = tmp_quote # 価格以外の情報がなければそのまま

except Exception as e:
    logging.warning('{0} {1}: {2}'.format(dt.datetime.now().strftime('%Y-%m-%d %H:%M:%S'), code, e))
    failed.append(code)
    print('Failed in {0} at get_quote'.format(code))
    print(e)

print('Failed in {0} stocks at get:'.format(len(failed)))
print(failed)


# In[ ]:

quote


# In[491]:

info


# ### 単一銘柄の更新

# In[ ]:

code = 1301 # 銘柄コード
end = None # 読み込み終了日

# ロガー設定
start_time = dt.datetime.now()
logging.basicConfig(filename='get_quote_{0}.log'.format(start_time.strftime('%Y-%m-%d')), filemode='w', level=logging.INFO)

sql = stock.sql() # MySQLに接続するクラスインスタンスを作成

info = sql.get_info() # 保存済み info の読み込み
failed = [] # 読み込みに失敗した銘柄のコードを書き込むリストを作成

try:
    quote = sql.get_quote(code)
    last_date = quote.index[-1]
    start = str((quote.index[-1] + offsets.Day()).date())
    
    # Yahooファイナスンスから時系列情報と銘柄名を取得
    tmp_quote, stock_name = stock.get_quote_yahoojp(code, start=start, end=end)

    # 価格と価格以外の情報を分離
    tmp_info = tmp_quote[tmp_quote.isnull().any(axis=1)].reset_index()
    if len(tmp_info) > 0:
        new_info = stock.reform_info(tmp_info, code, stock_name)
        info = info.append(new_info, ignore_index=True)

        new_quote = stock.extract_quote(tmp_quote)

    else:
        new_quote = tmp_quote # 価格以外の情報がなければそのまま
        
    quote = quote.append(new_quote)

except Exception as e:
    logging.warning('{0} {1}: {2}'.format(dt.datetime.now().strftime('%Y-%m-%d %H:%M:%S'), code, e))
    failed.append(code)
    print('Failed in {0} at get_quote'.format(code))
    print(e)

print('Failed in {0} stocks at get:'.format(len(failed)))
print(failed)


# # CSVからSQLへ(SQL書き込みに失敗した分用)

# In[225]:

tmp_failed = save_failed


# In[238]:

sql = stock.sql()

for tmp_index in range(len(tmp_failed)):
    tmp_code = tmp_failed[tmp_index]
    tmp_quote = pd.read_csv('/Users/Really/Stockyard/_csv/t_{0}.csv'.format(tmp_code), index_col='Date')
    tmp_quote = tmp_quote[~tmp_quote.index.duplicated()] # 重複している値が True となっているため ~ で論理否定をとって行選択する
    sql.write_quote(tmp_code, tmp_quote)
    print(tmp_code)


# # 読み込み済み価格データのCSVが保存されているかどうかの確認

# In[105]:

# original table
domestic_stock_table = pd.read_sql_table('domestic_stock_table', engine, index_col=None).drop('index', axis=1)

table_index = list(domestic_stock_table['code'][ : 2200])
len(table_index), end_index, table_index[-10:]


# In[31]:

# _csvフォルダ内の価格データ一覧をリスト化

import os

csv_table = os.listdir('/Users/Really/Stockyard/_csv')[4:] # TODO スキップするファイル数を指定するのではなく正規表現で書き直す
print(csv_table)


# In[37]:

type(list(map(int, csv_table[0]))), type(csv_table) #, type(table_index[0])


# In[33]:

# 銘柄コードのみ抽出
# TODO 4桁以上のコードもあるので正規表現で書き直す
for t in range(len(csv_table)):
    csv_table[t] = csv_table[t][2:6]


# In[36]:

# オブジェクト、データの型の変換例
list(map(int, csv_table))[:5]


# In[149]:

len(csv_table), len(table_index)


# In[176]:

# 内容が同一かを確認するためデータフレーム化
#TODO もっといいやり方があるはず
table_df = pd.DataFrame([table_index, csv_table])
table_df


# In[188]:

table_df.ix[0].astype(str) == table_df.ix[1]


# # 上場一覧から内国株のテーブルを作成

# In[11]:

# 東証のエクセルファイルを読み込む
all_stock_table = pd.read_excel('data_j.xls') # http://www.jpx.co.jp/markets/statistics-equities/misc/01.html
all_stock_table.columns = ['date', 'code', 'name', 'market', 'code_33', 'category_33', 'code_17', 'category_17', 'code_scale', 'scale'] # 列名を変更


# In[ ]:

# 上場一覧のテーブル保存
all_stock_table.to_csv('/Users/Really/Stockyard/_csv/all_stock_table.csv')
sql.write_table('all_stock_table', all_stock_table)
# all_stock_table.to_sql('all_stock_table', engine, if_exists='replace')


# In[5]:

# 内国株のテーブル作成
domestic_stock_table = all_stock_table.ix[all_stock_table['market'].str.contains('内国株')].reset_index(drop=True)


# In[222]:

# 内国株のテーブル保存
domestic_stock_table.to_csv('/Users/Really/Stockyard/_csv/domestic_stock_table.csv')
sql.write_table('domestic_stock_table', domestic_stock_table)
# domestic_stock_table.to_sql('domestic_stock_table', engine, if_exists='replace')


# In[6]:

# 内国株以外のテーブル作成
ex_stock_table = all_stock_table[~all_stock_table['code'].isin(domestic_stock_table['code'])].reset_index(drop=True)


# In[12]:

# 内国株以外のテーブル保存
ex_stock_table.to_csv('/Users/Really/Stockyard/_csv/ex_stock_table.csv')
sql.write_table('ex_stock_table', ex_stock_table)
# ex_stock_table.to_sql('ex_stock_table', engine, if_exists='replace')


# In[12]:

all_stock_table


# In[11]:

domestic_stock_table


# In[6]:

ex_stock_table


# In[9]:

pd.DataFrame([sql.read_table('all_stock_table').dtypes, sql.read_table('domestic_stock_table').dtypes, sql.read_table('ex_stock_table').dtypes],
            index=['all', 'domestic', 'ex'])


# # 価格データ以外の情報を格納するinfoテーブルを作成、読み込み
# ### (最初だけ。2回目以降は作成不要。上書き注意)

# In[10]:

info = pd.DataFrame(index=[], columns=['Code', 'StockName', 'Date', 'Open', 'High', 'Low', 'Close', 'Volume', 'AdjClose'])
info = info.astype({'Code': int}) # int型を代入してもなぜかfloat型になってしまうので、あらかじめ明示しておく
info


# ### MySQLに保存済みのinfoテーブルの読み込み (2回目以降、現行使用版)

# In[88]:

info = sql.get_info()
info


# ### 価格データの読み込み前にMySQLに保存済みのinfoテーブルとの結合が必要な場合

# In[ ]:

info_sql = sql.get_info()
info = info.append(info_sql).sort_values(by=['Code', 'Date']).reset_index(drop=True)
info['Date'] = pd.to_datetime(info['Date'])


# ### infoの内容を確認、修正

# In[70]:

info


# In[23]:

info = info.drop_duplicates()


# In[24]:

info = info.sort_values(by=['Code', 'Date'])


# In[40]:

info.duplicated().any()


# In[27]:

info[info.duplicated()]


# In[29]:

info.to_csv('/Users/Really/Stockyard/_csv/info.csv')


# In[28]:

sql.write_info('info', info)


# # 日時設定の試行

# In[18]:

from dateutil.parser import parse


# In[54]:

last_date


# In[55]:

t = parse("2017-10-20")
t


# In[56]:

t == last_date


# In[57]:

today = dt.datetime.today()
today


# In[58]:

parse("2017-11-02").date() == today.date()


# In[63]:

dt.datetime(2016, 7, 5, 13, 45, 27)


# In[85]:

st = dt.datetime.now()
st


# In[87]:

st.strftime('%Y-%m-%d_%H-%M-%S')


# In[86]:

st.date().strftime('%Y-%m-%d_%H-%M-%S')


# In[88]:

st.strftime('%Y-%m-%d')


# In[80]:

dt.datetime.now().date()


# # MySQLのみ保存済み、csvに書き出していない価格データを処理 (処理済み)

# In[216]:

sql_to_csv_list = domestic_stock_table.ix[:99, 1]

for i in range(len(sql_to_csv_list)):
    quote_sql = pd.read_sql_table('t_{0}'.format(sql_to_csv_list[i]), engine, index_col='Date')
    quote_sql.to_csv('/Users/Really/Stockyard/_csv/t_{0}.csv'.format(sql_to_csv_list[i]))


# #  クラス不使用版コード

# ### MySQLに接続

# In[19]:

db_settings = {
    "host": 'localhost',
    "database": 'StockPrice_Yahoo_1',
    "user": 'user',
    "password": 'password',
    "port":'3306'
}
engine = create_engine('mysql://{user}:{password}@{host}:{port}/{database}'.format(**db_settings))


# ### MySQLに保存済みのinfoテーブルの読み込み

# In[ ]:

# クラスを使わない場合
# テーブル全体を選択しているので read_sql_table を使用するのと同じこと
statement = "SELECT * FROM info"
info = pd.read_sql_query(statement, engine, index_col=None).drop('index', axis=1)
info['Date'] = pd.to_datetime(info['Date'])
info


# ### 読み込む内国株のコードリスト作成 (クラス不使用版)

# In[71]:

# 内国株だけにする
# MySQLに保存済みの内国株テーブルから作成。今後はこちらを使用する
domestic_stock_table = pd.read_sql_table('domestic_stock_table', engine, index_col=None).drop('index', axis=1)

start_index = 2810
increase_number = 10
end_index = start_index + increase_number

reading_code = list(domestic_stock_table['code'][start_index : end_index])
print(reading_code[-10:])
print('Next start from {0}'.format(start_index + increase_number))


# # いらなくなったコードの保管場所

# In[ ]:

# 列単位で個別に名称を変更する場合
all_stock_table = all_stock_table.rename(columns={'市場・商品区分': 'market'})

# marketの値を指定して選択
len(all_stock_table.query("market == '市場第一部（内国株）' | market == '市場第二部（内国株）'"))


# # チャートのプロット＊未完成
https://qiita.com/toyolab/items/1b5d11b5d376bd542022
# In[112]:

import matplotlib.pyplot as plt
get_ipython().magic('matplotlib inline')


# In[ ]:

import pandas.tools.plotting as plotting

class OhlcPlot(plotting.LinePlot):
    ohlc_cols = pd.Index(['open', 'high', 'low', 'close'])
    reader_cols = pd.Index(['Open', 'High', 'Low', 'Close', 'Volume', 'Adj Close'])

    def __init__(self, data, **kwargs):
        data = data.copy()
        self.freq = kwargs.pop('freq', 'B')

        if isinstance(data, pd.Series):
            data = data.resample(self.freq, how='ohlc')
        assert isinstance(data, pd.DataFrame)
        assert isinstance(data.index, pd.DatetimeIndex)
        if data.columns.equals(self.ohlc_cols):
            data.columns = [c.title() for c in data.columns]
        elif data.columns.equals(self.reader_cols):
            pass
        else:
            raise ValueError('data is not ohlc-like')
        # data = data[['Open', 'Close', 'High', 'Low']]
        data = data[['Open', 'High', 'Low', 'Close']]
        plotting.LinePlot.__init__(self, data, **kwargs)

    def _get_plot_function(self):
        # from matplotlib.finance import candlestick
        from matplotlib.finance import candlestick_ohlc
        def _plot(data, ax, **kwds):
            # candles = candlestick(ax, data.values, **kwds)
            # candles = candlestick_ohlc(ax, data.values, **kwds)
            candles = candlestick_ohlc(ax, data.values, width=0.7, colorup='g', colordown='r', **kwds)
            return candles
        return _plot

    def _make_plot(self):
        from pandas.tseries.plotting import _decorate_axes, format_dateaxis
        plotf = self._get_plot_function()
        ax = self._get_ax(0)

        data = self.data
        data.index.name = 'Date'
        data = data.to_period(freq=self.freq)
        data = data.reset_index(level=0)

        if self._is_ts_plot():
            data['Date'] = data['Date'].apply(lambda x: x.ordinal)
            _decorate_axes(ax, self.freq, self.kwds)
            candles = plotf(data, ax, **self.kwds)
            format_dateaxis(ax, self.freq)
        else:
            from matplotlib.dates import date2num, AutoDateFormatter, AutoDateLocator
            data['Date'] = data['Date'].apply(lambda x: date2num(x.to_timestamp()))
            candles = plotf(data, ax, **self.kwds)
            locator = AutoDateLocator()
            ax.xaxis.set_major_locator(locator)
            ax.xaxis.set_major_formatter(AutoDateFormatter(locator))


plotting._all_kinds.append('ohlc')
plotting._common_kinds.append('ohlc')
plotting._plot_klass['ohlc'] = OhlcPlot


# In[113]:

toyota_tse[-30:].plot(kind='ohlc')
plt.show()


# In[110]:

import matplotlib.pyplot as plt
import matplotlib.finance as mpf
from matplotlib.dates import date2num

fig = plt.figure()
ax = plt.subplot()

xdate = [x.date() for x in toyota_tse[-30:].index] #Timestamp -> datetime
ohlc = np.vstack((date2num(xdate), toyota_tse[-30:].values.T)).T #datetime -> float
mpf.candlestick_ohlc(ax, ohlc, width=0.7, colorup='g', colordown='r')

ax.grid() #グリッド表示
ax.set_xlim(toyota_tse[-30:].index[0].date(), toyota_tse[-30:].index[-1].date()) #x軸の範囲
fig.autofmt_xdate() #x軸のオートフォーマット


# In[115]:

toyota_tse[-30:].asfreq('B').plot(kind='ohlc')
plt.subplots_adjust(bottom=0.25)
plt.show()


# In[5]:

start ='2000-10-02'
mac_tse = get_quote_yahoojp(2702, start=start)
mac_tse.head()


# In[ ]:




# In[50]:

import numpy as np
import pandas as pd

idx = pd.date_range('2016/06/01', '2016/07/31 23:59', freq='T')
dn = np.random.randint(2, size=len(idx))*2-1
rnd_walk = np.cumprod(np.exp(dn*0.0002))*100


# In[51]:

df = pd.Series(rnd_walk, index=idx).resample('B').ohlc()
df.plot()


# In[48]:

import matplotlib.pyplot as plt
import matplotlib.finance as mpf
from matplotlib.dates import date2num
get_ipython().magic('matplotlib inline')

fig = plt.figure()
ax = plt.subplot()

xdate = [x.date() for x in df.index] #Timestamp -> datetime
ohlc = np.vstack((date2num(xdate), df.values.T)).T #datetime -> float
mpf.candlestick_ohlc(ax, ohlc, width=0.7, colorup='g', colordown='r')

ax.grid() #グリッド表示
ax.set_xlim(df.index[0].date(), df.index[-1].date()) #x軸の範囲
fig.autofmt_xdate() #x軸のオートフォーマット

