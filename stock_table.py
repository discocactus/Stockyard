
# coding: utf-8

# In[ ]:


# pandasのto_csvのデフォルトエンコーディングはUTF-8。
# 書き出したファイルをpandasで読み込む場合は問題ない。
# Excelで開こうとすると強制的にShift-JISで開こうとするので、日本語が文字化けするがいたしかたなし？
# エンコーディングをUTF-8以外にするのは弊害が大きいような気がする。
# Excelではファイルを開くのではなく、データのインポートを利用すればエンコードの指定が可能。
# メモ帳等で開いて上書き保存すれば自動的にBOMが付加され、Excelもエンコードを認識するらしいが、
# BOMを付けるのもExcel以外から開く際に弊害があるらしい。


# # import

# In[ ]:


import numpy as np
import pandas as pd
import pandas.tseries.offsets as offsets
import datetime as dt
import time
import importlib

import stock


# In[ ]:


importlib.reload(stock)


# In[ ]:


# pandas の最大表示列数を設定 (max_rows で表示行数の設定も可能)
pd.set_option('display.max_columns', 30)


# # パスの設定

# In[ ]:


# jpx_path = '/Users/Really/Stockyard/_dl_data'
jpx_path = 'D:\stockyard\_dl_data'


# In[ ]:


# csv_path = '/Users/Really/Stockyard/_csv'
csv_path = 'D:\stockyard\_csv'


# # 上場一覧から各種テーブルを作成 (メインはyahoo_stock_table)

# In[ ]:


file_month = 1801


# In[ ]:


# 東証のエクセルファイルを読み込む # http://www.jpx.co.jp/markets/statistics-equities/misc/01.html
all_stock_table = pd.read_excel('{0}/data_j_{1}.xls'.format(jpx_path, file_month))
all_stock_table.columns = ['date', 'code', 'name', 'market', 'code_33', 'category_33', 'code_17', 'category_17', 'code_scale', 'scale'] # 列名を変更


# In[ ]:


all_stock_table


# In[ ]:


# marketの種別で集計
all_stock_table.groupby('market').count()


# In[ ]:


# 上場一覧のテーブル保存
all_stock_table.to_csv('{0}/all_stock_table.csv'.format(csv_path))


# In[ ]:


pd.read_csv('{0}/all_stock_table.csv'.format(csv_path), index_col=0)


# In[ ]:


# PRO Marketを除いたテーブルの作成 (YahooにはPRO Marketのデータはない)
yahoo_stock_table = all_stock_table.loc[~all_stock_table['market'].str.contains('PRO Market')].reset_index(drop=True)
yahoo_stock_table


# In[ ]:


# PRO Marketを除いたテーブルの保存
yahoo_stock_table.to_csv('{0}/yahoo_stock_table.csv'.format(csv_path))


# In[ ]:


pd.read_csv('{0}/yahoo_stock_table.csv'.format(csv_path), index_col=0)


# In[ ]:


# 内国株のテーブル作成
domestic_stock_table = all_stock_table.loc[all_stock_table['market'].str.contains('内国株')].reset_index(drop=True)
domestic_stock_table


# In[ ]:


# 内国株のテーブル保存
domestic_stock_table.to_csv('{0}/domestic_stock_table.csv'.format(csv_path))


# In[ ]:


pd.read_csv('{0}/domestic_stock_table.csv'.format(csv_path), index_col=0)


# In[ ]:


# 内国株, PRO Market 以外のテーブル作成
ex_stock_table = yahoo_stock_table[~yahoo_stock_table['code'].isin(domestic_stock_table['code'])].reset_index(drop=True)

# 正規表現を使った書き方の例。
# ex_stock_table = all_stock_table.ix[all_stock_table['market'].str.contains('[^内国株）PRO Market]...$')].reset_index(drop=True)
# 文字列末尾の合致検索では、'$' の前の '.' の数で検索する文字数 ('.' * n + '$') が決定されているっぽい。
# つまりこの場合だと 'PRO Market' で実際に合致が確認されているのは末尾4文字の 'rket' 。
# '内国株）', 'PRO Market' をそれぞれグループ化するために' ()' で括る必要はないみたい。(ただし括っても同じ結果になる) 

# 下の書き方だと最後の1文字しか見ないことになるので、外国株も除外されてしまう。
# ex_stock_table = all_stock_table.ix[all_stock_table['market'].str.contains('[^内国株）PRO Market]$')].reset_index(drop=True)
# 上はつまり下の書き方と同じこと。
# ex_stock_table = all_stock_table.ix[all_stock_table['market'].str.contains('[^）t]$')].reset_index(drop=True)

ex_stock_table


# In[ ]:


# marketの種別で集計
ex_stock_table.groupby('market').count()


# In[ ]:


# 内国株, PRO Market 以外のテーブル保存
ex_stock_table.to_csv('{0}/ex_stock_table.csv'.format(csv_path))


# In[ ]:


pd.read_csv('{0}/ex_stock_table.csv'.format(csv_path), index_col=0)


# In[ ]:


# 外国株のテーブル作成
foreign_stock_table = all_stock_table.loc[all_stock_table['market'].str.contains('外国株')].reset_index(drop=True)
foreign_stock_table


# In[ ]:


# 外国株のテーブル保存
foreign_stock_table.to_csv('{0}/foreign_stock_table.csv'.format(csv_path))


# In[ ]:


pd.read_csv('{0}/foreign_stock_table.csv'.format(csv_path), index_col=0)


# In[ ]:


# 型の確認
pd.DataFrame([
    pd.read_csv('{0}/all_stock_table.csv'.format(csv_path), index_col=0).dtypes,
    pd.read_csv('{0}/yahoo_stock_table.csv'.format(csv_path), index_col=0).dtypes,
    pd.read_csv('{0}/domestic_stock_table.csv'.format(csv_path), index_col=0).dtypes,
    pd.read_csv('{0}/ex_stock_table.csv'.format(csv_path), index_col=0).dtypes,
    pd.read_csv('{0}/foreign_stock_table.csv'.format(csv_path), index_col=0).dtypes],
    index=['all', 'yahoo', 'domestic', 'ex', 'foreign'])


# # 上場一覧の更新

# ## 新旧ファイルの読み込み

# In[ ]:


new_file_month = 1801
old_file_month = 1712


# In[ ]:


# 東証のエクセルファイルを読み込む # http://www.jpx.co.jp/markets/statistics-equities/misc/01.html
new_stock_table = pd.read_excel('{0}/data_j_{1}.xls'.format(jpx_path, new_file_month))
new_stock_table.columns = ['date', 'code', 'name', 'market', 'code_33', 'category_33', 'code_17', 'category_17', 'code_scale', 'scale'] # 列名を変更


# In[ ]:


new_stock_table


# In[ ]:


new_stock_table.dtypes


# In[ ]:


# 旧エクセルファイルの読み込み
old_stock_table = pd.read_excel('{0}/data_j_{1}.xls'.format(jpx_path, old_file_month))
old_stock_table.columns = ['date', 'code', 'name', 'market', 'code_33', 'category_33', 'code_17', 'category_17', 'code_scale', 'scale'] # 列名を変更


# In[ ]:


old_stock_table


# In[ ]:


old_stock_table.dtypes


# ## 新規上場銘柄

# In[ ]:


# 新規上場銘柄のテーブル作成
new_added = new_stock_table[~new_stock_table['code'].isin(old_stock_table['code'])].reset_index(drop=True)
new_added


# In[ ]:


# 新規上場銘柄のテーブル保存
new_added.to_csv('{0}/new_added_stock_table.csv'.format(csv_path))


# In[ ]:


pd.read_csv('{0}/new_added_stock_table.csv'.format(csv_path), index_col=0)


# In[ ]:


# 新規上場銘柄の履歴テーブルの読み込み
saved_added = pd.read_csv('{0}/added_stock_table.csv'.format(csv_path), index_col=0)
saved_added


# In[ ]:


saved_added.dtypes


# In[ ]:


# 新旧テーブルの連結
added_stock_table = saved_added.append(new_added).reset_index(drop=True)
added_stock_table


# In[ ]:


added_stock_table.dtypes


# In[ ]:


# 新規上場銘柄の履歴テーブル保存
added_stock_table.to_csv('{0}/added_stock_table.csv'.format(csv_path))


# In[ ]:


pd.read_csv('{0}/added_stock_table.csv'.format(csv_path), index_col=0)


# ## 上場廃止銘柄

# In[ ]:


# 上場廃止銘柄のテーブル作成
new_discontinued = old_stock_table[~old_stock_table['code'].isin(new_stock_table['code'])].reset_index(drop=True)
new_discontinued


# In[ ]:


# 上場廃止銘柄の履歴テーブルの読み込み
saved_discontinued = pd.read_csv('{0}/discontinued_stock_table.csv'.format(csv_path), index_col=0)
saved_discontinued


# In[ ]:


saved_discontinued.dtypes


# In[ ]:


# 新旧テーブルの連結
discontinued_stock_table = saved_discontinued.append(new_discontinued).reset_index(drop=True)
discontinued_stock_table


# In[ ]:


discontinued_stock_table.dtypes


# In[ ]:


# 上場廃止銘柄の履歴テーブル保存
discontinued_stock_table.to_csv('{0}/discontinued_stock_table.csv'.format(csv_path))


# In[ ]:


pd.read_csv('{0}/discontinued_stock_table.csv'.format(csv_path), index_col=0)


# # 連続読み込み用コードリストの作成例

# ## yahoo 連続読み込み用コードリスト作成

# In[ ]:


start_index = 0
increase_number = 10
end_index = start_index + increase_number

reading_code = stock.get_yahoo_stock_code(start_index, end_index)
print(reading_code[-10:])
print('Next start from {0}'.format(start_index + increase_number))


# ## 新規銘柄読み込み用コードリスト作成

# In[ ]:


reading_code = stock.get_new_added_stock_code()
reading_code

