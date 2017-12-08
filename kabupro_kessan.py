
# coding: utf-8

# # memo
用語の英訳など
決算短信: Financial Summary, Consolidated Financial Results, summary of accounts, brief note on the settlement of accounts
# | 項目 | 英訳 | 型 |
# |---|---|
# | 証券コード | code | int64 |
# | 企業名 | company | object |
# | 会計基準 | accounting method | object |
# | 連結個別 | consolidate, non-consolidate | object |
# | 決算期 | settlement term, fiscal term | object |
# | 決算期間 | quarter | object |
# | 期首 | term beginning | datetime64[ns] |
# | 期末 | term end | datetime64[ns] |
# | 名寄前勘定科目 (売上高欄) | account type | object |
# | 売上高 | net sales | float64 |
# | 営業利益 | operating income | float64 |
# | 経常利益 | ordinary income | float64 |
# | 純利益 | net income | float64 |
# | 一株当り純利益 | EPS, earnings per share | float64 |
# | 希薄化後一株当り純利益 | diluted net income per share | float64 |
# | 純資産又は株主資本 | shareholders' equity | float64 |
# | 総資産 | total assets | float64 |
# | 一株当り純資産 | shareholders’ equity per share | float64 |
# | 営業キャッシュフロー | cash flows from operating activities | float64 |
# | 投資キャッシュフロー | cash flows from investing activities | float64 |
# | 財務キャッシュフロー | cash flows from financing activities | float64 |
# | 情報公開日 (更新日) | publication date | datetime64[ns] |

# # importなど

# In[ ]:

import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import os
import re
import math
from dateutil.parser import parse
from sqlalchemy import create_engine
from sqlalchemy.types import Date, Integer, Float, Text

import stock

get_ipython().run_line_magic('matplotlib', 'inline')


# __クラス不使用__

# In[ ]:

# MySQL の接続作成
db_settings = {
    "host": 'localhost',
    "database": 'StockPrice_Yahoo_1',
    "user": 'user',
    "password": 'password',
    "port":'3306'
}
engine = create_engine('mysql://{user}:{password}@{host}:{port}/{database}'.format(**db_settings))


# __クラス使用__

# In[ ]:

sql = stock.sql() # MySQLに接続するクラスインスタンスを作成


# # 結合済みファイルの読み込み

# ## 初回新規ファイル作成時のみの代替処理

# In[ ]:

kabupro_kessan = new_kessan


# ## MySQL からの読み込み

# In[ ]:

table_name = 'kabupro_kessan'


# __クラス不使用__

# In[ ]:

kabupro_kessan = pd.read_sql_table(table_name, engine, index_col=None).drop('index', axis=1)


# __クラス使用__

# In[ ]:

kabupro_kessan = sql.read_table(table_name)


# ## csv からの読み込みの場合

# In[ ]:

kabupro_kessan = pd.read_csv('/Users/Really/Stockyard/_csv/kabupro_kessan.csv')


# In[ ]:

kabupro_kessan


# csv では読み込みのたびに型を設定しなおさなければならず面倒。  
# 整形後に保存した csv であれば配列の型の辞書を適用可能。

# In[ ]:

datatype = {'証券コード': 'int64',
                   '企業名': 'O',
                   '会計基準': 'O',
                   '連結個別': 'O',
                   '決算期': 'O',
                   '決算期間': 'O',
                   '期首': '<M8[ns]',
                   '期末': '<M8[ns]',
                   '名寄前勘定科目 (売上高欄)': 'O',
                   '売上高': 'float64',
                   '営業利益': 'float64',
                   '経常利益': 'float64',
                   '純利益': 'float64',
                   '一株当り純利益': 'float64',
                   '希薄化後一株当り純利益': 'float64',
                   '純資産又は株主資本': 'float64',
                   '総資産': 'float64',
                   '一株当り純資産': 'float64',
                   '営業キャッシュフロー': 'float64',
                   '投資キャッシュフロー': 'float64',
                   '財務キャッシュフロー': 'float64',
                   '情報公開日 (更新日)': '<M8[ns]'
                   }


# In[ ]:

# 整形済みのデータが実行環境内に存在する場合
datatype = dict(new_kessan.dtypes)
datatype


# In[ ]:

kabupro_kessan = kabupro_kessan.astype(datatype)


# In[ ]:

kabupro_kessan.dtypes


# # 新規追加ファイルの読み込み、整形、保存 

# ## 決算プロの決算短信 xls ファイルの読み込み

# In[ ]:

xls_file = '/Users/Really/Stockyard/_dl_data/20171112f.xls' # http://ke.kabupro.jp/doc/down40.htm


# In[ ]:

# 決算プロの決算短信xlsを読み込む
new_kessan = pd.read_excel(xls_file) 


# ## 内容の確認

# In[ ]:

new_kessan.head(20)


# In[ ]:

new_kessan.columns


# In[ ]:

# 列名を少し変更
new_kessan.columns = [
    '証券コード',
    '企業名',
    '会計基準',
    '連結個別',
    '決算期',
    '決算期間',
    '期首',
    '期末',
    '名寄前勘定科目 (売上高欄)',
    '売上高',
    '営業利益',
    '経常利益',
    '純利益',
    '一株当り純利益',
    '希薄化後一株当り純利益',
    '純資産又は株主資本',
    '総資産',
    '一株当り純資産',
    '営業キャッシュフロー',
    '投資キャッシュフロー',
    '財務キャッシュフロー',
    '情報公開日 (更新日)'
]


# In[ ]:

new_kessan[['証券コード', '企業名', '会計基準', '連結個別', '決算期', '決算期間', '期首', '期末',
       '名寄前勘定科目 (売上高欄)', '情報公開日 (更新日)']].head(20)


# In[ ]:

new_kessan.groupby('決算期').count()


# In[ ]:

new_kessan.groupby('決算期間').count()


# In[ ]:

new_kessan[['証券コード', '企業名', '期末', '売上高', '営業利益', '経常利益', '純利益', '情報公開日 (更新日)']].head(20)


# In[ ]:

new_kessan[['証券コード', '企業名', '期末', '一株当り純利益', '希薄化後一株当り純利益', '純資産又は株主資本',
       '総資産', '一株当り純資産', '営業キャッシュフロー', '投資キャッシュフロー', '財務キャッシュフロー',
       '情報公開日 (更新日)']].head(20)


# In[ ]:

new_kessan.dtypes


# In[ ]:

# 2015年のファイルでは int
new_kessan['純利益'].max()


# In[ ]:

# 2017年のファイルでは float
kabupro_kessan_new['純利益'].max()


# In[ ]:

# 2015年のファイルでは文字列が混入しているため object
new_kessan['希薄化後一株当り純利益'].max()


# In[ ]:

new_kessan['希薄化後一株当り純利益'].min()


# In[ ]:

new_kessan['期末'].min()


# In[ ]:

new_kessan['情報公開日 (更新日)'].min()


# In[ ]:

new_kessan.duplicated().any()


# In[ ]:

new_kessan.isnull().any()


# ## 整形

# In[ ]:

# 2015年のファイルでは int 型だったので float 型に
new_kessan['純利益'] = new_kessan['純利益'].astype(float)


# In[ ]:

# 2015年のファイルでは index 17979 に '―' が入っていたために列が object 型になってしまっている
new_kessan[new_kessan['希薄化後一株当り純利益'].apply(lambda x: type(x) is str)]


# In[ ]:

# '―'  を NaN に置換
new_kessan.loc[new_kessan['希薄化後一株当り純利益'].apply(lambda x: type(x) is str), '希薄化後一株当り純利益'] = np.nan


# In[ ]:

new_kessan['希薄化後一株当り純利益'][17979]


# In[ ]:

new_kessan['希薄化後一株当り純利益'].max()


# In[ ]:

# float 型に変換
new_kessan['希薄化後一株当り純利益'] = new_kessan['希薄化後一株当り純利益'].astype(float)


# In[ ]:

# ソート
new_kessan = new_kessan.sort_values(['証券コード', '連結個別', '期末', '情報公開日 (更新日)']).reset_index(drop=True)


# ## csv に保存

# In[ ]:

# 保存
new_kessan.to_csv('/Users/Really/Stockyard/_csv/kabupro_kessan_20171112.csv')


# In[ ]:

# 保存したファイルの確認
csv_kessan = pd.read_csv('/Users/Really/Stockyard/_csv/kabupro_kessan_20171112.csv', index_col=0)
csv_kessan.head(20)


# ## MySQL に保存

# In[ ]:

table_name = 'kabupro_kessan_20171112'


# このケースではデータ型の定義は必要ないのかも  
# 定義しなくても意図通りの型で書き込めた

# In[ ]:

# データ型を定義しなくても意図通りの型で書き込めた
new_kessan.to_sql(table_name, engine, if_exists='replace')


# In[ ]:

# このケースではデータ型の定義は必要ないのかも
# sqlalchemy.typesで定義されたデータ型を辞書形式で設定して保存
data_type = {
    '`証券コード`': Integer(),
    '`企業名`': Text(),
    '`会計基準`': Text(),
    '`連結個別`': Text(),
    '`決算期`': Text(),
    '`決算期間`': Text(),
    '`期首`': Date(),
    '`期末`': Date(),
    '`名寄前勘定科目 (売上高欄)`': Text(),
    '`売上高`': Float(),
    '`営業利益`': Float(),
    '`経常利益`': Float(),
    '`純利益`': Float(),
    '`一株当り純利益`': Float(),
    '`希薄化後一株当り純利益`': Float(),
    '`純資産又は株主資本`': Float(),
    '`総資産`': Float(),
    '`一株当り純資産`': Float(),
    '`営業キャッシュフロー`': Float(),
    '`投資キャッシュフロー`': Float(),
    '`財務キャッシュフロー`': Float(),
    '`情報公開日 (更新日)`': Date()
}

kabupro_kessan.to_sql(table_name, engine, if_exists='replace', dtype=data_type)


# __保存したデータの確認__

# In[ ]:

sql_kessan = pd.read_sql_table(table_name, engine, index_col=None).drop('index', axis=1)


# In[ ]:

sql_kessan.head(20)


# In[ ]:

sql_kessan.columns


# In[ ]:

sql_kessan.dtypes


# In[ ]:

# nan 同士の比較は False になってしまうのでゼロで埋めて比較
(sql_kessan.fillna(0) == new_kessan.fillna(0)).any()


# # 新旧ファイルの結合

# In[ ]:

kabupro_kessan = kabupro_kessan.append(new_kessan)


# In[ ]:

kabupro_kessan.tail(20)


# In[ ]:

kabupro_kessan.dtypes


# In[ ]:

# ソート
kabupro_kessan = kabupro_kessan.sort_values(['証券コード', '連結個別', '期末', '情報公開日 (更新日)']).reset_index(drop=True)


# ## 重複の確認、削除

# In[ ]:

kabupro_kessan.duplicated().any()


# In[ ]:

# keep=Falseで重複行のいずれかではなくすべてを表示
kabupro_kessan[kabupro_kessan.duplicated(keep=False)]


# In[ ]:

kabupro_kessan = kabupro_kessan.drop_duplicates()


# In[ ]:

# インデックスの振り直し
kabupro_kessan = kabupro_kessan.reset_index(drop=True)


# ## 内容の確認

# In[ ]:

kabupro_kessan.isnull().any()


# In[ ]:

kabupro_kessan['期末'].min()


# In[ ]:

kabupro_kessan['情報公開日 (更新日)'].min()


# In[ ]:

kabupro_kessan['期末'].max()


# In[ ]:

kabupro_kessan['情報公開日 (更新日)'].max()


# In[ ]:

kabupro_kessan.describe()


# ## csv に保存

# In[ ]:

# 保存
kabupro_kessan.to_csv('/Users/Really/Stockyard/_csv/kabupro_kessan.csv')


# In[ ]:

# 保存したファイルの確認
csv_kessan = pd.read_csv('/Users/Really/Stockyard/_csv/kabupro_kessan.csv', index_col=0)
csv_kessan.head(20)


# ## MySQL に保存

# In[ ]:

table_name = 'kabupro_kessan'


# このケースではデータ型の定義は必要ないのかも  
# 定義しなくても意図通りの型で書き込めた

# __クラス不使用__

# In[ ]:

# データ型を定義しなくても意図通りの型で書き込めた
kabupro_kessan.to_sql(table_name, engine, if_exists='replace')


# In[ ]:

# このケースではデータ型の定義は必要ないのかも
# sqlalchemy.typesで定義されたデータ型を辞書形式で設定して保存
data_type = {
    '`証券コード`': Integer(),
    '`企業名`': Text(),
    '`会計基準`': Text(),
    '`連結個別`': Text(),
    '`決算期`': Text(),
    '`決算期間`': Text(),
    '`期首`': Date(),
    '`期末`': Date(),
    '`名寄前勘定科目 (売上高欄)`': Text(),
    '`売上高`': Float(),
    '`営業利益`': Float(),
    '`経常利益`': Float(),
    '`純利益`': Float(),
    '`一株当り純利益`': Float(),
    '`希薄化後一株当り純利益`': Float(),
    '`純資産又は株主資本`': Float(),
    '`総資産`': Float(),
    '`一株当り純資産`': Float(),
    '`営業キャッシュフロー`': Float(),
    '`投資キャッシュフロー`': Float(),
    '`財務キャッシュフロー`': Float(),
    '`情報公開日 (更新日)`': Date()
}

kabupro_kessan.to_sql(table_name, engine, if_exists='replace', dtype=data_type)


# __クラス使用__

# In[ ]:

sql.write_table(table_name, kabupro_kessan)


# __保存したデータの確認__

# __クラス不使用__

# In[ ]:

sql_kessan = pd.read_sql_table(table_name, engine, index_col=None).drop('index', axis=1)


# __クラス使用__

# In[ ]:

sql_kessan = sql.read_table(table_name)


# In[ ]:

sql_kessan.head(20)


# In[ ]:

sql_kessan.tail(20)


# In[ ]:

sql_kessan.columns


# In[ ]:

sql_kessan.dtypes


# In[ ]:

kabupro_kessan.dtypes


# In[ ]:

# nan 同士の比較は False になってしまうのでゼロで埋めて比較
(sql_kessan.fillna(0) == kabupro_kessan.fillna(0)).any()


# # 別アプリで csv に変換済みのファイルを読み込む場合

# In[ ]:

# 決算プロの決算短信csv(エクセルファイルより書き出し)を読み込む http://ke.kabupro.jp/doc/down40.htm
csv_version = pd.read_csv('/Users/Really/Stockyard/_dl_data/kabupro_20171112f.csv') 


# ## 内容の確認

# In[ ]:

csv_version


# csv からの読み込みではほとんどの列の型が object になる (読み込み時にオプションで指定することも可能らしいが、列が多いと面倒)

# In[ ]:

csv_version.dtypes


# In[ ]:

csv_version.duplicated().any()


# In[ ]:

csv_version.isnull().any()


# In[ ]:

csv_version['期末'].min()


# In[ ]:

csv_version['情報公開日 (更新日)'].min()


# In[ ]:

csv_version[csv_version['連結個別'] == '個別']


# In[ ]:

csv_version[csv_version['連結個別'] == '連結']


# In[ ]:

csv_version.describe()


# ## 整形

# In[ ]:

# 日付のパース、datetime.dateへの型変換
csv_version['期首'] = csv_version['期首'].apply(lambda x: parse(x).date())
csv_version['期末'] = csv_version['期末'].apply(lambda x: parse(x).date())
csv_version['情報公開日 (更新日)'] = csv_version['情報公開日 (更新日)'].apply(lambda x: parse(x).date())


# In[ ]:

# 同じく日付のパース、datetime.dateへの型変換をまとめて。1列ずつやるより少し遅い？ 23s -> 30s
csv_version[['期首', '期末', '情報公開日 (更新日)']] = csv_version[['期首', '期末', '情報公開日 (更新日)']].applymap(lambda x: parse(x).date())


# In[ ]:

# pandasのTimestampへの型変換
# まとめてできないの？
# csv_version[['期首', '期末', '情報公開日 (更新日)']] = pd.to_datetime(csv_version[['期首', '期末', '情報公開日 (更新日)']], format='%Y-%m-%d')
csv_version['期首'] = pd.to_datetime(csv_version['期首'], format='%Y-%m-%d')
csv_version['期末'] = pd.to_datetime(csv_version['期末'], format='%Y-%m-%d')
csv_version['情報公開日 (更新日)'] = pd.to_datetime(csv_version['情報公開日 (更新日)'], format='%Y-%m-%d')


# In[ ]:

# 数値に変換する列のリスト作成
num_list = [
    '売上高', 
    '営業利益', 
    '経常利益', 
    '純利益', 
    '一株当り純利益',
    '希薄化後一株当り純利益', 
    '純資産又は株主資本', 
    '総資産', 
    '一株当り純資産', 
    '営業キャッシュフロー',
    '投資キャッシュフロー', 
    '財務キャッシュフロー'
]


# In[ ]:

# 数値に変換する項目の「,」を削除、float型に変換
csv_version[num_list] = csv_version[num_list].apply(lambda x: x.str.replace(',','')).astype(float)


# In[ ]:

# 冗長なやり方

# 2つのリストから1つの辞書を作成する方法
# num_dict = dict(zip(num_list, [str for i in range(len(num_list))]))

csv_version = csv_version.astype(dict(zip(num_list, [str for i in range(len(num_list))])))
csv_version[num_list] = csv_version[num_list].applymap(lambda x: x.replace(',', ''))
csv_version = csv_version.astype(dict(zip(num_list, [float for i in range(len(num_list))])))


# In[ ]:

# さらに冗長なやり方

csv_version = csv_version.astype({
    '売上高': str, 
    '営業利益': str, 
    '経常利益': str, 
    '純利益': str, 
    '一株当り純利益': str,
    '希薄化後一株当り純利益': str, 
    '純資産又は株主資本': str, 
    '総資産': str, 
    '一株当り純資産': str, 
    '営業キャッシュフロー': str,
    '投資キャッシュフロー': str, 
    '財務キャッシュフロー': str})

csv_version[[
    '売上高', 
    '営業利益', 
    '経常利益', 
    '純利益', 
    '一株当り純利益',
    '希薄化後一株当り純利益', 
    '純資産又は株主資本', 
    '総資産', 
    '一株当り純資産', 
    '営業キャッシュフロー',
    '投資キャッシュフロー', 
    '財務キャッシュフロー'
    ]] = csv_version[[
        '売上高', 
        '営業利益', 
        '経常利益', 
        '純利益', 
        '一株当り純利益',
        '希薄化後一株当り純利益', 
        '純資産又は株主資本', 
        '総資産', 
        '一株当り純資産', 
        '営業キャッシュフロー',
        '投資キャッシュフロー', 
        '財務キャッシュフロー'
        ]].applymap(lambda x: x.replace(',', ''))

csv_version = csv_version.astype({
    '売上高': float, 
    '営業利益': float, 
    '経常利益': float, 
    '純利益': float, 
    '一株当り純利益': float,
    '希薄化後一株当り純利益': float, 
    '純資産又は株主資本': float, 
    '総資産': float, 
    '一株当り純資産': float, 
    '営業キャッシュフロー': float,
       '投資キャッシュフロー': float, 
    '財務キャッシュフロー': float})


# In[ ]:

# ソート
csv_version = csv_version.sort_values(['証券コード', '連結個別', '期末', '情報公開日 (更新日)']).reset_index(drop=True)


# # 四半期業績の差分を作ってみる

# __もうちょっと上手いやり方ありそう__

# In[ ]:

code = 7203


# In[ ]:

kabupro.columns


# In[ ]:

kabupro.ix[(kabupro['証券コード'] == code) & (kabupro['会計基準'] == '米国基準'), 
           ['決算期', '期末', '売上高', '営業利益', '経常利益', '純利益', '一株当り純利益', '情報公開日 (更新日)']].tail(10)


# In[ ]:

diff_test = kabupro.ix[(kabupro['証券コード'] == code) & (kabupro['会計基準'] == '米国基準'), 
           ['決算期', '売上高', '営業利益', '経常利益', '純利益', '一株当り純利益']].tail(10)


# In[ ]:

diff_test


# In[ ]:

diff_test[['売上高差分', '営業利益差分', '経常利益差分', '純利益差分', '一株当り純利益差分']] = diff_test[['売上高', '営業利益', '経常利益', '純利益', '一株当り純利益']]


# In[ ]:

# 全銘柄一括でやるなら、上の行とコードが一致するかの判定を追加
for count in range(70501, 70501 + len(diff_test) - 1):
    if diff_test.loc[count, '決算期'] == diff_test.loc[count - 1, '決算期']:
        diff_test.loc[count, '売上高差分'] = diff_test.loc[count, '売上高'] - diff_test.loc[count - 1, '売上高']
        diff_test.loc[count, '営業利益差分'] = diff_test.loc[count, '営業利益'] - diff_test.loc[count - 1, '営業利益']
        diff_test.loc[count, '経常利益差分'] = diff_test.loc[count, '経常利益'] - diff_test.loc[count - 1, '経常利益']
        diff_test.loc[count, '純利益差分'] = diff_test.loc[count, '純利益'] - diff_test.loc[count - 1, '純利益']
        diff_test.loc[count, '一株当り純利益差分'] = diff_test.loc[count, '一株当り純利益'] - diff_test.loc[count - 1, '一株当り純利益']


# In[ ]:

diff_test[['決算期', '売上高差分', '営業利益差分', '経常利益差分', '純利益差分', '一株当り純利益差分']]
# 一株当り純利益差分が株探の１株益と揃わない

