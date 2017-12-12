
# coding: utf-8

# # API Reference

# http://pandas.pydata.org/pandas-docs/version/0.19.2/api.html

# # Import

# In[ ]:

import numpy as np
import pandas as pd
import pandas.tseries.offsets as offsets
import datetime as dt
import time
import importlib
import logging
from retry import retry
from sqlalchemy import create_engine
from sqlalchemy.types import Date, Integer, Text

import stock


# In[ ]:

importlib.reload(stock)


# In[ ]:

# pandas の最大表示列数を設定 (max_rows で表示行数の設定も可能)
pd.set_option('display.max_columns', 30)


# # 超大事。浅いコピーと深いコピー

# ## 浅いコピー。  
# コピーというか参照なので変更内容は両方に反映される

# In[ ]:

df = tables[11]


# ## 深いコピー。  
# 独立したコピーを作りたい場合は.copy()メソッドで

# In[ ]:

df = tables[11].copy()


# # 要素選択 - 科学技術計算のためのPython入門  
# https://github.com/pyjbooks/py4science  
# 注) ラベルによるスライシングの場合、Python標準とは異なり終端ポイントまで含まれる  
# OSや実装により処理速度に差がある場合がある

# ## サンプル作成

# In[ ]:

df = pd.DataFrame(np.arange(12).reshape((3,4)),
                 index=list('xyz'),
                 columns=list('abcd'))


# In[ ]:

df


# ## 要素参照 (プロパティ使用)

# In[ ]:

# y行 b列
df.at['y', 'b']


# In[ ]:

# y行 b列
df.loc['y', 'b']


# In[ ]:

# x〜y行 b列
df.loc['x':'y', 'b']


# In[ ]:

# 最初からy行 全列
df.loc[:'y', :]


# In[ ]:

# 0行 1列の値 (1行目、2列目)
df.iat[0, 1]


# In[ ]:

# 1行 1列の値 (2行目、2列目)
df.iloc[1, 1]


# In[ ]:

# 0〜1行 最後の2列の値
df.iloc[0:2, -2:]


# In[ ]:

# 0行 全列
df.iloc[:1, :]


# In[ ]:

# x行 a, d列の値 (結果はシリーズ)
df.ix['x', ['a', 'd']]


# In[ ]:

# x行を取り出し (結果はシリーズ)
df.ix['x']


# In[ ]:

# x行 a列
df.ix['x']['a']


# In[ ]:

# a列を取り出し (結果はシリーズ)
df.ix[:, 'a']


# In[ ]:

# d列の値が6より大の行の最初の2列
df.ix[df.d > 6, :2]


# ## 要素参照 (直接指定)

# In[ ]:

# 列ラベル a (結果はシリーズ)
df['a']


# In[ ]:

# 列ラベル a,c
df[['a', 'c']]


# In[ ]:

# 0〜1行
df[:2]


# In[ ]:

# d列の値が6より大の行
df[df['d'] > 6]


# # pandas データ選択処理をちょっと詳しく - StatsFragments  
# http://sinhrks.hatenablog.com/entry/2014/11/12/233216

# ## サンプルデータの準備

# In[ ]:

import pandas as pd

s = pd.Series([1, 2, 3], index = ['I1', 'I2', 'I3'])

df = pd.DataFrame({'C1': [11, 21, 31],
                   'C2': [12, 22, 32],
                   'C3': [13, 23, 33]},
                  index = ['I1', 'I2', 'I3'])


# In[ ]:

s


# In[ ]:

df


# ## \__getitem__ での選択

# ### 直接選択

# In[ ]:

s[0]


# In[ ]:

s['I1']


# In[ ]:

df['C1']


# In[ ]:

# 番号を数値として渡すとNG!
df[1]


# In[ ]:

# 番号のリストならOK (リストだと columns からの選択)
df[[1]] 


# In[ ]:

# 番号のスライスもOK (スライスだと index からの選択)
df[1:2]


# In[ ]:

# NG!
df['I1']


# In[ ]:

s[[True, False, True]]


# In[ ]:

df[[True, False, True]] # (index を指定したことになる)


# In[ ]:

# bool の DataFrame を作る
df > 21


# In[ ]:

# bool の DataFrame で選択
df[df > 21] 


# __引数による 返り値の違い__  
#   
# 引数を値だけ (ラベル, 数値)で渡すと次元が縮約され、Series では単一の値, DataFrame では Series が返ってくる。  
# 元のデータと同じ型の返り値がほしい場合は 引数をリスト型にして渡せばいい。

# In[ ]:

# 返り値は 値
s['I1']


# In[ ]:

# 返り値は Series
s[['I1']]


# In[ ]:

# 返り値は Series
df['C1']


# In[ ]:

# 返り値は DataFrame
df[['C1']]


# ## index, columns を元にした選択 ( ix, loc, iloc )  

# ### ix プロパティ

# ix プロパティを使うと DataFrame で index, columns 両方を指定してデータ選択を行うことができる。  
# ( Series の挙動は \__getitem\__ と同じ)  
#   
# 引数として使える形式は \__getitem\__ と同じだが、ix では DataFrame も以下すべての形式を使うことができる。  
# ・名前 (もしくは名前のリスト)  
# ・順序 (番号)　(もしくは番号のリスト)  
# ・index, もしくは columns と同じ長さの bool のリスト  
#   
# ix はメソッドではなくプロパティなので、呼び出しは以下のようになる。  
# ・Series.ix[?] : ? にはindex を特定できるものを指定  
# ・DataFrame.ix[?, ?] : ? にはそれぞれ index, columns の順に特定できるものを指定  

# In[ ]:

# 名前による指定
s.ix['I2']


# In[ ]:

df.ix['I2', 'C2']


# In[ ]:

# 順序による指定
s.ix[1]


# In[ ]:

df.ix[1, 1]


# In[ ]:

# 名前のリストによる指定
s.ix[['I1', 'I3']]


# In[ ]:

df.ix[['I1', 'I3'], ['C1', 'C3']]


# In[ ]:

# bool のリストによる指定
s.ix[[True, False, True]]


# In[ ]:

df.ix[[True, False, True], [True, False, True]]


# In[ ]:

# 第一引数, 第二引数で別々の形式を使うこともできる
df.ix[1:, "C1"]


# __DataFrame.ix の補足__  
#   
# DataFrameで第二引数を省略した場合は index への操作になる。

# In[ ]:

df.ix[1]


# DataFrame で columns に対して操作したい場合、以下のように第一引数を空にするとエラーになる。  
# 第一引数には : を渡す必要がある (もしくは ixを使わず 直接 \__getitem\__ する )。

# In[ ]:

df.ix[, 'C3']


# In[ ]:

df.ix[:, 'C3']


# __引数による 返り値の違い__  
# 
# 引数の型による返り値の違いは \__getitem\__ の動きと同じ。  
# Series については挙動もまったく同じなので、ここでは DataFrame の場合だけ例示。

# In[ ]:

# 返り値は 値
df.ix[1, 1]


# In[ ]:

# 返り値は Series
df.ix[[1], 1]


# In[ ]:

# 返り値は DataFrame
df.ix[[1], [1]]


# ### iloc, loc プロパティ - ラベル名 もしくは 番号 どちらかだけを指定してデータ選択したい場合

# 例えば、  
# ・index, columns が int 型である  
# ・index, columns に重複がある  
# ・データによって index, columns の値が変わる  
# 
# 内部的には ix は ラベルを優先して処理を行うため、  
# 上記のようなデータでは ix を使うと意図しない挙動をする可能性がある。  
# 明示的に index, columns を番号で指定したい！というときには iloc を使う。  
# 同様に、明示的にラベルのみで選択したい場合は loc を使う。  
# ix, iloc, loc については文法 / 挙動は基本的に一緒で、使える引数の形式のみが異なる。

# | 使える引数の形式 | ix | iloc | loc |
# |---|:---:|:---:|:---:|
# | 名前 (もしくは名前のリスト) |	○ |	- |	○ |
# | 順序 (番号)　(もしくは番号のリスト) | ○ | ○ | - |
# | index もしくは columns と同じ長さの bool のリスト | ○ | ○ | ○ |

# ### .columns プロパティによるアクセスについて

# Series の index, DataFrame の columns はプロパティとしてもアクセスできる。  
# が、オブジェクト自体のメソッド/プロパティと衝突した場合 (例えば ix を列名に持つデータがある等) は  
# メソッド/プロパティが優先されるので使わないほうがよい。  

# __つまり__  
#   
# 
# ・手元で対話的にちょっと試す場合は ix が便利。  
# ・ある程度の期間使うようなスクリプトを書く場合は 少し面倒でも iloc, loc が安全。

# # bool のリストによるデータ選択 - StatsFragments  
# http://sinhrks.hatenablog.com/entry/2014/11/15/230705

# ## 序章

# pandas.Series や numpy.array への演算は原則 リスト内の各要素に対して適用され、  
# 結果は真偽値の numpy.array (もしくは Series ) になる。  
# また、真偽値の numpy.array 同士で論理演算することもできる。

# In[ ]:

df.columns == 'C3'


# In[ ]:

df.columns.isin(['C1', 'C2'])


# In[ ]:

(df.columns == 'C3') | (df.columns == 'C2')


# そのため、上記のような条件式をそのまま行列選択時の引数として使うことができる。

# In[ ]:

df.ix[df.index.isin(['I1', 'I2']), df.columns == 'C3']


# 上の例ではあまりありがたみはないと思うが、  
# 外部関数で bool のリストを作ってしまえばどんなに複雑な行列選択でもできる。

# ## サンプルデータの準備

# In[ ]:

df = pd.DataFrame({'N1': [1, 2, 3, 4, 5, 6],
                   'N2': [10, 20, 30, 40, 50, 60],
                   'N3': [6, 5, 4, 3, 2, 1],
                   'F1': [1.1, 2.2, 3.3, 4.4, 5.5, 6.6],
                   'F2': [1.1, 2.2, 3.3, 4.4, 5.5, 6.6],
                   'S1': ['A', 'b', 'C', 'D', 'E', 'F'],
                   'S2': ['A', 'X', 'X', 'X', 'E', 'F'],
                   'D1': pd.date_range('2014-11-01', freq='D', periods=6)},
                  index=pd.date_range('2014-11-01', freq='M', periods=6),
                  columns=['N1', 'N2', 'N3', 'F1', 'F2', 'S1', 'S2', 'D1'])


# In[ ]:

df


# pd.date_range の使い方は以下の記事参照。  
# http://sinhrks.hatenablog.com/entry/2014/11/09/183603

# ## index, columns のラベルを特定の条件で選択

# __補足)__ 内部実装としては index, columns は どちらも pd.Index 型のクラスが使われている ( DatetimeIndex は Index のサブクラス)。  
# index, columns とも裏側にあるオブジェクトは同一のため、このセクションでの方法は 行 / 列が入れ替わっても使える。

# In[ ]:

df.index


# In[ ]:

df.columns


# ### pd.Index.map - ラベルに関数適用して選択したい

# たとえば 大文字の "N" から始まるラベル名のみ抽出したいなら

# In[ ]:

df.columns.map(lambda x: x.startswith('N'))


# In[ ]:

df.ix[:, df.columns.map(lambda x: x.startswith('N'))]


# ということで map を使えば index, columns のラベルに対してあらゆる関数を適用してデータ選択できる。  

# ### .str アクセサを使用した文字列処理関数による同様の選択

# In[ ]:

df.ix[:, df.columns.str.startswith('N')]


#  .str アクセサについては以降の記載を参照。  

# ### pd.Index.isin - リストに含まれるラベルだけ選択したい

#   
# たとえば選択したいラベルのリストがあり、そこに含まれるものだけ選択したい場合、  
# 選択候補リストに余計なラベルが含まれていると、\__getitem\__ では KeyError になり、ix では NaN (値のない) 列ができてしまう。

# In[ ]:

df[['N1', 'N2', 'N4']]


# In[ ]:

df.ix[:, ['N1', 'N2', 'N4']]


# 存在する列だけがほしい、NaN の列は不要な場合、pd.Index.isin。

# In[ ]:

df.columns.isin(['N1', 'N2', 'N4'])


# In[ ]:

df.ix[:, df.columns.isin(['N1', 'N2', 'N4'])]


# ### pd.Index.sort_values - ラベルをソートして選択したい

# __補足)__ インデックスのソートは .sort_index

# columns をアルファベット順に並べ替えて、前から3つを取得したければ、

# In[ ]:

df.columns.sort_values()


# In[ ]:

df.columns.sort_values()[:3]


# In[ ]:

df[df.columns.sort_values()[:3]]


# ### DatetimeIndex へのプロパティアクセス - 特定の年, 月, etc... のデータだけ選択したい

# DatetimeIndex へのプロパティアクセスを使う。  
# 使えるプロパティはこちら。  
# http://pandas.pydata.org/pandas-docs/version/0.15.1/api.html#time-date-components  
# 
# index が 2015年の日付になっている行のみ抽出するときは、pd.DatetimeIndex.year で 年のみを含む numpy.array を作って論理演算する。

# In[ ]:

df.index.year


# In[ ]:

df.index.year == 2015


# In[ ]:

df[df.index.year == 2015]


# ### 日時-like な文字列を使った \__getitem\__ による選択

# __補足)__ DatetimeIndex (と PeriodIndex という日時関連の別クラス ) を index に持つ Series, DataFrame では、   
# 例外的に \__getitem\__ の引数として日時-like な文字列が使えたる。  
# 詳しくは こちら。  
# http://pandas.pydata.org/pandas-docs/version/0.15.1/timeseries.html#datetimeindex-partial-string-indexing  
# そのため、同じ処理は以下のようにも書ける。

# In[ ]:

df['2015']


# ## 列, 行の値から特定の条件で選択

# __補足)__ DataFrame では 実データは列持ち (各列が特定の型のデータを保持している) なので、  
# ここからの方法では行/列の方向を意識する必要がある。

# ### .dtypes プロパティで特定の型の列のみ取り出す

# DataFrame.dtypes プロパティで各カラムの型が取得できるので、それらに対して論理演算をかける。

# In[ ]:

df.dtypes


# In[ ]:

df.dtypes == np.float64


# In[ ]:

df.ix[:, df.dtypes == np.float64]


# ### 値が特定の条件を満たす行/列を選択したい

# たいていは 普通の演算でいける。  
# "N1" カラムの値が偶数の行だけ抽出するには、

# In[ ]:

df['N1'] % 2 == 0


# In[ ]:

df[df['N1'] % 2 == 0]


# 各列の合計値が 50 を超えるカラムを抽出するには、

# In[ ]:

df.sum()


# In[ ]:

indexer = df.sum() > 50
indexer


# In[ ]:

indexer.index[indexer]


# In[ ]:

df[indexer.index[indexer]]


# 各行 の値に関数適用して選択したいときは apply。  
# apply に渡す関数は 行 もしくは 列を Series として受け取って処理できるものでないとダメ。  
# apply での関数の適用方向は axis オプションで決める。  
#   
# ・axis=0 : 各列への関数適用  
# ・axis=1 : 各行への関数適用  
#   
# "N1" カラムと "N2" カラムの積が 100 を超える行だけをフィルタする場合、

# In[ ]:

df.apply(lambda x: x['N1'] * x['N2'], axis=1)


# In[ ]:

df.apply(lambda x: x['N1'] * x['N2'], axis=1) > 100


# In[ ]:

df[df.apply(lambda x: x['N1'] * x['N2'], axis=1) > 100]


# __補足)__ 上ではあえて apply を使ったが、各列同士は直接 要素の積をとれるため別に apply が必須ではない。

# In[ ]:

df[df['N1'] * df['N2'] > 100] 


# ### リストに含まれる値だけ選択したい

# index の例と同じ。  
# Series も isin メソッドを持っているので、"S1" カラムの値が "A" もしくは "D" の列を選択するときは、

# In[ ]:

df['S1'].isin(['A', 'D'])


# In[ ]:

df[df['S1'].isin(['A', 'D'])]


# ### .sort_values - 値をソートして選択したい

# ソート順序の変更など、オプションの詳細はこちら。  
# http://pandas.pydata.org/pandas-docs/version/0.15.1/generated/pandas.DataFrame.sort.html#pandas.DataFrame.sort  
# 
# "N2" カラムの値が大きいものを 上から順に 3行分 取得するには、ソートして 行番号でスライスすればよい。

# In[ ]:

df.sort_values('N2', ascending=False)[:3]


# ### .dt アクセサ - 特定の年, 月, etc... のデータだけ選択したい

# 日時型のカラムに対しては、dt アクセサを利用して日時型の各プロパティにアクセスできる。  
# 使えるプロパティはこちら。  
# http://pandas.pydata.org/pandas-docs/version/0.15.1/api.html#datetimelike-properties  
# 
# "D1" カラムの日付が 2日, 3日, 5日の行だけ取得したければ、dt アクセサ + isin で、

# In[ ]:

df['D1']


# In[ ]:

df['D1'].dt.day


# In[ ]:

df['D1'].dt.day.isin([2, 3, 5])


# In[ ]:

df[df['D1'].dt.day.isin([2, 3, 5])]


# __補足)__ Python pandas アクセサ / Grouperで少し高度なグルーピング/集計 - StatsFragments  
# http://sinhrks.hatenablog.com/entry/2014/10/30/233606

# ### .str アクセサ - Python組み込みの文字列関数を使ってデータ選択

# object型のカラムに対しては、str アクセサを利用して、Python 組み込みの 文字列関数を実行した結果を Series として取得できる。  
# 使えるメソッドはこちら。  
# https://docs.python.org/3/library/stdtypes.html?highlight=string%20methods#string-methods  
#  
# したがって、文字列関数を実行するだけならわざわざ apply を使わなくても済む。  
#   
# str.islower を使って値が小文字の列を選択してみる。

# In[ ]:

df[df['S1'].str.islower()] 


# ## .drop_duplicates - 値が重複したデータを削除 

# http://pandas.pydata.org/pandas-docs/version/0.19.2/generated/pandas.DataFrame.drop_duplicates.html?highlight=drop_duplicate

# __DataFrame.drop_duplicates(\*args, \**kwargs)__  
#   
# Return DataFrame with duplicate rows removed, optionally only considering certain columns  
#     
# __Parameters:__	 
# 
#     subset : column label or sequence of labels, optional
#         Only consider certain columns for identifying duplicates, by default use all of the columns
# 
#     keep : {‘first’, ‘last’, False}, default ‘first’
#         first : Drop duplicates except for the first occurrence.
#         last : Drop duplicates except for the last occurrence.
#         False : Drop all duplicates.
# 
#     take_last : deprecated
# 
#     inplace : boolean, default False
#         Whether to drop duplicates in place or to return a copy
# 
# __Returns:__	
# 
#     deduplicated : DataFrame

# ## .dropna - 欠損値 ( NaN ) のデータを削除

# http://pandas.pydata.org/pandas-docs/version/0.19.2/generated/pandas.DataFrame.dropna.html?highlight=dropna#pandas.DataFrame.dropna

# __DataFrame.dropna(axis=0, how='any', thresh=None, subset=None, inplace=False)__  
#   
# Return object with labels on given axis omitted where alternately any or all of the data are missing
#   
# __Parameters:__	
# 
#     axis : {0 or ‘index’, 1 or ‘columns’}, or tuple/list thereof
#         Pass tuple or list to drop on multiple axes
# 
#     how : {‘any’, ‘all’}
#             any : if any NA values are present, drop that label
#             all : if all values are NA, drop that label
# 
#     thresh : int, default None
#         int value : require that many non-NA values
# 
#     subset : array-like
#         Labels along other axis to consider, e.g. if you are dropping rows these would be a list of columns to include
# 
#     inplace : boolean, default False
#         If True, do operation inplace and return None.
# 
# __Returns:__	
# 
#     dropped : DataFrame

# ## .fillna - 欠損値 ( NaN ) のデータを埋める

# http://pandas.pydata.org/pandas-docs/version/0.19.2/generated/pandas.DataFrame.fillna.html?highlight=fillna#pandas.DataFrame.fillna

# # その他のデータ選択方法 - StatsFragments

# http://sinhrks.hatenablog.com/entry/2014/11/18/003204

# ## サンプルデータの準備

# In[ ]:

s1 = pd.Series([1, 2, 3], index = ['I1', 'I2', 'I3'])
s1


# In[ ]:

df1 = pd.DataFrame({'C1': [11, 21, 31],
                    'C2': [12, 22, 32],
                    'C3': [13, 23, 33]},
                   index = ['I1', 'I2', 'I3'])
df1


# ## .where - データ選択後、元データと同じ長さの戻り値が得られる

# Series から \__getitem\__ する方法では、条件に該当する要素のみが返ってくる。

# In[ ]:

s1[s1 > 2]


# 元データと同じ長さのデータがほしい場合、  
# __Series.where__ を使うと、条件に該当しないラベルは NaN でパディングし、元データと同じ長さの結果を返してくれる。  
# where の引数はデータと同じ長さの bool 型の numpy.array もしくは Series である必要がある。  
# (Series への論理演算では長さは変わらないので、以下のような使い方なら特別 意識する必要はない)

# In[ ]:

s1.where(s1 > 2)


# NaN 以外でパディングしたい場合、第二引数にパディングに使う値を渡す。

# In[ ]:

# NaN ではなく 0 でパディング
s1.where(s1 > 2, 0)


# 第二引数にはデータと同じ長さの numpy.array や Series も渡せる。  
# この場合、パディングはそれぞれ対応する位置にある値で行われ、第一引数の条件に該当しないデータを第二引数で置換される。  
# つまり if - else のような動作だと考えていい。

# In[ ]:

# 第一引数の条件に該当しない s1 の 1, 2番目の要素が array の 1, 2 番目の要素で置換される
s1.where(s1 > 2, np.array([4, 5, 6]))


# In[ ]:

# 置換用の Series を作る
s2 = pd.Series([4, 5, 6], index = ['I1', 'I2', 'I3'])
s2


# In[ ]:

# 第一引数の条件に該当しない s1 の 1, 2番目の要素が Series s2 の 1, 2 番目の要素で置換される
s1.where(s1 > 2, s2)


# DataFrame でも同様。

# In[ ]:

df1.where(df1 > 22)


# In[ ]:

# 0 でパディング
df1.where(df1 > 22, 0)


# In[ ]:

# 置換用の DataFrame を作る
df2 = pd.DataFrame({'C1': [44, 54, 64],
                    'C2': [45, 55, 65],
                    'C3': [46, 56, 66]},
                   index = ['I1', 'I2', 'I3'])
df2


# In[ ]:

# df1 のうち、22以下の値を df2 の値で置換
df1.where(df1 > 22, df2)


# where がことさら便利なのは以下のようなケース。  
# ・DataFrame に新しいカラムを作りたい。 (あるいは既存の列の値を置き換えたい)  
# ・新しいカラムは、"C2" 列の値が 30を超える場合は "C1" 列の値を使う。  
# ・それ以外は "C3" 列の値を使う。  
# 
# これが一行で書ける。

# In[ ]:

df1['C4'] = df1['C1'].where(df1['C2'] > 30, df1['C3'])
df1


# ## .mask - where の逆。第一引数の条件に該当するセルをマスクする。

# In[ ]:

df1.mask(df1 > 22)


# In[ ]:

df1.mask(df1 > 22, 0)


# ## .query - 複数の条件式の組み合わせによるデータ選択をシンプルな記述で

# \__getitem\__ と同じ操作がよりシンプル な表現で書ける。  
# 
# __注)__ query を使うには numexpr パッケージが必要なので、入っていなければ pip install numexpr

# ### サンプルデータの準備

# In[ ]:

df1 = pd.DataFrame({'C1': [11, 21, 31],
                    'C2': [12, 22, 32],
                    'C3': [13, 23, 33]},
                   index = ['I1', 'I2', 'I3'])
df1


# ### .query の利用  

# \__getitem\__ を利用したデータ選択では、論理演算の組み合わせで bool の Series を作る必要がある。  
# そのため、[] 内で元データへの参照 ( 下の例では df1 )を繰り返し記述しなければならず、  
# 複数条件の組み合わせの際は演算順序の都合上 () を記述したりと式が複雑でわかりにくくなる。

# In[ ]:

df1[df1['C1'] > 20]


# In[ ]:

df1[df1['C2'] < 30]


# In[ ]:

df1[(df1['C1'] > 20) & (df1['C2'] < 30)]


# 同じ処理は __.query__ を使うとすっきり書ける。  
# query の引数にはデータ選択に使う条件式を文字列で渡す。  
# この式が評価される名前空間 (= query 名前空間) の中では、query を呼び出したデータの列名が あたかも変数のように参照できる。

# In[ ]:

df1.query('C1 > 20 & C2 < 30')


# ただし、query 名前空間の中で使える表現は限られるので注意。  
# 例えば以下のような メソッド呼び出しはできない。

# In[ ]:

# NG!
df1.query('C1.isin([11, 21])')
# TypeError: NotImplementedError: 'Call' nodes are not implemented
# TypeError: 'Series' objects are mutable, thus they cannot be hashed


# 同じ処理を行う場合は in 演算子を使う。

# In[ ]:

# in を使えば OK
df1.query('C1 in [11, 21]')


# また、numexpr で利用できる関数も query 名前空間上で呼び出せるようになった模様。

# In[ ]:

df1.query('C1 > sqrt(400)')


# 上記のように query に渡す式表現に論理表現以外を含めることができなかった当時のテクニックとして、  
# 条件によって式表現を変えたい場合などは、  
# 式表現を都度文字列として連結 + 生成するか、  
# ローカル変数に計算結果を入れて式表現に渡す方法もある。
# 
# ローカル変数を式表現中に含める際は、変数名を @ ではじめる。

# In[ ]:

x = 20


# In[ ]:

# NG!
df1.query('C1 > x')
# UndefinedVariableError: name 'x' is not defined


# In[ ]:

# OK!
df1.query('C1 > @x')


# query 名前空間上で index の値を参照する場合は、式表現中で index と指定する。

# In[ ]:

df1.query('index in ["I1", "I2"]')


# index が列名と重複した場合は 列名が優先。

# In[ ]:

df_idx = pd.DataFrame({'index': [1, 2, 3]}, index=[3, 2, 1])
df_idx


# In[ ]:

df_idx.query('index >= 2')


# # My Case Studies

# ## 数値のみであるべき列に文字列が混入している場合の特定と置換

# In[ ]:

# この方法でも可能だが、正しい使用法ではないらしく警告が出る
kessan_table_15[kessan_table_15['希薄化後一株当り純利益'].str.contains('([^0-9.]*)', na=False)]


# In[ ]:

kessan_table_15[kessan_table_15['希薄化後一株当り純利益'].apply(lambda x: type(x) is str)]


# In[ ]:

# __getitem__ による指定を行うと警告がでるので .loc を使う
kessan_table_15.loc[kessan_table_15['希薄化後一株当り純利益'].apply(lambda x: type(x) is str), '希薄化後一株当り純利益'] = np.nan


# ## 文字列で表現された数値(小数点も含む)以外の文字列の要素を取り出し

# object 型のシリーズにしか使えない。  
# object 型のシリーズに含まれる float 型の要素は NaN が返る。

# In[ ]:

df = pd.DataFrame({'C1': [11, 21, 31, 41],
                    'C2': [12.0, 22.0, 32.0, 42],
                    'C3': [np.nan, '-', '33', 44]},
                   index = ['i1', 'i2', 'i3', 'i4'])
df


# In[ ]:

df['C3'].str.extract('([^0-9.]*)', expand=True)


# ## 文字列で表現された数値が小数点を含む場合

# In[ ]:

# '－'  を NaN に置換
# .str を2回も使わないといけないのはなんだか。。。
tables[27].loc[~tables[27]['１株純資産'].str.replace('.', '').str.isnumeric(), '１株純資産'] = np.nan
tables[27].loc[~tables[27]['有利子負債倍率'].str.replace('.', '').str.isnumeric(), '有利子負債倍率'] = np.nan


# ## 複数のデータフレームで構成されるリストから列数が任意の数以下のデータフレームを削除

# In[ ]:

# 列数が 5 以下のテーブルを削除
tables = list(filter(lambda x: len(x.columns) > 5, tables))


# ## csv

# In[ ]:

# 保存
kabupro_kessan.to_csv('/Users/Really/Stockyard/_csv/kabupro_kessan.csv')


# In[ ]:

# 保存したファイルの確認
csv_kessan = pd.read_csv('/Users/Really/Stockyard/_csv/kabupro_kessan.csv', index_col=0)


# In[ ]:

# 'Date'列をインデックスに指定してCSVの読み込み、インデックスをdatetime型に変換
df_quote = pd.read_csv('/Users/Really/Stockyard/_csv/t_1758.csv', index_col='Date')
df_quote.index = pd.to_datetime(df_quote.index)
df_quote


# In[ ]:

# kabutan
pl_csv = pd.read_csv('pl_table.csv', index_col = 0, dtype={'決算期': object}, parse_dates=['発表日'])


# ## NaN の判定

# In[ ]:

# 行単位や列単位ではなく要素 (float) を判定したい場合、isnull() が使えない

# math.isnan() を使う
import math
math.isnan(df.ix[0,0])

# 同じ値を比較 (NaN 同士の比較は False になる仕様を利用)
df.ix[0,0] == df.ix[0,0]


# ## 列単位でのリネーム

# In[ ]:

df_sample.rename(columns={'score1': 'point1'})  #対応関係を辞書型で入れてやる


# ## その他

# In[ ]:

# .astype()の引数は辞書で指定できる。
df.astype({'a': int, 'c': str})


# In[ ]:

# all_stock_tableにあってdomestic_stock_tableにない'code'を持つ行を抽出
all_stock_table[~all_stock_table['code'].isin(domestic_stock_table['code'])]


# In[ ]:

# marketの種別で集計
all_stock_table.groupby('market').count()


# In[ ]:

# 指定した値をNaNに置換、NaNはfloat型
all_stock_table.replace('-', np.NaN)


# In[ ]:

result.columns = ['Date', 'Open', 'High', 'Low', 'Close', 'Volume', 'AdjClose'] # 列名を変更


# In[ ]:

# 列単位で個別に名称を変更する場合
all_stock_table = all_stock_table.rename(columns={'市場・商品区分': 'market'})


# In[ ]:

# marketの値を指定して選択
len(all_stock_table.query("market == '市場第一部（内国株）' | market == '市場第二部（内国株）'"))


# In[ ]:

df_quote.dtypes


# In[ ]:

df_quote.index


# In[ ]:

# datetime型インデックスの作成例
dtidx = pd.date_range('2000-01-01', '2017-12-01', freq='B') # freq='B'はBusiness Day
dtidx


# In[ ]:

# 他のdatetime型インデックスにデータをあてはめる
quote_bd = pd.DataFrame(df_quote, index=pd.date_range(df_quote.index[0], df_quote.index[-1], freq='B'))
quote_bd


# In[ ]:

df['発表日'].apply(lambda x: parse(x, yearfirst=True).date())


# In[ ]:

df['決算期'].apply(lambda x: datetime.strptime(x, '%Y.%m').date())


# In[ ]:

tables[11].isnull()


# In[ ]:

tables[11].isnull().any()


# In[ ]:

# null が存在する行を取り除いて価格データとする 参考 https://qiita.com/u1and0/items/fd2780813b690a40c197
result = tmp_price[~tmp_price.isnull().any(axis=1)].astype(float).astype(int) # この場合、"~"は "== False" とするのと同じこと


# In[ ]:

# 価格と価格以外の情報を分離
tmp_info = tmp_price[tmp_price.isnull().any(axis=1)].reset_index()


# In[ ]:

# 全ての列項目がnullの行を排除
tables[11][~tables[11].isnull().all(axis=1)]


# In[ ]:

# リスト内包表記。pandasではなくリストの話
csv_table = [i for i in csv_table if re.search(r't_\d*.csv', x)]


# In[ ]:

# str アクセサ
[tables[11]['決算期'].str.contains('前期比')]


# In[ ]:

# 複数条件による判定
# https://qiita.com/HirofumiYashima/items/fa76119d29bcb6e0bae7
# この場合、"|"ではなく"or"を使うとなぜかうまくいかない
tables[11][~((tables[11]['決算期'].str.contains('予')) | (tables[11]['決算期'].str.contains('前期比')))]


# In[ ]:

# 行の要素を分割、元の列と新しく追加する列にそれぞれ代入
df[['会計基準', '決算期']] = pd.DataFrame(list(df['決算期'].str.split(' ')))


# In[ ]:

# インデックスを振り直す
tables[11].reset_index(drop=True)


# In[ ]:

# 列の並べ替えは列名のリストで
df = df[['会計基準', '決算期', '売上高', '営業益', '経常益', '最終益', '１株益', '１株配', '発表日']]


# In[ ]:

# 列ごとに関数適用
# applyは遅いという話もあるので要検討。 http://sinhrks.hatenablog.com/entry/2015/07/11/223124
df[['売上高', '営業益', '経常益', '最終益']] = df[['売上高', '営業益', '経常益', '最終益']].apply(lambda x: x * 1000000)


# In[ ]:

df.duplicated() # booleanのシリーズ
df.duplicated().any() # Trueが含まれるかどうか
kessan_table[kessan_table.duplicated(keep=False)] # 重複行をそれぞれ見たい場合
# keep : {‘first’, ‘last’, False}, default ‘first’


# In[ ]:

df.drop_duplicates() # 行全体で重複をチェック


# In[ ]:

df.drop_duplicates(['x', 'y']) # 列指定で重複をチェック、前方残し


# In[ ]:

df.drop_duplicates(['x', 'y'], keep='last') # 後方残し


# # MySQLに接続

# In[ ]:

sql = stock.sql()


# In[ ]:

help(sql)


# # MySQLに接続 (クラス不使用)

# In[ ]:

db_settings = {
    "host": 'localhost',
    "database": 'StockPrice_Yahoo_1',
    "user": 'user',
    "password": 'password',
    "port":'3306'
}
engine = create_engine('mysql://{user}:{password}@{host}:{port}/{database}'.format(**db_settings))


# # - Memo - MySQL クエリ

# In[ ]:

# mysql>
CREATE DATABASE StockPrice_Yahoo_1 DEFAULT CHARACTER SET utf8mb4;
GRANT ALL ON StockPrice_Yahoo_1.* TO user@localhost IDENTIFIED BY 'password';
show databases;
use StockPrice_Yahoo_1;
show tables;
drop tables
drop database
select*from
show columns from # テーブルの中に含まれるカラムの情報を取得する
select*from t_1382 where Date between '2013-12-01' and '2013-12-31';
select * from t_8848 order by Date desc limit 5; # 最後の5行

