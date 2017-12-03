
# coding: utf-8

# # memo
※単位：売上高、営業益、経常益、最終益は「百万円」。１株益、１株配は「円」。率は「％」

※１株純資産は「円」。自己資本比率は「％」。総資産、自己資本、剰余金は「百万円」。有利子負債倍率は「倍」

・「連」：日本会計基準［連結決算］、「単」：日本会計基準［非連結決算(単独決算)］、「U」：米国会計基準、「I」：国際会計基準(IFRS)、
「予」：予想業績、「旧」：修正前の予想業績、「新」：修正後の予想業績、「実」：実績業績、「変」：決算期変更

・［連結／非連結］決算区分の変更があった場合は、連続的に業績推移を追えるように、連結と非連結を混在して表示しています。
連結と非連結が混在しない場合は、「連」「単」表記は省略します。

・前期比および前年同期比は、会計基準や決算期間が異なる場合は比較できないため、「－」で表記しています。

・米国会計基準と国際会計基準では、「経常益」欄の数値は「税引き前利益」を表記しています。

・業績予想がレンジで開示された場合は中央値を表記しています。25935 伊藤園第1種優先株式 ない
# # import など準備

# ## import, MySQL 接続

# In[ ]:

# import
import sys
import os
import re
import datetime as dt
import time
import importlib
import logging
import numpy as np
import pandas as pd
from robobrowser import RoboBrowser
# from robobrowser.browser import RoboState
from retry import retry
from dateutil.parser import parse
from datetime import datetime

import stock


# In[ ]:

importlib.reload(stock)


# In[ ]:

# pandas の最大表示列数を設定 (max_rows で表示行数の設定も可能)
pd.set_option('display.max_columns', 30)


# In[ ]:

sql = stock.sql()


# In[ ]:

help(stock.sql)


# ## 比較参照用、株プロ決算の読み込み

# In[ ]:

table_name = 'kabupro_kessan'


# In[ ]:

kabupro = sql.read_table(table_name)


# In[ ]:

kabupro


# In[ ]:

kabupro.columns


# In[ ]:

kabupro.ix[14]
# 株プロにしか無い項目: 希薄化後一株当り純利益, 純資産又は株主資本, 営業キャッシュフロー, 投資キャッシュフロー, 財務キャッシュフロー


# # 銘柄コードリスト

# In[ ]:

domestic_stock_table = sql.read_table('domestic_stock_table')


# In[ ]:

domestic_stock_table


# In[ ]:

code_list = list(domestic_stock_table['code'])


# In[ ]:

# 伊藤園第1種優先株式を削除
code_list.remove(25935)


# In[ ]:

code_list[-10:]


# In[ ]:

len(code_list)


# In[ ]:

start_index = 30
increase_number = 10
# end_index = start_index + increase_number
end_index = len(code_list)

reading_code = code_list[start_index : end_index]
print(reading_code[-10:])
print('Next start from {0}'.format(start_index + increase_number))


# # 保存した html ファイルからテーブル属性のみ読み込み、整形

# __TODO__
通期業績以降のテーブルを発表日でくっつけてみる
# In[ ]:

code = 9432


# In[ ]:

# 保存した html からテーブル属性を読み込み
tables = pd.read_html('/Users/Really/Stockyard/_kabutan_html/kabutan_{0}.html'.format(code), header=0)


# ## リスト、テーブルの概要

# In[ ]:

len(tables)


# In[ ]:

len(tables[12])


# In[ ]:

len(tables[12].columns)


# ## 整形処理

# In[ ]:

# tables[11] 通期業績

# 全ての列項目がnullの行を除去
tables[11] = tables[11][~tables[11].isnull().all(axis=1)].reset_index(drop=True)

# 予想値と前期比の行を除去
tables[11] = tables[11][~((tables[11]['決算期'].str.contains('予')) | (tables[11]['決算期'].str.contains('前期比')))].reset_index(drop=True)

# 決算期列の要素を会計基準と決算期に分割、それぞれの列に代入(同時に会計基準列を新規作成)
tables[11][['会計基準', '決算期']] = pd.DataFrame(list(tables[11]['決算期'].str.split(' ')))

# 列の並び替え
tables[11] = tables[11][['会計基準', '決算期', '売上高', '営業益', '経常益', '最終益', '１株益', '１株配', '発表日']]

# 100万円単位換算
tables[11][['売上高', '営業益', '経常益', '最終益']] = tables[11][['売上高', '営業益', '経常益', '最終益']].apply(lambda x: x * 1000000)

# 型変換
tables[11]['１株配'] = tables[11]['１株配'].astype(float)

# 日付のパース、datetime.dateへの型変換
# tables[11]['決算期'] = tables[11]['決算期'].apply(lambda x: datetime.strptime(x, '%Y.%m').date()) # 日付ではないので文字列のままの方がいいかも？
tables[11]['発表日'] = tables[11]['発表日'].apply(lambda x: parse(x, yearfirst=True).date())
# pandasのTimestampへの型変換
tables[11]['発表日'] = pd.to_datetime(tables[11]['発表日'], format='%Y-%m-%d')
# tables[11]['決算期'] = pd.to_datetime(tables[11]['決算期'], format='%Y-%m-%d')

# tables[12] 業績予想

tables[12].columns = ['会計基準', '決算期', '発表日', 
                                   '結合修正方向', '売上高修正方向', '営業益修正方向', '経常益修正方向', '最終益修正方向', '修正配当修正方向', 
                                   '予想売上高', '予想営業益', '予想経常益', '予想最終益', '予想修正配当',]

# 不要行、不要列の削除、並び替え
# 実績(と修正配当)はいる?いらない?
# 実績の発表と同時に次の予想が出ているのでやっぱりここではいらないのかな?
tables[12] = tables[12].ix[tables[12].index % 2 == 0, ['会計基準', '決算期', '予想売上高', '予想営業益', '予想経常益', '予想最終益', '発表日']].reset_index(drop=True)
tables[12] = tables[12].ix[tables[12]['決算期'] != '実績']

# 決算期の NaN 埋め
tables[12]['決算期'] = tables[12]['決算期'].fillna(method='ffill')

# 100万円単位換算
tables[12][['予想売上高', '予想営業益', '予想経常益', '予想最終益']] = tables[12][['予想売上高', '予想営業益', '予想経常益', '予想最終益']].apply(lambda x: x * 1000000)

# 日付のパース、datetime.dateへの型変換
# tables[12]['決算期'] = tables[12]['決算期'].apply(lambda x: datetime.strptime(x, '%Y.%m').date()) # 日付ではないので文字列のままの方がいいかも？
tables[12]['発表日'] = tables[12]['発表日'].apply(lambda x: parse(x, yearfirst=True).date())
# pandasのTimestampへの型変換
tables[12]['発表日'] = pd.to_datetime(tables[12]['発表日'], format='%Y-%m-%d')
# tables[12]['決算期'] = pd.to_datetime(tables[12]['決算期'], format='%Y-%m-%d')

# 修正配当用の処理なので不要
# '－'  を NaN に置換
# tables[12].loc[~tables[12]['修正配当'].str.isnumeric(), '修正配当'] = np.nan
# 型変換
# tables[12]['修正配当'] = tables[12]['修正配当'].astype(float)

# tables[26] ３ヵ月業績の推移【実績】(過去5年 + 前年同期比) 累積ではなく差分

'''
不要かな？
ちょっと株プロと見比べてみよう → １株益の値が揃わない
修正発表があった項目は上書きされてしまっていると思われる
修正の可能性を考えなければ累積の株プロよりこちらの方が使いやすいかも
株プロで差分を作成するべきか?
前年同期比はいらなそう
'''

# 全ての列項目がnullの行を除去
tables[26] = tables[26][~tables[26].isnull().all(axis=1)].reset_index(drop=True)

# 前年同期比の行を除去
tables[26] = tables[26][~tables[26]['決算期'].str.contains('前年同期比')].reset_index(drop=True)

# 決算期列の要素を会計基準と決算期に分割、それぞれの列に代入(同時に会計基準列を新規作成)
tables[26][['会計基準', '四半期期首']] = pd.DataFrame(list(tables[26]['決算期'].str.split(' ')))

# 列の並び替え
tables[26] = tables[26][['会計基準', '四半期期首', '売上高', '営業益', '経常益', '最終益', '１株益', '売上営業損益率', '発表日']]

tables[26].columns = ['会計基準', '四半期期首', '四半期売上高', '四半期営業益', '四半期経常益', '四半期最終益', '四半期１株益', '四半期売上営業損益率', '発表日']

# 100万円単位換算
tables[26][['四半期売上高', '四半期営業益', '四半期経常益', '四半期最終益']] = tables[26][['四半期売上高', '四半期営業益', '四半期経常益', '四半期最終益']].apply(lambda x: x * 1000000)

# 日付のパース、datetime.dateへの型変換
tables[26]['四半期期首'] = tables[26]['四半期期首'].apply(lambda x: parse(x.replace('-', '.'), yearfirst=True).date())
tables[26]['発表日'] = tables[26]['発表日'].apply(lambda x: parse(x, yearfirst=True).date())
# pandasのTimestampへの型変換
tables[26]['四半期期首'] = pd.to_datetime(tables[26]['四半期期首'], format='%Y-%m-%d')
tables[26]['発表日'] = pd.to_datetime(tables[26]['発表日'], format='%Y-%m-%d')

# tables[27] 財務 【実績】

'''
期間は株プロよりこちらの方が長い
修正発表があった項目は上書きされてしまっていると思われる
'''

# 7203 トヨタ 元の値が '90/08/01' で明らかにおかしい。これどうしよう。。。
# tables[27].ix[2, '発表日'] = '99/08/01'

# 全ての列項目がnullの行を除去
tables[27] = tables[27][~tables[27].isnull().all(axis=1)].reset_index(drop=True)

# 決算期列の要素を会計基準と決算期に分割、それぞれの列に代入(同時に会計基準列を新規作成)
tables[27][['会計基準', '決算期']] = pd.DataFrame(list(tables[27]['決算期'].str.split(' ')))

# 列の並び替え
tables[27] = tables[27][['会計基準', '決算期', '１株純資産', '自己資本比率', '総資産', '自己資本', '剰余金', '有利子負債倍率', '発表日']]

# 決算期が 'yyyy.mm' 表記ではない行は確定決算前と判断して削除
tables[27] = tables[27][tables[27]['決算期'].str.contains('\d\d\d\d.\d\d')]

# 決算期が 1999.03 のデータは他のテーブルには無く、発表日も不自然なので行ごと削除
tables[27] = tables[27][~tables[27]['決算期'].str.contains('1998.03')]

# '－'  を NaN に置換
# .str を2回も使わないといけないのはなんだか。。。
tables[27].loc[~tables[27]['１株純資産'].str.replace('.', '').str.isnumeric(), '１株純資産'] = np.nan
tables[27].loc[~tables[27]['有利子負債倍率'].str.replace('.', '').str.isnumeric(), '有利子負債倍率'] = np.nan

# 型変換
tables[27][['１株純資産', '有利子負債倍率']] = tables[27][['１株純資産', '有利子負債倍率']].astype(float)

# 100万円単位換算
tables[27][['総資産', '自己資本', '剰余金']] = tables[27][['総資産', '自己資本', '剰余金']].apply(lambda x: x * 1000000)

# 日付のパース、datetime.dateへの型変換
# tables[27]['決算期'] = tables[27]['決算期'].apply(lambda x: parse(x.replace('-', '.'), yearfirst=True).date()) # 日付ではないので文字列のままの方がいいかも？
tables[27]['発表日'] = tables[27]['発表日'].apply(lambda x: parse(x, yearfirst=True).date())
# pandasのTimestampへの型変換
# tables[27]['決算期'] = pd.to_datetime(tables[27]['決算期'], format='%Y-%m-%d')
tables[27]['発表日'] = pd.to_datetime(tables[27]['発表日'], format='%Y-%m-%d')


# # 内容の確認

# In[ ]:

# tables[11] 通期業績
tables[11]
# 株プロに無い項目: １株配


# In[ ]:

# tables[12] 業績予想
tables[12]


# In[ ]:

# tables[26] ３ヵ月業績の推移
tables[26]
# 株プロに無い項目: 売上営業損益率 = 営業益 / 売上高?


# In[ ]:

# tables[27] 財務
tables[27]
# 株プロに無い項目: 自己資本比率, 自己資本, 剰余金, 有利子負債倍率


# In[ ]:

tables[11].columns


# In[ ]:

tables[12].columns


# In[ ]:

tables[26].columns


# In[ ]:

tables[27].columns


# In[ ]:

tables[11].dtypes


# In[ ]:

tables[12].dtypes


# In[ ]:

tables[26].dtypes


# In[ ]:

tables[27].dtypes


# In[ ]:

len(tables[11])


# In[ ]:

len(tables[12])


# In[ ]:

len(tables[26])


# In[ ]:

len(tables[27])


# # 結合

# In[ ]:

# 通期業績 & 財務
merged_1 = pd.merge(tables[11], tables[27], on=['会計基準', '決算期', '発表日'], how='outer').sort_values('発表日')


# In[ ]:

merged_1.columns


# In[ ]:

merged_1[['会計基準', '発表日', '決算期', '売上高', '営業益', '経常益', '最終益', '１株益', '１株配', '１株純資産',
       '自己資本比率', '総資産', '自己資本', '剰余金', '有利子負債倍率']]


# In[ ]:

# 通期業績 & 業績予想
merged_2 = pd.merge(tables[11], tables[12], on=['会計基準', '決算期', '発表日'], how='outer').sort_values(['決算期', '発表日'])


# In[ ]:

merged_2.columns


# In[ ]:

merged_2[['会計基準', '発表日', '決算期', '売上高', '予想売上高', '営業益', '予想営業益', '経常益', '予想経常益', '最終益', '予想最終益', '１株益', '１株配']]


# In[ ]:

# 通期業績 & 四半期業績
merged_3 = pd.merge(tables[11], tables[26], on=['会計基準', '発表日'], how='outer').sort_values(['発表日'])


# In[ ]:

merged_3.columns


# In[ ]:

merged_3[['会計基準', '発表日', '決算期', '四半期期首', '売上高', '四半期売上高', '営業益', '四半期営業益', '経常益', '四半期経常益', 
          '最終益', '四半期最終益', '１株益', '四半期１株益', '１株配', '四半期売上営業損益率']].reset_index(drop=True)


# In[ ]:

# すべて 通期業績 + 業績予想
merged_all = pd.merge(tables[11], tables[12], on=['会計基準', '決算期', '発表日'], how='outer').sort_values(['発表日', '決算期'])


# In[ ]:

merged_all.reset_index(drop=True)


# In[ ]:

# すべて + 四半期業績
merged_all = pd.merge(merged_all, tables[26], on=['会計基準', '発表日'], how='outer').sort_values(['発表日', '決算期'])


# In[ ]:

# すべて + 財務
merged_all = pd.merge(merged_all, tables[27], on=['会計基準', '決算期', '発表日'], how='outer').sort_values(['発表日', '決算期'])


# In[ ]:

whos


# In[ ]:

# すべて 通期業績 + 財務
merged_all = pd.merge(tables[11], tables[27], on=['会計基準', '決算期', '発表日'], how='outer').sort_values(['発表日', '決算期'])


# In[ ]:

merged_all.reset_index(drop=True)


# In[ ]:

# すべて + 四半期業績
merged_all = pd.merge(merged_all, tables[26], on=['会計基準', '発表日'], how='outer').sort_values(['発表日', '決算期'])


# In[ ]:

# すべて + 財務
merged_all = pd.merge(merged_all, tables[27], on=['会計基準', '決算期', '発表日'], how='outer').sort_values(['発表日', '決算期'])


# In[ ]:

merged_all = merged_all.reset_index(drop=True)


# In[ ]:

merged_all.columns


# In[ ]:

merged_all = merged_all[['発表日', '決算期', '四半期期首', '会計基準', '売上高', '営業益', '経常益', '最終益', '１株益', '１株配', '予想売上高',
       '予想営業益', '予想経常益', '予想最終益', '四半期売上高', '四半期営業益', '四半期経常益',
       '四半期最終益', '四半期１株益', '四半期売上営業損益率', '１株純資産', '自己資本比率', '総資産', '自己資本',
       '剰余金', '有利子負債倍率']]


# In[ ]:

merged_all


# # 作成したテーブルの保存

# In[ ]:

table_name = kt_7203


# In[ ]:

write_table(table_name, merged_all)


# # 不要テーブル

# ## tables[7] 銘柄概要

# In[ ]:

tables[7]


# ##  tables[9] 銘柄概要

# In[ ]:

tables[9]


# ## tables [10] ＰＥＲ ＰＢＲ 利回り 信用倍率 (データ取得日時点?)

# In[ ]:

tables[10]


# ## tables[23] 過去最高 【実績】
不要かな？
百万単位
# In[ ]:

tables[23]


# ## tables[24] 下期業績 (過去3年 + 今年予想 + 前年同期比)
不要かな？
ちょっと株プロと見比べてみたいけどめんどくさい
百万単位
# In[ ]:

tables[24]


# In[ ]:

tables[24].columns


# ## tables[25] 第２四半期累計決算【実績】 (過去3年 + 前年同期比)
不要かな？
ちょっと株プロと見比べてみよう → 同じみたい
対通期進捗率って経常益の最終的な通期実績に対して? 今年度分は予想に対して？
百万単位
# In[ ]:

tables[25]


# In[ ]:

kabupro.ix[(kabupro['証券コード'] == code) & (kabupro['会計基準'] == '米国基準') & (kabupro['決算期間'] == '第2四半期'), 
           ['連結個別', '期首', '売上高', '営業利益', '経常利益', '純利益', '一株当り純利益', '情報公開日 (更新日)']].tail(3)


# In[ ]:

kabupro.ix[(kabupro['証券コード'] == code)& (kabupro['会計基準'] == '米国基準') & (kabupro['決算期間'].isin(['第2四半期', '通期'])), # 
           ['連結個別', '期首', '決算期間', '売上高', '営業利益', '経常利益', '純利益', '一株当り純利益', '情報公開日 (更新日)']].tail(5)


# In[ ]:

tables[12].tail(1)


# In[ ]:

# 比較参照用
kabupro.columns


# ## おしまい

# In[ ]:

tables[29]

