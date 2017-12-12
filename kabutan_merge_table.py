
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

# In[ ]:

code = 7203


# In[ ]:

# 保存した html からテーブル属性を読み込み
tables = pd.read_html('/Users/Really/Stockyard/_kabutan_html/kabutan_{0}.html'.format(code), header=0)

# 列数が 5 以下のテーブルを削除
tables = list(filter(lambda x: len(x.columns) > 5, tables))


# In[ ]:

# 抽出用テーブルの作成
pl_table = pd.DataFrame()
fc_table = pd.DataFrame()
qr_table = pd.DataFrame()
bs_table = pd.DataFrame()

# 必要なテーブルの抽出
# リストを要素ごとに for で回す書き方
for table in tables:
    # 通期業績: profit and loss statement
    if len(table.columns) == 8: 
        if (table.columns[-2] == "１株配") & (pl_table.shape[1] == 0): 
            pl_table = table.copy()
    # 業績予想: forecast
    if len(table.columns) >= 8: 
        if (table.columns[1] == "修正日") & (fc_table.shape[1] == 0): 
            fc_table = table.copy()
    # 3ヶ月業績: quater
    if len(table.columns) == 8: 
        if (table.columns[-2] == "売上営業損益率") & (qr_table.shape[1] == 0): 
            qr_table = table.copy()
    # 財務: balance sheet
    if len(table.columns) == 8: 
        if (table.columns[1] == "１株純資産") & (bs_table.shape[1] == 0): 
            bs_table = table.copy()


# ## 整形処理

# In[ ]:

## pl_table (tables[3]) 通期業績

# 株プロに無い項目: １株配

# 全ての列項目がnullの行を除去
pl_table = pl_table[~pl_table.isnull().all(axis=1)].reset_index(drop=True)

# 予想値と前期比の行を除去
pl_table = pl_table[~((pl_table['決算期'].str.contains('予')) | (pl_table['決算期'].str.contains('前期比')))].reset_index(drop=True)

# 決算期列の要素を会計基準と決算期に分割、それぞれの列に代入(同時に会計基準列を新規作成)
if not pl_table['決算期'].str.contains(' ').all():
    pl_table['会計基準'] = list('J' * len(pl_table))
else:
    pl_table[['会計基準', '決算期']] = pd.DataFrame(list(pl_table['決算期'].str.split(' ')))

# 列の並び替え
pl_table = pl_table[['会計基準', '決算期', '売上高', '営業益', '経常益', '最終益', '１株益', '１株配', '発表日']]

# 日付のパース、datetime.dateへの型変換、最終的に '－'  は NaT に置換される
# pl_table['決算期'] = pl_table['決算期'].apply(lambda x: datetime.strptime(x, '%Y.%m').date()) # 日付ではないので文字列のままの方がいいかも？
pl_table['発表日'] = pl_table.loc[pl_table['発表日'].str.match('\d\d/\d\d/\d\d'), '発表日'].apply(lambda x: parse(x, yearfirst=True).date())
# pandasのTimestampへの型変換
pl_table['発表日'] = pd.to_datetime(pl_table['発表日'], format='%Y-%m-%d')
# pl_table['決算期'] = pd.to_datetime(pl_table['決算期'], format='%Y-%m-%d')

# 数値の列の数値以外の文字列 ('－' 等) を NaN に置換
num_col = ('売上高', '営業益', '経常益', '最終益', '１株益', '１株配')
for key in num_col:
    if pl_table[key].dtypes == object:
        pl_table.loc[~pl_table[key].str.replace('.', '').str.isnumeric(), key] = np.nan # .str を2回も使わないといけないのはなんだか。。。
        # pl_table.loc[pl_table[key].str.contains('－'), key] = np.nan # この書き方だと '－'  以外の文字列に対応できないので不安

# 型変換
# 辞書内包表記による一括変換
pl_table = pl_table.astype({x: float for x in ('売上高', '営業益', '経常益', '最終益', '１株益', '１株配')})

# 100万円単位換算
million_col = ('売上高', '営業益', '経常益', '最終益')
pl_table.loc[:, million_col] = pl_table.loc[:, million_col].apply(lambda x: x * 1000000)

## fc_table (tables[4]) 業績予想

fc_table.columns = ['会計基準', '決算期', '発表日', 
                                   '結合修正方向', '売上高修正方向', '営業益修正方向', '経常益修正方向', '最終益修正方向', '修正配当修正方向', 
                                   '予想売上高', '予想営業益', '予想経常益', '予想最終益', '予想修正配当',]

# 不要行、不要列の削除、並び替え
# 実績(と修正配当)はいる?いらない?
# 実績の発表と同時に次の予想が出ているのでやっぱりここではいらないのかな?
fc_table = fc_table.ix[fc_table.index % 2 == 0, ['会計基準', '決算期', '予想売上高', '予想営業益', '予想経常益', '予想最終益', '発表日']].reset_index(drop=True)
fc_table = fc_table.ix[fc_table['決算期'] != '実績']

# 会計基準の NaN 埋め
# 同じ値を比較 (NaN 同士の比較は False になる仕様を利用)
if fc_table.loc[0, '会計基準'] != fc_table.loc[0, '会計基準']:
    fc_table.loc[0, '会計基準'] = 'J'
fc_table['会計基準'] = fc_table['会計基準'].fillna(method='ffill')

# 決算期の NaN 埋め
fc_table['決算期'] = fc_table['決算期'].fillna(method='ffill')

# 日付のパース、datetime.dateへの型変換、最終的に '－'  は NaT に置換される
# fc_table['決算期'] = fc_table['決算期'].apply(lambda x: datetime.strptime(x, '%Y.%m').date()) # 日付ではないので文字列のままの方がいいかも？
fc_table['発表日'] = fc_table.loc[fc_table['発表日'].str.match('\d\d/\d\d/\d\d'), '発表日'].apply(lambda x: parse(x, yearfirst=True).date())
# pandasのTimestampへの型変換
fc_table['発表日'] = pd.to_datetime(fc_table['発表日'], format='%Y-%m-%d')
# fc_table['決算期'] = pd.to_datetime(fc_table['決算期'], format='%Y-%m-%d')

# 数値の列の数値以外の文字列 ('－' 等) を NaN に置換
num_col = ('予想売上高', '予想営業益', '予想経常益', '予想最終益')
for key in num_col:
    if fc_table[key].dtypes == object:
        fc_table.loc[~fc_table[key].str.replace('.', '').str.isnumeric(), key] = np.nan # .str を2回も使わないといけないのはなんだか。。。
        # fc_table.loc[fc_table[key].str.contains('－'), key] = np.nan # この書き方だと '－'  以外の文字列に対応できないので不安

# 型変換
# 辞書内包表記による一括変換
fc_table = fc_table.astype({x: float for x in ('予想売上高', '予想営業益', '予想経常益', '予想最終益')})

# 100万円単位換算
million_col = ('予想売上高', '予想営業益', '予想経常益', '予想最終益')
fc_table.loc[:, million_col] = fc_table.loc[:, million_col].apply(lambda x: x * 1000000)

# 修正配当用の処理なので不要
# '－'  を NaN に置換
# fc_table.loc[~fc_table['修正配当'].str.isnumeric(), '修正配当'] = np.nan
# 型変換
# fc_table['修正配当'] = fc_table['修正配当'].astype(float)

## qr_table (tables[8?]) ３ヵ月業績の推移【実績】(過去5年 + 前年同期比) 累積ではなく差分

# 株プロに無い項目: 売上営業損益率 = 営業益 / 売上高?
# 不要かな？
# ちょっと株プロと見比べてみよう → １株益の値が揃わない
# 修正発表があった項目は上書きされてしまっていると思われる
# 修正の可能性を考えなければ累積の株プロよりこちらの方が使いやすいかも
# 株プロで差分を作成するべきか?
# 前年同期比はいらなそう

# 全ての列項目がnullの行を除去
qr_table = qr_table[~qr_table.isnull().all(axis=1)].reset_index(drop=True)

# 前年同期比の行を除去
qr_table = qr_table[~qr_table['決算期'].str.contains('前年同期比')].reset_index(drop=True)

# 決算期列の要素を会計基準と決算期に分割、それぞれの列に代入(同時に会計基準列を新規作成)
if not qr_table['決算期'].str.contains(' ').all():
    qr_table['会計基準'] = list('J' * len(qr_table))
    qr_table = qr_table.rename(columns={'決算期': 'Q期首'})
else:
    qr_table[['会計基準', 'Q期首']] = pd.DataFrame(list(qr_table['決算期'].str.split(' ')))

# 列の並び替え
qr_table = qr_table[['会計基準', 'Q期首', '売上高', '営業益', '経常益', '最終益', '１株益', '売上営業損益率', '発表日']]

# 列名の変更
qr_table.columns = ['会計基準', 'Q期首', 'Q売上高', 'Q営業益', 'Q経常益', 'Q最終益', 'Q１株益', 'Q売上営業損益率', '発表日']

# 日付のパース、datetime.dateへの型変換、最終的に '－'  は NaT に置換される
qr_table['Q期首'] = qr_table.loc[qr_table['Q期首'].str.match('\d\d.\d\d-\d\d'), 'Q期首'].apply(lambda x: 
                                                                                               parse(x.replace('-', '.'), yearfirst=True).date())
qr_table['発表日'] = qr_table.loc[qr_table['発表日'].str.match('\d\d/\d\d/\d\d'), '発表日'].apply(lambda x: parse(x, yearfirst=True).date())
# pandasのTimestampへの型変換
qr_table['Q期首'] = pd.to_datetime(qr_table['Q期首'], format='%Y-%m-%d')
qr_table['発表日'] = pd.to_datetime(qr_table['発表日'], format='%Y-%m-%d')

# 数値の列の数値以外の文字列 ('－' 等) を NaN に置換
num_col = ('Q売上高', 'Q営業益', 'Q経常益', 'Q最終益', 'Q１株益', 'Q売上営業損益率')
for key in num_col:
    if qr_table[key].dtypes == object:
        qr_table.loc[~qr_table[key].str.replace('.', '').str.isnumeric(), key] = np.nan # .str を2回も使わないといけないのはなんだか。。。
        # qr_table.loc[qr_table[key].str.contains('－'), key] = np.nan # この書き方だと '－'  以外の文字列に対応できないので不安

# 型変換
# 辞書内包表記による一括変換
qr_table = qr_table.astype({x: float for x in ('Q売上高', 'Q営業益', 'Q経常益', 'Q最終益', 'Q１株益', 'Q売上営業損益率')})

# 100万円単位換算
million_col = ('Q売上高', 'Q営業益', 'Q経常益', 'Q最終益')
qr_table.loc[:, million_col] = qr_table.loc[:, million_col].apply(lambda x: x * 1000000)

## bs_table (tables[9?]) 財務 【実績】

# 株プロに無い項目: 自己資本比率, 自己資本, 剰余金, 有利子負債倍率

# 2000年以前の財務実績の発表日は全体的に信用できない
# 1998年は何らかの日付で固定 or 捨て、1999年と2000年は通期業績の発表日と同じにする
# 期間は株プロよりこちらの方が長い
# 修正発表があった項目は上書きされてしまっていると思われる

# 全ての列項目がnullの行を除去
bs_table = bs_table[~bs_table.isnull().all(axis=1)].reset_index(drop=True)

# 決算期列の要素を会計基準と決算期に分割、それぞれの列に代入(同時に会計基準列を新規作成)
if not bs_table['決算期'].str.contains(' ').all():
    bs_table['会計基準'] = list('J' * len(bs_table))
else:
    bs_table[['会計基準', '決算期']] = pd.DataFrame(list(bs_table['決算期'].str.split(' ')))

# 列の並び替え
bs_table = bs_table[['会計基準', '決算期', '１株純資産', '自己資本比率', '総資産', '自己資本', '剰余金', '有利子負債倍率', '発表日']]

# 決算期が 'yyyy.mm' 表記ではない行は確定決算前と思われるので削除
bs_table = bs_table[bs_table['決算期'].str.contains('\d\d\d\d.\d\d')].reset_index(drop=True)

# 決算期が 1998.mm のデータは他のテーブルには無く、発表日も不自然なので行ごと削除
bs_table = bs_table[~bs_table['決算期'].str.contains('1998.\d\d')].reset_index(drop=True)

# 日付のパース、datetime.dateへの型変換、最終的に '－'  は NaT に置換される
# bs_table['決算期'] = bs_table['決算期'].apply(lambda x: datetime.strptime(x, '%Y.%m').date()) # 日付ではないので文字列のままの方がいいかも？
bs_table['発表日'] = bs_table.loc[bs_table['発表日'].str.match('\d\d/\d\d/\d\d'), '発表日'].apply(lambda x: parse(x, yearfirst=True).date())
# pandasのTimestampへの型変換
bs_table['発表日'] = pd.to_datetime(bs_table['発表日'], format='%Y-%m-%d')
# bs_table['決算期'] = pd.to_datetime(bs_table['決算期'], format='%Y-%m-%d')

# 発表日の欠損値を通期業績の発表日に置換
for idx, date in bs_table['発表日'].iteritems():
    if date != date:
        bs_table.loc[idx, '発表日'] = pl_table.loc[pl_table['決算期'] == bs_table.loc[idx, '決算期'], '発表日'].values[0]

# 数値の列の数値以外の文字列 ('－' 等) を NaN に置換
num_col = ('１株純資産', '自己資本比率', '総資産', '自己資本', '剰余金', '有利子負債倍率')
for key in num_col:
    if bs_table[key].dtypes == object:
        bs_table.loc[~bs_table[key].str.replace('.', '').str.isnumeric(), key] = np.nan # .str を2回も使わないといけないのはなんだか。。。
        # bs_table.loc[bs_table[key].str.contains('－'), key] = np.nan # この書き方だと '－'  以外の文字列に対応できないので不安

# 型変換
# 辞書内包表記による一括変換
bs_table = bs_table.astype({x: float for x in ('１株純資産', '自己資本比率', '総資産', '自己資本', '剰余金', '有利子負債倍率')})

# 100万円単位換算
million_col = ('総資産', '自己資本', '剰余金')
bs_table.loc[:, million_col] = bs_table.loc[:, million_col].apply(lambda x: x * 1000000)


# # 内容の確認

# In[ ]:

# 通期業績
pl_table
# 株プロに無い項目: １株配


# In[ ]:

# 業績予想
fc_table


# In[ ]:

# ３ヵ月業績の推移
qr_table
# 株プロに無い項目: 売上営業損益率 = 営業益 / 売上高?


# In[ ]:

# 財務
bs_table
# 株プロに無い項目: 自己資本比率, 自己資本, 剰余金, 有利子負債倍率


# In[ ]:

pl_table.columns


# In[ ]:

fc_table.columns


# In[ ]:

qr_table.columns


# In[ ]:

bs_table.columns


# In[ ]:

pl_table.dtypes


# In[ ]:

fc_table.dtypes


# In[ ]:

qr_table.dtypes


# In[ ]:

bs_table.dtypes


# In[ ]:

len(pl_table)


# In[ ]:

len(fc_table)


# In[ ]:

len(qr_table)


# In[ ]:

len(bs_table)


# # 結合

# In[ ]:

# 通期業績 & 財務
merged_1 = pd.merge(pl_table, bs_table, on=['会計基準', '決算期', '発表日'], how='outer').sort_values('発表日')


# In[ ]:

merged_1.columns


# In[ ]:

merged_1[['会計基準', '発表日', '決算期', '売上高', '営業益', '経常益', '最終益', '１株益', '１株配', '１株純資産',
       '自己資本比率', '総資産', '自己資本', '剰余金', '有利子負債倍率']]


# In[ ]:

# 通期業績 & 業績予想
merged_2 = pd.merge(pl_table, fc_table, on=['会計基準', '決算期', '発表日'], how='outer').sort_values(['決算期', '発表日'])


# In[ ]:

merged_2.columns


# In[ ]:

merged_2[['会計基準', '発表日', '決算期', '売上高', '予想売上高', '営業益', '予想営業益', '経常益', '予想経常益', '最終益', '予想最終益', '１株益', '１株配']]


# In[ ]:

# 通期業績 & 四半期業績
merged_3 = pd.merge(pl_table, qr_table, on=['会計基準', '発表日'], how='outer').sort_values(['発表日'])


# In[ ]:

merged_3.columns


# In[ ]:

merged_3[['会計基準', '発表日', '決算期', 'Q期首', '売上高', 'Q売上高', '営業益', 'Q営業益', '経常益', 'Q経常益', 
          '最終益', 'Q最終益', '１株益', 'Q１株益', '１株配', 'Q売上営業損益率']].reset_index(drop=True)


# In[ ]:

# すべて 通期業績 + 業績予想
merged_all = pd.merge(pl_table, fc_table, on=['会計基準', '決算期', '発表日'], how='outer').sort_values(['発表日', '決算期'])


# In[ ]:

merged_all.reset_index(drop=True)


# In[ ]:

# すべて + 四半期業績
merged_all = pd.merge(merged_all, qr_table, on=['会計基準', '発表日'], how='outer').sort_values(['発表日', '決算期'])


# In[ ]:

# すべて + 財務
merged_all = pd.merge(merged_all, bs_table, on=['会計基準', '決算期', '発表日'], how='outer').sort_values(['発表日', '決算期'])


# In[ ]:

merged_all = merged_all.reset_index(drop=True)


# In[ ]:

merged_all.columns


# In[ ]:

merged_all = merged_all[['発表日', '決算期', 'Q期首', '会計基準', '売上高', '営業益', '経常益', '最終益', '１株益', '１株配', '予想売上高',
       '予想営業益', '予想経常益', '予想最終益', 'Q売上高', 'Q営業益', 'Q経常益',
       'Q最終益', 'Q１株益', 'Q売上営業損益率', '１株純資産', '自己資本比率', '総資産', '自己資本',
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

