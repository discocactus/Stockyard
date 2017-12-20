
# coding: utf-8

# # memo
# ※単位：売上高、営業益、経常益、最終益は「百万円」。１株益、１株配は「円」。率は「％」

# ※１株純資産は「円」。自己資本比率は「％」。総資産、自己資本、剰余金は「百万円」。有利子負債倍率は「倍」

# ・「連」：日本会計基準［連結決算］、「単」：日本会計基準［非連結決算(単独決算)］、「U」：米国会計基準、「I」：国際会計基準(IFRS)、
# 「予」：予想業績、「旧」：修正前の予想業績、「新」：修正後の予想業績、「実」：実績業績、「変」：決算期変更

# ・［連結／非連結］決算区分の変更があった場合は、連続的に業績推移を追えるように、連結と非連結を混在して表示しています。
# 連結と非連結が混在しない場合は、「連」「単」表記は省略します。

# ・前期比および前年同期比は、会計基準や決算期間が異なる場合は比較できないため、「－」で表記しています。

# ・米国会計基準と国際会計基準では、「経常益」欄の数値は「税引き前利益」を表記しています。

# ・業績予想がレンジで開示された場合は中央値を表記しています。# PC: 2017-11-26 html 取得
# mobile: 2017-12-13 html 取得
# 2017年11月分東証銘柄一覧により取得

# 25935 伊藤園第1種優先株式 ない

# 銘柄によってテーブルの構成が違う (例) 7203 トヨタ 28 個、9432 NTT 29 個
# 修正方向の矢印のパターンの数でテーブル数が動的に変わる?
# 列数が 5 以下のテーブルを削除 → 原則、各銘柄テーブル数は 10 個固定で大丈夫?
# 新規上場銘柄には存在しないテーブルがある (例) 3995 SKIYAKI 過去最高と３ヵ月業績の推移が無い、業績予想の修正履歴は空)
# → 数銘柄だけのはずなので、例外としてパスして後でマニュアル処理する?

# 2000年以前の財務実績の発表日は全体的に信用できない
# 1998年は何らかの日付で固定 or 捨て、1999年と2000年は通期業績の発表日と同じにする# ahaha infomation

# 【 株探：HTMLから解析する際の注意点 】

# １．業績予想は、修正されていない場合は通期実績欄にしか情報が無い。
# ２．3ヶ月単位業績に、銘柄によって予想があったり無かったりする。

# －－－－

# 【株探：CSV利用時の注意点】

# １．業績予測以外は、業績実績を修正しても、「発表日」は反映されない。　 
# → トレードシステム作る時は、先に取得したデータを優先させる必要あり。

# ２．「適時開示」したタイミングで、公表データ以外の過去実績データも更新される事がある。
# → トレードシステム作る時は、先に取得したデータを優先させる必要あり。

# ３．「通期業績」で同じ「発表日」に、複数の決算期の発表をしていることがある。「財務 実績」は重複なし。
# 「通期業績」：１５銘柄
# ['8215', '2374', '8439', '6727', '4924', '2763', '2349', '2754', '2300', '4321', '4337', '4316', '7847', '8628', '2721']

# ４．発表日がNaTの場合がけっこうある。
# 特に2000年3月期データの発表日が無いデータが多い（1216銘柄）。　
# 「通期業績」と「財務 実績」それぞれの発表日でそれらしい方を選んで穴埋めした方がよさそう。

# ６．発表日1990-08-01のデータはおかしい
# 実際は、1998-10-20からというのが正しい。
# 「2910」など８銘柄。　BS側のデータが間違えている。　PL側は正しい。　決算期は「1999.04」
# 2910,3103,4203,5103,6103,6203,7203,8143

# －－－－

# ■ メモリ使用量
# 今後の計算のために、メモリ使用量について調べてみた。
# 横軸に「財務項目×全銘柄コード」という２段構成、
# 縦軸には、日付（1998/10/20以降の全営業日）という贅沢テーブルを作ってみたら、
# 計算過程で、最大＋７．５ＧＢに膨らんで、最終的には３ＧＢぐらいに戻った。　
# ただ、価格データもメモリに載せると、あわせて１０ＧＢぐらいに一旦膨らんで、５．５ＧＢぐらいに戻る感じ。
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
import pandas.tseries.offsets as offsets
from IPython.display import display, HTML

import stock


# In[ ]:

importlib.reload(stock)


# In[ ]:

# pandas の最大表示列数を設定 (max_rows で表示行数の設定も可能)
pd.set_option('display.max_columns', 30)


# In[ ]:

sql = stock.sql()


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

# 2017年11月分東証銘柄一覧のエクセルファイルを読み込む # http://www.jpx.co.jp/markets/statistics-equities/misc/01.html
all_stock_table = pd.read_excel('/Users/Really/Stockyard/_dl_data/data_j_1711.xls')
all_stock_table.columns = ['date', 'code', 'name', 'market', 'code_33', 'category_33', 'code_17', 'category_17', 'code_scale', 'scale'] # 列名を変更


# In[ ]:

# 内国株のテーブル作成
domestic_stock_table = all_stock_table.ix[all_stock_table['market'].str.contains('内国株')].reset_index(drop=True)


# In[ ]:

# 表示
domestic_stock_table


# In[ ]:

code_list = list(domestic_stock_table['code'])


# In[ ]:

# 伊藤園第1種優先株式を削除
# 要素の値を直接指定して削除することができる
code_list.remove(25935)


# In[ ]:

code_list[-10:]


# In[ ]:

len(code_list)


# In[ ]:

start_index = 0
increase_number = 1000
#end_index = start_index + increase_number
end_index = len(code_list)

reading_code = code_list[start_index : end_index]
print(len(reading_code))
print(reading_code[-10:])
print('Next start from {0}'.format(start_index + increase_number))


# # 保存した html ファイルからテーブル属性のみ読み込み、整形

# In[ ]:

code = 1301 # 1909


# In[ ]:

# 失敗分再実行用
reading_code = failed


# __TODO 検討__  
# 予想値が無いからといって予想値テーブルの結合をスキップしてしまうと列の構成が違ってしまう  
# かといって結合すると決算期が NaN の行が追加されてしまうのがいまいち  
# 3975 通期業績と財務実績テーブルがない  
# 3995 四半期業績テーブルがない

# In[ ]:

# ロガー設定
start_time = dt.datetime.now()
logging.basicConfig(filename='kabutan_merge_{0}.log'.format(start_time.strftime('%Y-%m-%d')), filemode='w', level=logging.INFO)
logging.info('{0} kabutan_merge Started'.format(start_time.strftime('%Y-%m-%d %H:%M:%S')))

# failed = []

for index_num, code in enumerate(reading_code):
    try:
        # 保存した html からテーブル属性を読み込み
        tables = pd.read_html('/Users/Really/Stockyard/_kabutan_html/kabutan_{0}.html'.format(code), header=0)
        # 列数が 5 以下のテーブルを削除
        tables = list(filter(lambda x: len(x.columns) > 5, tables))
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

        # 保存した モバイル用 html からテーブル属性を読み込み
        mobile = pd.read_html('/Users/Really/Stockyard/_kabutan_mobile_html/kabutan_{0}.html'.format(code), header=0)
        # 抽出用テーブルの作成
        pl_mobile = pd.DataFrame()
        fc_mobile = pd.DataFrame()
        qr_mobile = pd.DataFrame()
        bs_mobile = pd.DataFrame()
        # 必要なテーブルの抽出
        # リストを要素ごとに for で回す書き方
        for table in mobile:
            # 通期業績: profit and loss statement
            if len(table.columns) == 8: 
                if (table.columns[-3] == "１株配") & (pl_mobile.shape[1] == 0): 
                    pl_mobile = table.copy()
            # 業績予想: forecast
            if len(table.columns) >= 7: 
                if (table.columns[0] == "修正日") & (fc_mobile.shape[1] == 0): 
                    fc_mobile = table.copy()
            # 3ヶ月業績: quater
            if len(table.columns) == 8: 
                if (table.columns[-3] == "売上営業損益率") & (qr_mobile.shape[1] == 0): 
                    qr_mobile = table.copy()
            # 財務: balance sheet
            if len(table.columns) == 8: 
                if (table.columns[0] == "１株純資産") & (bs_mobile.shape[1] == 0): 
                    bs_mobile = table.copy()

        try:
            ## pl_table (tables[3]) 通期業績
            # 全ての列項目がnullの行を除去
            pl_table = pl_table[~pl_table.isnull().all(axis=1)].reset_index(drop=True)
            # モバイル版の会計基準を結合
            pl_table['会計基準'] = pl_mobile['会計基準']
            # 後で四半期業績の決算期作成に使うので予想値行削除前に保持しておく
            pl_end = pl_table['決算期'][~pl_table['決算期'].str.contains('前期比')].apply(lambda x: x.split(' ')[-1])
            # 予想値と前期比の行を除去
            pl_table = pl_table[~((pl_table['決算期'].str.contains('予')) | (pl_table['決算期'].str.contains('前期比')))].reset_index(drop=True)
            # 決算期変更列を新規作成、決算期列から決算期と決算期変更を抽出、代入
            # 後で四半期業績の決算期作成に使うのでこのテーブルでは予想値行削除前に処理する
            pl_table['決算期'] = pl_table['決算期'].astype(str) # 決算期列が float 型になっている場合に備え str 型を明示
            pl_table['決算期変更'] = ""
            for idx, end in pl_table['決算期'].iteritems():
                if '変' in end:
                    pl_table.loc[idx, '決算期変更'] = '変更'
                pl_table.loc[idx, '決算期'] = end.split(' ')[-1]
            # 日付のパース、datetime.dateへの型変換、最終的に '－'  は NaT に置換される
            pl_table['発表日'] = pl_table.loc[pl_table['発表日'].str.match('\d\d/\d\d/\d\d'), '発表日'].apply(lambda x: parse(x, yearfirst=True).date())
            # pandasのTimestampへの型変換
            pl_table['発表日'] = pd.to_datetime(pl_table['発表日'], format='%Y-%m-%d')
            # 数値の列の数値以外の文字列 ('－' 等) を NaN に置換
            num_col = ('売上高', '営業益', '経常益', '最終益', '１株益', '１株配')
            for key in num_col:
                if pl_table[key].dtypes == object:
                    pl_table.loc[~pl_table[key].str.replace(r'\.|\-', "").str.isnumeric(), key] = np.nan # .str を2回も使わないといけないのはなんだか。。。
            # 型変換 # 辞書内包表記による一括変換
            pl_table = pl_table.astype({x: float for x in ('売上高', '営業益', '経常益', '最終益', '１株益', '１株配')})
            # 100万円単位換算
            million_col = ('売上高', '営業益', '経常益', '最終益')
            pl_table.loc[:, million_col] = pl_table.loc[:, million_col].apply(lambda x: x * 10 ** 6)
            # 列の並び替え
            pl_table = pl_table[['発表日', '決算期', '売上高', '営業益', '経常益', '最終益', '１株益', '１株配', '会計基準', '決算期変更']]

            ## fc_table (tables[4]) 業績予想
            # 業績予想データが無い場合、ダミーのデータフレームを作成
            if len(fc_table.columns) < 9:
                fc_table = pd.DataFrame("",
                                 index=[0],
                                 columns=range(14))
            # 列名の変更
            fc_table.columns = ['会計基準', '決算期', '発表日', 
                                               '結合修正方向', '売上高修正方向', '営業益修正方向', '経常益修正方向', '最終益修正方向', '修正配当修正方向', 
                                               '予想売上高', '予想営業益', '予想経常益', '予想最終益', '予想修正配当',]
            # 不要行、不要列の削除、並び替え
            fc_table = fc_table.ix[fc_table.index % 2 == 0, ['会計基準', '決算期', '予想売上高', '予想営業益', '予想経常益', '予想最終益', '予想修正配当', '発表日']].reset_index(drop=True)
            # モバイル版の会計基準を代入 (業績予想データが無い場合はスキップ)
            if len(fc_mobile) > 0:
                fc_table['会計基準'] = fc_mobile['会計基準']
            # 実績は不要?
            fc_table = fc_table.ix[fc_table['決算期'] != '実績'].reset_index(drop=True)
            # 決算期の NaN 埋め
            fc_table['決算期'] = fc_table['決算期'].fillna(method='ffill')
            # 決算期変更列を新規作成、決算期列から決算期と決算期変更を抽出、代入
            fc_table['決算期'] = fc_table['決算期'].astype(str) # 決算期列が float 型になっている場合に備え str 型を明示
            fc_table['決算期変更'] = ""
            for idx, end in fc_table['決算期'].iteritems():
                if '変' in end:
                    fc_table.loc[idx, '決算期変更'] = '変更'
                fc_table.loc[idx, '決算期'] = end.split(' ')[-1]
            # 日付のパース、datetime.dateへの型変換、最終的に '－'  は NaT に置換される
            fc_table['発表日'] = fc_table.loc[fc_table['発表日'].str.match('\d\d/\d\d/\d\d'), '発表日'].apply(lambda x: parse(x, yearfirst=True).date())
            # pandasのTimestampへの型変換
            fc_table['発表日'] = pd.to_datetime(fc_table['発表日'], format='%Y-%m-%d')
            # 修正配当の列から分割併合記号を分離 (修正配当の予想値は入っていない銘柄もある)
            fc_table['分割併合'] = ""
            if fc_table['予想修正配当'].dtypes == object:
                for idx, col in fc_table['予想修正配当'].iteritems():
                    splited = re.findall(r'[\d.]+|\D+', col)
                    if len(splited) > 1:
                        if splited[1] == '*':
                            splited[1] = '分割併合実施'
                        elif splited[1] == '#':
                            splited[1] = '当期実施予定'
                        fc_table.loc[idx, ['予想修正配当', '分割併合']] = splited
            # 数値の列の数値以外の文字列 ('－' 等) を NaN に置換
            num_col = ('予想売上高', '予想営業益', '予想経常益', '予想最終益', '予想修正配当')
            for key in num_col:
                if fc_table[key].dtypes == object:
                    fc_table.loc[~fc_table[key].str.replace(r'\.|\-', "").str.isnumeric(), key] = np.nan # .str を2回も使わないといけないのはなんだか。。。
            # 型変換 # 辞書内包表記による一括変換
            fc_table = fc_table.astype({x: float for x in ('予想売上高', '予想営業益', '予想経常益', '予想最終益', '予想修正配当')})
            # 100万円単位換算
            million_col = ('予想売上高', '予想営業益', '予想経常益', '予想最終益')
            fc_table.loc[:, million_col] = fc_table.loc[:, million_col].apply(lambda x: x * 10 ** 6)
            # 列の並び替え
            fc_table = fc_table[['発表日', '決算期', '予想売上高', '予想営業益', '予想経常益', '予想最終益', '予想修正配当', '分割併合', '会計基準', '決算期変更']]

            ## qr_table (tables[8?]) ３ヵ月業績の推移【実績】(過去5年 + 前年同期比) 累積ではなく差分
            # 全ての列項目がnullの行を除去
            qr_table = qr_table[~qr_table.isnull().all(axis=1)].reset_index(drop=True)
            # モバイル版の会計基準を結合
            qr_table['会計基準'] = qr_mobile['会計基準']
            # 予想値と前年同期比の行を除去
            qr_table = qr_table[~((qr_table['決算期'].str.contains('予')) | (qr_table['決算期'].str.contains('前年同期比')))].reset_index(drop=True)
            # 決算期変更列を新規作成、決算期列から決算期と決算期変更を抽出、代入
            qr_table['決算期'] = qr_table['決算期'].astype(str) # 決算期列が float 型になっている場合に備え str 型を明示
            qr_table['決算期変更'] = ""
            for idx, end in qr_table['決算期'].iteritems():
                if '変' in end:
                    qr_table.loc[idx, '決算期変更'] = '変更'
                qr_table.loc[idx, '決算期'] = end.split(' ')[-1]
            # 列名の変更
            qr_table.columns = ['Q期首', 'Q売上高', 'Q営業益', 'Q経常益', 'Q最終益', 'Q１株益', 'Q売上営業損益率', '発表日', '会計基準', '決算期変更']
            # 日付のパース、datetime.dateへの型変換、最終的に '－'  は NaT に置換される
            qr_table['Q期首'] = qr_table.loc[qr_table['Q期首'].str.match('\d\d.\d\d-\d\d'), 'Q期首'].apply(lambda x: 
                                                                                                           parse(x.replace('-', '.'), yearfirst=True).date())
            qr_table['発表日'] = qr_table.loc[qr_table['発表日'].str.match('\d\d/\d\d/\d\d'), '発表日'].apply(lambda x: parse(x, yearfirst=True).date())
            # pandasのTimestampへの型変換
            qr_table['Q期首'] = pd.to_datetime(qr_table['Q期首'], format='%Y-%m-%d')
            qr_table['発表日'] = pd.to_datetime(qr_table['発表日'], format='%Y-%m-%d')
            # 通期業績の決算期を参照して決算期列を追加
            # 通期業績の予想値削除前に別名でキープした決算期シリーズを利用
            for start_idx, start in qr_table['Q期首'].iteritems():
                for end in pl_end:
                    if start < pd.to_datetime(end, format='%Y.%m') + offsets.MonthEnd():
                        qr_table.loc[start_idx, '決算期'] = end
                        break
            # 数値の列の数値以外の文字列 ('－' 等) を NaN に置換
            num_col = ('Q売上高', 'Q営業益', 'Q経常益', 'Q最終益', 'Q１株益', 'Q売上営業損益率')
            for key in num_col:
                if qr_table[key].dtypes == object:
                    qr_table.loc[~qr_table[key].str.replace(r'\.|\-', "").str.isnumeric(), key] = np.nan # .str を2回も使わないといけないのはなんだか。。。
            # 型変換 # 辞書内包表記による一括変換
            qr_table = qr_table.astype({x: float for x in ('Q売上高', 'Q営業益', 'Q経常益', 'Q最終益', 'Q１株益', 'Q売上営業損益率')})
            # 100万円単位換算
            million_col = ('Q売上高', 'Q営業益', 'Q経常益', 'Q最終益')
            qr_table.loc[:, million_col] = qr_table.loc[:, million_col].apply(lambda x: x * 10 ** 6)
            # 列の並び替え
            qr_table = qr_table[['発表日', '決算期', 'Q期首', 'Q売上高', 'Q営業益', 'Q経常益', 'Q最終益', 'Q１株益', 'Q売上営業損益率', '会計基準', '決算期変更']]

            ## bs_table (tables[9?]) 財務 【実績】
            # 全ての列項目がnullの行を除去
            bs_table = bs_table[~bs_table.isnull().all(axis=1)].reset_index(drop=True)
            # モバイル版の会計基準を結合
            bs_table['会計基準'] = bs_mobile['会計基準']
            # 予想値と前期比の行を除去
            bs_table = bs_table[~((bs_table['決算期'].str.contains('予')) | (bs_table['決算期'].str.contains('前期比')))].reset_index(drop=True)
            # 決算期変更列を新規作成、決算期列から決算期と決算期変更を抽出、代入
            bs_table['決算期'] = bs_table['決算期'].astype(str) # 決算期列が float 型になっている場合に備え str 型を明示
            bs_table['決算期変更'] = ""
            for idx, end in bs_table['決算期'].iteritems():
                if '変' in end:
                    bs_table.loc[idx, '決算期変更'] = '変更'
                bs_table.loc[idx, '決算期'] = end.split(' ')[-1]
            # 決算期が 'yyyy.mm' 表記ではない行は確定決算前と思われるので削除
            bs_table = bs_table[bs_table['決算期'].str.contains('\d\d\d\d.\d\d')].reset_index(drop=True)
            # 決算期が 1998.mm のデータは他のテーブルには無く、発表日も不自然なので行ごと削除
            # bs_table = bs_table[~bs_table['決算期'].str.contains('1998.\d\d')].reset_index(drop=True)
            # 通期業績には無い期間の行を削除
            for idx, end in bs_table['決算期'].iteritems():
                if not end in pl_table['決算期'].values:
                    bs_table = bs_table.drop(idx)
            # 日付のパース、datetime.dateへの型変換、最終的に '－'  は NaT に置換される
            bs_table['発表日'] = bs_table.loc[bs_table['発表日'].str.match('\d\d/\d\d/\d\d'), '発表日'].apply(lambda x: parse(x, yearfirst=True).date())
            # pandasのTimestampへの型変換
            bs_table['発表日'] = pd.to_datetime(bs_table['発表日'], format='%Y-%m-%d')
            # 決算期の同じ年の月が通期業績と異なる場合があるので、通期業績の決算期に置換
            # 決算期の変更があり、なおかつ決算期に「変」記載のない銘柄で確認 (1909)
            for idx, end in bs_table['決算期'].iteritems():
                bs_table.loc[idx, '決算期'] = pl_table.loc[pl_table['決算期'].apply(lambda x: x[:4]) == bs_table.loc[idx, '決算期'][:4], '決算期'].values[0]
            # 発表日の欠損値および異常値を通期業績の発表日に置換
            for idx, date in bs_table['発表日'].iteritems():
                if (date != date) or (date < pd.to_datetime('2001-01-01')):
                    bs_table.loc[idx, '発表日'] = pl_table.loc[pl_table['決算期'] == bs_table.loc[idx, '決算期'], '発表日'].values[0]
            # 決算期変更の欠損値を通期業績の値に置換
            for idx, change in bs_table['決算期変更'].iteritems():
                if change == "":
                    bs_table.loc[idx, '決算期変更'] = pl_table.loc[pl_table['決算期'] == bs_table.loc[idx, '決算期'], '決算期変更'].values[0]
            # 数値の列の数値以外の文字列 ('－' 等) を NaN に置換
            num_col = ('１株純資産', '自己資本比率', '総資産', '自己資本', '剰余金', '有利子負債倍率')
            for key in num_col:
                if bs_table[key].dtypes == object:
                    bs_table.loc[~bs_table[key].str.replace(r'\.|\-', "").str.isnumeric(), key] = np.nan # .str を2回も使わないといけないのはなんだか。。。
                    # bs_table.loc[bs_table[key].str.contains('－'), key] = np.nan # この書き方だと '－'  以外の文字列に対応できないので不安
            # 型変換 # 辞書内包表記による一括変換
            bs_table = bs_table.astype({x: float for x in ('１株純資産', '自己資本比率', '総資産', '自己資本', '剰余金', '有利子負債倍率')})
            # 100万円単位換算
            million_col = ('総資産', '自己資本', '剰余金')
            bs_table.loc[:, million_col] = bs_table.loc[:, million_col].apply(lambda x: x * 10 ** 6)
            # 列の並び替え
            bs_table = bs_table[['発表日', '決算期', '１株純資産', '自己資本比率', '総資産', '自己資本', '剰余金', '有利子負債倍率', '会計基準', '決算期変更']]
            
            try:
                ## テーブル結合
                # 通期業績 + 財務
                merged_table = pd.merge(pl_table, bs_table, on=['発表日', '決算期', '会計基準', '決算期変更'], how='outer').sort_values(['発表日', '決算期'])
                # + 四半期業績
                merged_table = pd.merge(merged_table, qr_table, on=['発表日', '決算期', '会計基準', '決算期変更'], how='outer').sort_values(['発表日', '決算期'])
                # + 業績予想 (業績予想データが無い場合はスキップ、空文字の分割併合列を追加)
                if (len(fc_table) == 1) and (fc_table.loc[0, '決算期'] == ""):
                    merged_table['分割併合'] = ""
                else:
                    merged_table = pd.merge(merged_table, fc_table, on=['発表日', '決算期', '会計基準', '決算期変更'], how='outer').sort_values(['発表日', '決算期'])
                # 再インデックス
                merged_table = merged_table.reset_index(drop=True)
                # 結合により代入された分割併合の NaN を空の文字列に置換
                for idx, value in merged_table['分割併合'].iteritems():
                    if value != value:
                        merged_table.loc[idx, '分割併合'] = ""
                        
                try:
                    ## 保存
                    # MySQL
                    sql.write_table('kt_{0}'.format(code), merged_table)
                    # CSV
                    merged_table.to_csv('/Users/Really/Stockyard/_kabutan_csv/kt_{0}.csv'.format(code))

                except Exception as e:
                    logging.warning('{0} {1}: at saving - {2}'.format(dt.datetime.now().strftime('%Y-%m-%d %H:%M:%S'), code, e))
                    failed.append(code)
                    print('Failed in {0} at saving.'.format(code))
                    print(e)
                    
                print('{0} - {1}: processed. shape{2}'.format(index_num, code, merged_table.shape))

            except Exception as e:
                logging.warning('{0} {1}: at merge - {2}'.format(dt.datetime.now().strftime('%Y-%m-%d %H:%M:%S'), code, e))
                failed.append(code)
                print('Failed in {0} at merge.'.format(code))
                print(e)

        except Exception as e:
            logging.warning('{0} {1}: at shaping - {2}'.format(dt.datetime.now().strftime('%Y-%m-%d %H:%M:%S'), code, e))
            failed.append(code)
            print('Failed in {0} at shaping.'.format(code))
            print(e)
    
    except Exception as e:
        logging.warning('{0} {1}: at read html - {2}'.format(dt.datetime.now().strftime('%Y-%m-%d %H:%M:%S'), code, e))
        failed.append(code)
        print('Failed in {0} at read html.'.format(code))
        print(e)
    
print('Failed in {0} stocks:'.format(len(failed)))
print(failed)
logging.info('{0} kabutan_merge Finished'.format(dt.datetime.now().strftime('%Y-%m-%d %H:%M:%S')))


# In[ ]:

failed


# In[ ]:

pd.Series(failed).to_csv('/Users/Really/Stockyard/_csv/kabutan_merge_failed.csv')


# In[ ]:

failed = pd.read_csv('/Users/Really/Stockyard/_csv/kabutan_merge_failed.csv', header=None, index_col=0)


# In[ ]:

merged_table


# In[ ]:

display(pl_table)
display(bs_table)
display(fc_table)
display(qr_table)
# display(bs_table)


# In[ ]:

shorter_table = []
for code in reading_code:
    try:
        table_sql = sql.read_table('kt_{0}'.format(code))
        if table_sql.shape[1] < 29:
            shorter_table.append(code)
            print('{0}: shorter'.format(code))
    except Exception as e:
        print(e)


# In[ ]:

shorter_table


# In[ ]:

pd.Series(shorter_table).to_csv('/Users/Really/Stockyard/_csv/kabutan_merge_shorter_table.csv')


# # 結合

# In[ ]:

# 通期業績 & 財務
pl_bs= pd.merge(pl_table, bs_table, on=['発表日', '決算期', '会計基準', '決算期変更'], how='outer').sort_values(['発表日', '決算期'])


# In[ ]:

# 通期業績 & 予想
pl_fc = pd.merge(pl_table, fc_table, on=['発表日', '決算期', '会計基準', '決算期変更'], how='outer').sort_values(['発表日', '決算期'])


# In[ ]:

pl_fc.columns


# In[ ]:

pl_fc = pl_fc[['発表日', '決算期',
                 '予想売上高', '売上高', '予想営業益', '営業益', '予想経常益', '経常益', '予想最終益', '最終益', '１株益', '予想修正配当', '１株配', 
                 '会計基準', '決算期変更', '分割併合']]


# In[ ]:

pl_fc


# In[ ]:

# 通期業績 + 財務
merged_table = pd.merge(pl_table, bs_table, on=['発表日', '決算期', '会計基準', '決算期変更'], how='outer').sort_values(['発表日', '決算期'])

# + 四半期業績
merged_table = pd.merge(merged_table, qr_table, on=['発表日', '決算期', '会計基準', '決算期変更'], how='outer').sort_values(['発表日', '決算期'])

# + 業績予想
merged_table = pd.merge(merged_table, fc_table, on=['発表日', '決算期', '会計基準', '決算期変更'], how='outer').sort_values(['発表日', '決算期'])

# 再インデックス
merged_table = merged_table.reset_index(drop=True)

# 結合により代入された分割併合の NaN を空の文字列に置換
for idx, value in merged_table['分割併合'].iteritems():
    if value != value:
        merged_table.loc[idx, '分割併合'] = ""


# In[ ]:

merged_table.columns


# In[ ]:

merged_table


# In[ ]:

merged_table.dtypes


# In[ ]:

sql.write_table('kt_{0}'.format(code), merged_table)


# In[ ]:

table_sql = sql.read_table('kt_{0}'.format(code))


# In[ ]:

table_sql


# In[ ]:

table_sql.dtypes


# In[ ]:

merged_table.to_csv('/Users/Really/Stockyard/_kabutan_csv/kt_{0}.csv'.format(code))


# In[ ]:



