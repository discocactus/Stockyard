
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

# ・業績予想がレンジで開示された場合は中央値を表記しています。# 25935 伊藤園第1種優先株式 ない

# 銘柄によってテーブルの構成が違う (例) 7203 トヨタ 28 個、9432 NTT 29 個
# 修正方向の矢印のパターンの数でテーブル数が動的に変わる?
# 列数が 5 以下のテーブルを削除 → 原則、各銘柄テーブル数は 10 個固定で大丈夫?
# 新規上場銘柄には存在しないテーブルがある (例) 3995 SKIYAKI 過去最高と３ヵ月業績の推移が無い、業績予想の修正履歴は空)
# → 数銘柄だけのはずなので、例外としてパスして後でマニュアル処理する?

# 通期業績テーブルの予想値の行は業績予想修正履歴テーブルに連結すべきか?

# 2000年以前の財務実績の発表日は全体的に信用できない
# 1998年は何らかの日付で固定 or 捨て、1999年と2000年は通期業績の発表日と同じにする
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
from IPython.display import display

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


# ## 参考コード

# In[ ]:

def get_price_yahoojp(code, start=None, end=None, interval='d'): # start = '2017-01-01'
    # http://sinhrks.hatenablog.com/entry/2015/02/04/002258
    # http://jbclub.xii.jp/?p=598
    base = 'http://info.finance.yahoo.co.jp/history/?code={0}.T&{1}&{2}&tm={3}&p={4}'
    
    start = pd.to_datetime(start) # Timestamp('2017-01-01 00:00:00')

    if end == None:
        end = pd.to_datetime(pd.datetime.now())
    else :
        end = pd.to_datetime(end)
    start = 'sy={0}&sm={1}&sd={2}'.format(start.year, start.month, start.day) # 'sy=2017&sm=1&sd=1'
    end = 'ey={0}&em={1}&ed={2}'.format(end.year, end.month, end.day)
    p = 1
    tmp_result = []

    if interval not in ['d', 'w', 'm', 'v']:
        raise ValueError("Invalid interval: valid values are 'd', 'w', 'm' and 'v'")

    while True:
        url = base.format(code, start, end, interval, p)
        # print(url)
        # https://info.finance.yahoo.co.jp/history/?code=7203.T&sy=2000&sm=1&sd=1&ey=2017&em=10&ed=13&tm=d&p=1
        tables = get_table(url)
        if len(tables) < 2 or len(tables[1]) == 0:
            # print('break')
            break
        tmp_result.append(tables[1]) # ページ内の3つのテーブルのうち2番目のテーブルを連結
        p += 1
        # print(p)
        
    result = pd.concat(tmp_result, ignore_index=True) # インデックスをゼロから振り直す

    result.columns = ['Date', 'Open', 'High', 'Low', 'Close', 'Volume', 'AdjClose'] # 列名を変更
    if interval == 'm':
        result['Date'] = pd.to_datetime(result['Date'], format='%Y年%m月')
    else:
        result['Date'] = pd.to_datetime(result['Date'], format='%Y年%m月%d日') # 日付の表記を変更
    result = result.set_index('Date') # インデックスを日付に変更
    result = result.sort_index()
    
    stock_name = tables[0].columns[0]
    # print([code, stock_name])
    
    return [result, stock_name]


# In[ ]:

# yahoo 初回連続読み込み
# 読み込み期間の設定
start = '2000-01-01'
end = None

# ロガー設定
start_time = dt.datetime.now()
logging.basicConfig(filename='get_price_{0}.log'.format(start_time.strftime('%Y-%m-%d')), filemode='w', level=logging.INFO)
logging.info('{0} get_price Started'.format(start_time.strftime('%Y-%m-%d %H:%M:%S')))

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
            price.to_csv('/Users/Really/Stockyard/_csv/t_{0}.csv'.format(code))
            info.to_csv('/Users/Really/Stockyard/_csv/info.csv')
            # MySQLに保存
            sql.write_price(code, price)
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
        print('{0}: Failed in {1} at get_price'.format(index, code))
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

logging.info('{0} get_price Finished'.format(dt.datetime.now().strftime('%Y-%m-%d %H:%M:%S')))


# In[ ]:

get_ipython().run_cell_magic('writefile', 'amazon_order_history.py', '\n# Amazon.co.jpの注文履歴を取得する\n\nimport sys\nimport os\nfrom robobrowser import RoboBrowser\n\n# 認証の情報は環境変数から取得する\nAMAZON_EMAIL = os.environ[\'AMAZON_EMAIL\']\nAMAZON_PASSWORD = os.environ[\'AMAZON_PASSWORD\']\n\n# RoboBrowserオブジェクトを作成する\nbrowser = RoboBrowser(\n    parser=\'html.parser\', # Beatiful Soupで使用するパーサーを指定\n    # Cookieが使用できないと表示されてログインできない問題を回避するため\n    # 通常のブラウザーのUser-Agent(ここではFirefoxのもの)を使う\n    user_agent=\'Mozilla/5.0 (Macintosh; Intel Mac OS X 10.10; rv:45.0) Gecko/20100101 Firefox/45.0\')\n\n\ndef main():\n    # 注文履歴のページを開く\n    print(\'Navigating...\', file=sys.stderr)\n    browser.open(\'https://www.amazon.co.jp/gp/css/order-history\')\n    \n    # サインインページにリダイレクトされていることを確認する\n    assert \'Amazonサインイン\' in browser.parsed.title.string\n    \n    # name="signIn" というサインインフォームを埋める。\n    # フォームのname属性の値はブラウザーの開発者ツールで確認できる。\n    form = browser.get_form(attrs={\'name\': \'signIn\'})\n    form[\'email\'] = AMAZON_EMAIL\n    form[\'password\'] = AMAZON_PASSWORD\n    \n    # フォームを送信する。正常にログインするにはRefererヘッダーとAccept-Languageヘッダーが必要。\n    print(\'Signing in...\', file=sys.stderr)\n    browser.submit_form(form, headers={\n        \'Referer\': browser.url,\n        \'Accept-Language\': \'ja,en-US;q=0.7,en;q=0.3\',\n    })\n    \n    # ログインに失敗する場合は、次の行のコメントを外してHTMLのソースを確認すると良い。\n    # print(browser.parsed.prettify())\n\n    # ページャーをたどる。\n    while True:\n        assert \'注文履歴\' in browser.parsed.title.string # 注文履歴画面が表示されていることを確認する。\n        \n        print_order_history() # 注文履歴を表示する。\n        \n        link_to_next = browser.get_link(\'次へ\') #「次へ」というテキストを持つリンクを取得する。\n        if not link_to_next:\n            break #「次へ」のリンクがない場合はループを抜けて終了する。\n            \n        print(\'Following link to next page...\', file=sys.stderr)\n        browser.follow_link(link_to_next) # 次へ」というリンクをたどる。\n        \n        \ndef print_order_history():\n    """\n    現在のページのすべての注文履歴を表示する\n    """\n    for line_item in browser.select(\'.order-info\'):\n        order = {} # 注文の情報を格納するためのdict\n        # ページ内のすべての注文履歴について反復する。ブラウザーの開発者ツールでclass属性の値を確認できる\n        # 注文の情報のすべての列について反復する\n        for column in line_item.select(\'.a-column\'):\n            label_element = column.select_one(\'.label\')\n            value_element = column.select_one(\'.value\')\n            # ラベルと値がない列は無視する。\n            if label_element and value_element:\n                label = label_element.get_text().strip()\n                value = value_element.get_text().strip()\n                order[label] = value\n        print(order[\'注文日\'], order[\'合計\']) # 注文の情報を表示する。\n        \n\nif __name__ == \'__main__\':\n    main()')


# In[ ]:

get_ipython().system('forego run python amazon_order_history.py')


# # ログイン。入力したメアドとパスワードが出力に表示されてしまうのでそのままでGitHubに上げちゃダメ！ログインを済ませたら必ずすぐにクリア！

# __TODO__ forego を利用してログイン情報を隠せないか検討

# In[ ]:

# sign-in
# 認証の情報
KT_EMAIL = input('Name?')
KT_PASSWORD = input('Password?')

# RoboBrowserオブジェクトを作成する
browser = RoboBrowser(
    parser='html.parser', # Beatiful Soupで使用するパーサーを指定
    # Cookieが使用できないと表示されてログインできない問題を回避するため
    # 通常のブラウザーのUser-Agent(ここではFirefoxのもの)を使う
    user_agent='Mozilla/5.0 (Macintosh; Intel Mac OS X 10.10; rv:45.0) Gecko/20100101 Firefox/45.0')

# ログインページを開く
print('Navigating...', file=sys.stderr)
browser.open('https://account.kabutan.jp/login')

# 株探プレミアムページにいることを確認する
assert '株探プレミアム' in browser.parsed.title.string

# name="signIn" というサインインフォームを埋める。
# フォームのname属性の値はブラウザーの開発者ツールで確認できる。
form = browser.get_form(action='/login')
form['session[email]'] = KT_EMAIL
form['session[password]'] = KT_PASSWORD

# フォームを送信する。
# 正常にログインするにはRefererヘッダーとAccept-Languageヘッダーが必要な場合がある。
print('Signing in...', file=sys.stderr)
browser.submit_form(form)

print(browser.select('.is-success')[0].text.strip())


# # 銘柄コードリスト

# In[ ]:

domestic_stock_table = sql.read_table('domestic_stock_table')


# In[ ]:

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

start_index = 30
increase_number = 10
# end_index = start_index + increase_number
end_index = len(code_list)

reading_code = code_list[start_index : end_index]
print(reading_code[-10:])
print('Next start from {0}'.format(start_index + increase_number))


# # 上場日本株全銘柄の決算ページの連続読み込み

# ## get_html 関数 (retry 付き)

# In[ ]:

@retry(tries=5, delay=1, backoff=2)
def get_html(url):
    browser.open(url)
    assert '決算' in browser.parsed.title.string # 決算ページにいることを確認する
    stock_name = browser.select('.kobetsu_data_table1_meigara')[0].text.strip()
    print('{0}: {1}'.format(code, stock_name))
    result = browser.find()
    
    return result


# ## 連続読み込み

# In[ ]:

# ロガー設定
start_time = dt.datetime.now()
logging.basicConfig(filename='get_kabutan_html_{0}.log'.format(start_time.strftime('%Y-%m-%d')), filemode='w', level=logging.INFO)
logging.info('{0} get_html Started'.format(start_time.strftime('%Y-%m-%d %H:%M:%S')))

failed = [] # 読み込みに失敗した銘柄のコードを書き込むリストを作成


# 連続読み込み書き込み
for index in range(len(reading_code)):
    code = reading_code[index]
    
    try:
        time.sleep(3 + np.random.randint(0, 3))
        
        url = 'https://kabutan.jp/stock/finance?code={0}&mode=k'.format(code)
        result = get_html(url)

        with open('/Users/Really/Stockyard/_kabutan_html/kabutan_{0}.html'.format(code), 'w') as write_html:
            write_html.write(str(result))
                    
    except Exception as e:
        logging.warning('{0} {1}: {2}'.format(dt.datetime.now().strftime('%Y-%m-%d %H:%M:%S'), code, e))
        failed.append(code)
        print('{0}: Failed in {1} at get_html'.format(index, code))
        print(e)


print('Failed in {0} stocks at get:'.format(len(failed)))
print(failed)

logging.info('{0} get_html Finished'.format(dt.datetime.now().strftime('%Y-%m-%d %H:%M:%S')))


# ## 確認

# In[ ]:

code = 7203


# In[ ]:

# 保存した html からテーブルを読み込んでみる
tables = pd.read_html('/Users/Really/Stockyard/_kabutan_html/kabutan_{0}.html'.format(code), header=0)


# In[ ]:

# 通期業績
tables[11]


# In[ ]:

# 業績予想
tables[12]


# __各銘柄のテーブル数をカウント__

# In[ ]:

table_qty = []

for index in range(len(code_list)):
    try:
        tables = pd.read_html('/Users/Really/Stockyard/_kabutan_html/kabutan_{0}.html'.format(code_list[index]), header=0)
        table_qty.append(len(tables))
    except Exception as e:
        print(code_list[index])
        print(e)


# In[ ]:

len(table_qty)


# In[ ]:

max(table_qty)
# 38


# In[ ]:

min(table_qty)
# 17


# In[ ]:

# +1 はリストに 25935 が含まれていたため
code_list[table_qty.index(max(table_qty))] # + 1
# 9101


# In[ ]:

# +1 はリストに 25935 が含まれていたため
code_list[table_qty.index(min(table_qty))] # + 1
# 3995


# __各テーブルの列数の確認__

# In[ ]:

for table_number in range(len(tables)):
    print('{0}: {1}'.format(table_number, len(tables[table_number].columns)))


# In[ ]:

for table_number in range(23, len(tables)):
    print('table_number: {0}'.format(table_number))
    display(tables[table_number])


# __列数が 5 以下のテーブルを削除して確認してみる__
# 9101
# In[ ]:

# 列数が 5 以下のテーブルを削除
tables2 = list(filter(lambda x: len(x.columns) > 5, tables))


# In[ ]:

len(tables2)


# In[ ]:

for table_number in range(len(tables2)):
    print('table_number: {0}'.format(table_number))
    display(tables2[table_number])

# 3995
# 新規上場銘柄のため、過去最高と３ヵ月業績の推移が無い
# In[ ]:

# 列数が 5 以下のテーブルを削除
tables3 = list(filter(lambda x: len(x.columns) > 5, tables))


# In[ ]:

len(tables3)


# In[ ]:

for table_number in range(len(tables3)):
    print('table_number: {0}'.format(table_number))
    display(tables3[table_number])

# 7203
# In[ ]:

# 列数が 5 以下のテーブルを削除
tables4 = list(filter(lambda x: len(x.columns) > 5, tables))


# In[ ]:

len(tables4)


# In[ ]:

for table_number in range(len(tables4)):
    print('table_number: {0}'.format(table_number))
    display(tables4[table_number])

# 9432
# In[ ]:

# 列数が 5 以下のテーブルを削除
tables5 = list(filter(lambda x: len(x.columns) > 5, tables))


# In[ ]:

len(tables5)


# In[ ]:

for table_number in range(len(tables5)):
    print('table_number: {0}'.format(table_number))
    display(tables5[table_number])


# # 保存した html ファイルからテーブル属性のみ読み込み、整形

# In[ ]:

# 保存した html からテーブル属性を読み込み
tables = pd.read_html('/Users/Really/Stockyard/_kabutan_html/kabutan_{0}.html'.format(code), header=0)


# In[ ]:

# 列数が 5 以下のテーブルを削除
tables = list(filter(lambda x: len(x.columns) > 5, tables))


# ## リスト、テーブルの概要

# In[ ]:

len(tables)


# In[ ]:

len(tables[12])


# In[ ]:

len(tables[12].columns)


# ## tables[2] 銘柄概要

# In[ ]:

tables[2]


# ## tables[3] 通期業績
# 通期業績テーブルの予想値の行は業績予想修正履歴テーブルに連結?
# In[ ]:

# 保存した html からテーブル属性を読み込み
tables = pd.read_html('/Users/Really/Stockyard/_kabutan_html/kabutan_{0}.html'.format(code), header=0)


# In[ ]:

# 列数が 5 以下のテーブルを削除
tables = list(filter(lambda x: len(x.columns) > 5, tables))


# In[ ]:

tables[3]
# 株プロに無い項目: １株配


# In[ ]:

# 全ての列項目がnullの行を除去
tables[3] = tables[3][~tables[3].isnull().all(axis=1)].reset_index(drop=True)


# In[ ]:

# 通期業績テーブルの予想値の行は業績予想修正履歴テーブルに連結?
# 予想値と前期比の行を除去
tables[3] = tables[3][~((tables[3]['決算期'].str.contains('予')) | (tables[3]['決算期'].str.contains('前期比')))].reset_index(drop=True)


# In[ ]:

# 決算期列の要素を会計基準と決算期に分割、それぞれの列に代入(同時に会計基準列を新規作成)
tables[3][['会計基準', '決算期']] = pd.DataFrame(list(tables[3]['決算期'].str.split(' ')))


# In[ ]:

# 列の並び替え
tables[3] = tables[3][['会計基準', '決算期', '売上高', '営業益', '経常益', '最終益', '１株益', '１株配', '発表日']]


# In[ ]:

# 100万円単位換算
tables[3][['売上高', '営業益', '経常益', '最終益']] = tables[3][['売上高', '営業益', '経常益', '最終益']].apply(lambda x: x * 1000000)


# In[ ]:

# 型変換
tables[3]['１株配'] = tables[3]['１株配'].astype(float)


# In[ ]:

# 日付のパース、datetime.dateへの型変換
# tables[3]['決算期'] = tables[3]['決算期'].apply(lambda x: datetime.strptime(x, '%Y.%m').date()) # 日付ではないので文字列のままの方がいいかも？
tables[3]['発表日'] = tables[3]['発表日'].apply(lambda x: parse(x, yearfirst=True).date())
# pandasのTimestampへの型変換
tables[3]['発表日'] = pd.to_datetime(tables[3]['発表日'], format='%Y-%m-%d')
# tables[3]['決算期'] = pd.to_datetime(tables[3]['決算期'], format='%Y-%m-%d')


# In[ ]:

tables[3].dtypes


# In[ ]:

tables[3]


# In[ ]:

kabupro.ix[(kabupro['証券コード'] == code) & (kabupro['会計基準'] == '米国基準') & (kabupro['決算期間'] == '通期'), 
           ['連結個別', '決算期', '期首', '売上高', '営業利益', '経常利益', '純利益', '一株当り純利益', '情報公開日 (更新日)']]


# In[ ]:

kabupro.columns


# ## tables[4] 業績予想
# 通期業績テーブルの予想値の行は業績予想修正履歴テーブルに連結?
# In[ ]:

# 保存した html からテーブル属性を読み込み
tables = pd.read_html('/Users/Really/Stockyard/_kabutan_html/kabutan_{0}.html'.format(code), header=0)


# In[ ]:

# 列数が 5 以下のテーブルを削除
tables = list(filter(lambda x: len(x.columns) > 5, tables))


# In[ ]:

tables[4]


# In[ ]:

tables[4].columns


# In[ ]:

tables[4].columns = ['会計基準', '決算期', '発表日', 
                                   '結合修正方向', '売上高修正方向', '営業益修正方向', '経常益修正方向', '最終益修正方向', '修正配当修正方向', 
                                   '予想売上高', '予想営業益', '予想経常益', '予想最終益', '予想修正配当',]


# In[ ]:

# 実体行
tables[4][tables[4].index % 2 == 0].reset_index(drop=True)


# In[ ]:

# 不要行
tables[4][tables[4].index % 2 != 0]


# In[ ]:

tables[4].columns


# In[ ]:

# 不要行、不要列の削除、並び替え
# 実績(と修正配当)はいる?いらない?
# 実績の発表と同時に次の予想が出ているのでやっぱりここではいらないのかな?
tables[4] = tables[4].ix[tables[4].index % 2 == 0, ['会計基準', '決算期', '予想売上高', '予想営業益', '予想経常益', '予想最終益', '発表日']].reset_index(drop=True)
tables[4] = tables[4].ix[tables[4]['決算期'] != '実績']


# In[ ]:

# 決算期の NaN 埋め
tables[4]['決算期'] = tables[4]['決算期'].fillna(method='ffill')


# In[ ]:

# 100万円単位換算
tables[4][['予想売上高', '予想営業益', '予想経常益', '予想最終益']] = tables[4][['予想売上高', '予想営業益', '予想経常益', '予想最終益']].apply(lambda x: x * 1000000)


# In[ ]:

# 日付のパース、datetime.dateへの型変換
# tables[4]['決算期'] = tables[4]['決算期'].apply(lambda x: datetime.strptime(x, '%Y.%m').date()) # 日付ではないので文字列のままの方がいいかも？
tables[4]['発表日'] = tables[4]['発表日'].apply(lambda x: parse(x, yearfirst=True).date())
# pandasのTimestampへの型変換
tables[4]['発表日'] = pd.to_datetime(tables[4]['発表日'], format='%Y-%m-%d')
# tables[4]['決算期'] = pd.to_datetime(tables[4]['決算期'], format='%Y-%m-%d')


# In[ ]:

# 修正配当用の処理なので不要
# '－'  を NaN に置換
# tables[4].loc[~tables[4]['修正配当'].str.isnumeric(), '修正配当'] = np.nan
# 型変換
# tables[4]['修正配当'] = tables[4]['修正配当'].astype(float)


# In[ ]:

tables[4]


# In[ ]:

tables[4].dtypes


# ## tables[5] 過去最高 【実績】
# 不要かな？
# 百万単位
# In[ ]:

tables[5]


# ## tables[6] 下期業績 (過去3年 + 今年予想 + 前年同期比)
# 不要かな？
# ちょっと株プロと見比べてみたいけどめんどくさい
# 百万単位
# In[ ]:

tables[6]


# In[ ]:

tables[6].columns


# ## tables[7] 第２四半期累計決算【実績】 (過去3年 + 前年同期比)
# 不要かな？
# ちょっと株プロと見比べてみよう → 同じみたい
# 対通期進捗率って経常益の最終的な通期実績に対して? 今年度分は予想に対して？
# 百万単位
# In[ ]:

tables[7]


# In[ ]:

kabupro.ix[(kabupro['証券コード'] == code) & (kabupro['会計基準'] == '米国基準') & (kabupro['決算期間'] == '第2四半期'), 
           ['連結個別', '期首', '売上高', '営業利益', '経常利益', '純利益', '一株当り純利益', '情報公開日 (更新日)']].tail(3)


# In[ ]:

kabupro.ix[(kabupro['証券コード'] == code)& (kabupro['会計基準'] == '米国基準') & (kabupro['決算期間'].isin(['第2四半期', '通期'])), # 
           ['連結個別', '期首', '決算期間', '売上高', '営業利益', '経常利益', '純利益', '一株当り純利益', '情報公開日 (更新日)']].tail(5)


# In[ ]:

tables[4].tail(1)


# In[ ]:

# 比較参照用
kabupro.columns


# ## tables[8] ３ヵ月業績の推移【実績】(過去5年 + 前年同期比) 累積ではなく差分

# In[ ]:

# 保存した html からテーブル属性を読み込み
tables = pd.read_html('/Users/Really/Stockyard/_kabutan_html/kabutan_{0}.html'.format(code), header=0)


# In[ ]:

# 列数が 5 以下のテーブルを削除
tables = list(filter(lambda x: len(x.columns) > 5, tables))

# 不要かな？
# ちょっと株プロと見比べてみよう → １株益の値が揃わない
# 修正発表があった項目は上書きされてしまっていると思われる
# 修正の可能性を考えなければ累積の株プロよりこちらの方が使いやすいかも
# 株プロで差分を作成するべきか?
# 前年同期比はいらなそう
# In[ ]:

tables[8]
# 株プロに無い項目: 売上営業損益率 = 営業益 / 売上高?


# In[ ]:

# 全ての列項目がnullの行を除去
tables[8] = tables[8][~tables[8].isnull().all(axis=1)].reset_index(drop=True)


# In[ ]:

# 前年同期比の行を除去
tables[8] = tables[8][~tables[8]['決算期'].str.contains('前年同期比')].reset_index(drop=True)


# In[ ]:

# 決算期列の要素を会計基準と決算期に分割、それぞれの列に代入(同時に会計基準列を新規作成)
tables[8][['会計基準', '四半期期首']] = pd.DataFrame(list(tables[8]['決算期'].str.split(' ')))


# In[ ]:

# 列の並び替え
tables[8] = tables[8][['会計基準', '四半期期首', '売上高', '営業益', '経常益', '最終益', '１株益', '売上営業損益率', '発表日']]


# In[ ]:

tables[8].columns


# In[ ]:

tables[8].columns = ['会計基準', '四半期期首', '四半期売上高', '四半期営業益', '四半期経常益', '四半期最終益', '四半期１株益', '四半期売上営業損益率', '発表日']


# In[ ]:

# 100万円単位換算
tables[8][['四半期売上高', '四半期営業益', '四半期経常益', '四半期最終益']] = tables[8][['四半期売上高', '四半期営業益', '四半期経常益', '四半期最終益']].apply(lambda x: x * 1000000)


# In[ ]:

# 日付のパース、datetime.dateへの型変換
tables[8]['四半期期首'] = tables[8]['四半期期首'].apply(lambda x: parse(x.replace('-', '.'), yearfirst=True).date())
tables[8]['発表日'] = tables[8]['発表日'].apply(lambda x: parse(x, yearfirst=True).date())
# pandasのTimestampへの型変換
tables[8]['四半期期首'] = pd.to_datetime(tables[8]['四半期期首'], format='%Y-%m-%d')
tables[8]['発表日'] = pd.to_datetime(tables[8]['発表日'], format='%Y-%m-%d')


# In[ ]:

tables[8].dtypes


# __比較検証用に株プロの四半期業績の差分を作ってみる__

# __もうちょっと上手いやり方ありそう__

# In[ ]:

kabupro.columns


# In[ ]:

kabupro.ix[(kabupro['証券コード'] == code) & (kabupro['会計基準'] == '米国基準'), 
           ['決算期', '期末', '売上高', '営業利益', '経常利益', '純利益', '一株当り純利益', '情報公開日 (更新日)']].tail(10)


# In[ ]:

diff_test = kabupro.ix[(kabupro['証券コード'] == code) & (kabupro['会計基準'] == '米国基準'), 
           ['決算期', '期末', '売上高', '営業利益', '経常利益', '純利益', '一株当り純利益', '情報公開日 (更新日)']]


# In[ ]:

diff_test


# In[ ]:

diff_test[['売上高差分', '営業利益差分', '経常利益差分', '純利益差分', '一株当り純利益差分']] = diff_test[['売上高', '営業利益', '経常利益', '純利益', '一株当り純利益']]


# In[ ]:

diff_test.index[1]


# In[ ]:

for count in range(diff_test.index[1], diff_test.index[1] + len(diff_test) - 1):
    if diff_test.loc[count, '決算期'] == diff_test.loc[count - 1, '決算期']:
        diff_test.loc[count, '売上高差分'] = diff_test.loc[count, '売上高'] - diff_test.loc[count - 1, '売上高']
        diff_test.loc[count, '営業利益差分'] = diff_test.loc[count, '営業利益'] - diff_test.loc[count - 1, '営業利益']
        diff_test.loc[count, '経常利益差分'] = diff_test.loc[count, '経常利益'] - diff_test.loc[count - 1, '経常利益']
        diff_test.loc[count, '純利益差分'] = diff_test.loc[count, '純利益'] - diff_test.loc[count - 1, '純利益']
        diff_test.loc[count, '一株当り純利益差分'] = diff_test.loc[count, '一株当り純利益'] - diff_test.loc[count - 1, '一株当り純利益']


# In[ ]:

diff_test[['決算期', '期末', '売上高差分', '営業利益差分', '経常利益差分', '純利益差分', '一株当り純利益差分', '情報公開日 (更新日)']]
# 一株当り純利益差分が株探の１株益と揃わない


# In[ ]:

tables[8]


# ## tables[9] 財務 【実績】

# In[ ]:

# 保存した html からテーブル属性を読み込み
tables = pd.read_html('/Users/Really/Stockyard/_kabutan_html/kabutan_{0}.html'.format(code), header=0)


# In[ ]:

# 列数が 5 以下のテーブルを削除
tables = list(filter(lambda x: len(x.columns) > 5, tables))

# 2000年以前の財務実績の発表日は全体的に信用できない
# 1998年は何らかの日付で固定 or 捨て、1999年と2000年は通期業績の発表日と同じにする
# 期間は株プロよりこちらの方が長い
# 修正発表があった項目は上書きされてしまっていると思われる
# In[ ]:

tables[9]
# 株プロに無い項目: 自己資本比率, 自己資本, 剰余金, 有利子負債倍率


# In[ ]:

tables[9].columns


# In[ ]:

# 7203 トヨタ 元の値が '90/08/01' で明らかにおかしい。これどうしよう。。。
tables[9].ix[2, '発表日'] = '99/08/01'


# In[ ]:

# 全ての列項目がnullの行を除去
tables[9] = tables[9][~tables[9].isnull().all(axis=1)].reset_index(drop=True)


# In[ ]:

# 決算期列の要素を会計基準と決算期に分割、それぞれの列に代入(同時に会計基準列を新規作成)
tables[9][['会計基準', '決算期']] = pd.DataFrame(list(tables[9]['決算期'].str.split(' ')))


# In[ ]:

# 列の並び替え
tables[9] = tables[9][['会計基準', '決算期', '１株純資産', '自己資本比率', '総資産', '自己資本', '剰余金', '有利子負債倍率', '発表日']]


# In[ ]:

# 決算期が 'yyyy.mm' 表記ではない行は確定決算前と判断して削除
tables[9] = tables[9][tables[9]['決算期'].str.contains('\d\d\d\d.\d\d')]


# In[ ]:

# 決算期が 1998.03 のデータは他のテーブルには無く、発表日も不自然なので行ごと削除
tables[9] = tables[9][~tables[9]['決算期'].str.contains('1998.03')].reset_index(drop=True)


# In[ ]:

tables[3]


# In[ ]:

len(tables[9]) == len(tables[3])


# In[ ]:

pd.Series(tables[9]['決算期']) == pd.Series(tables[3]['決算期'])


# In[ ]:

pd.Series(tables[9]['発表日']) == pd.Series(tables[3]['発表日'])


# In[ ]:

count = 0


# In[ ]:

if (tables[9].ix[count, '決算期'] == tables[3].ix[count, '決算期'] and tables[9].ix[count, '発表日'] == tables[3].ix[count, '発表日']):
    print('ok')
else:
    print('ng')


# In[ ]:

# '－'  を NaN に置換
# .str を2回も使わないといけないのはなんだか。。。
tables[9].loc[~tables[9]['１株純資産'].str.replace('.', '').str.isnumeric(), '１株純資産'] = np.nan
tables[9].loc[~tables[9]['有利子負債倍率'].str.replace('.', '').str.isnumeric(), '有利子負債倍率'] = np.nan


# In[ ]:

# 型変換
tables[9][['１株純資産', '有利子負債倍率']] = tables[9][['１株純資産', '有利子負債倍率']].astype(float)


# In[ ]:

# 100万円単位換算
tables[9][['総資産', '自己資本', '剰余金']] = tables[9][['総資産', '自己資本', '剰余金']].apply(lambda x: x * 1000000)


# In[ ]:

# 日付のパース、datetime.dateへの型変換
# tables[9]['決算期'] = tables[9]['決算期'].apply(lambda x: parse(x.replace('-', '.'), yearfirst=True).date()) # 日付ではないので文字列のままの方がいいかも？
tables[9]['発表日'] = tables[9]['発表日'].apply(lambda x: parse(x, yearfirst=True).date())
# pandasのTimestampへの型変換
# tables[9]['決算期'] = pd.to_datetime(tables[9]['決算期'], format='%Y-%m-%d')
tables[9]['発表日'] = pd.to_datetime(tables[9]['発表日'], format='%Y-%m-%d')


# In[ ]:

tables[9].dtypes


# In[ ]:

tables[9]
# 株プロに無い項目: 自己資本比率, 自己資本, 剰余金, 有利子負債倍率


# In[ ]:

kabupro.ix[(kabupro['証券コード'] == code) & (kabupro['会計基準'] == '米国基準') & (kabupro['決算期間'] == '通期'), 
           ['連結個別', '決算期', '期首', '一株当り純資産', '総資産', '情報公開日 (更新日)']]


# In[ ]:

# 比較参照用
kabupro.columns


# # (準備) 単一銘柄の決算ページの取得

# In[ ]:

# 個別銘柄の決算ページを開く
code = 9437 # トヨタ

print('Navigating...', file=sys.stderr)
browser.open('https://kabutan.jp/stock/finance?code={0}&mode=k'.format(code))

# 決算ページにいることを確認する
assert '決算' in browser.parsed.title.string

print(browser.select('.kobetsu_data_table1_meigara')[0].text.strip())


# ## html 全体の取得と保存

# In[ ]:

kessan_html = browser.find()
kessan_html


# In[ ]:

tables = pd.read_html(str(kessan_html), header=0)
tables[11]


# In[ ]:

# 後でhtml形式で読み込み可能なファイルとして書き出す方法
kabutan_kessan = open('kabutan_kessan.html', 'w')
kabutan_kessan.write(str(browser.find()))
kabutan_kessan.close()


# In[ ]:

# 保存したhtmlファイルからの読み込み
tables = pd.read_html('/Users/Really/Stockyard/kabutan_kessan.html', header=0)


# ## html 内のテーブル属性のみの取得と保存

# In[ ]:

tables = pd.read_html(str(browser.select('table')), header=0)
tables[11]


# In[ ]:

# 後でhtml形式で読み込み可能なファイルとして書き出す方法
kabutan_kessan_tables = open('kabutan_kessan_tables.html', 'w')
kabutan_kessan_tables.write(str(browser.select('table')))
kabutan_kessan_tables.close()


# In[ ]:

# 保存したhtmlファイルからの読み込み
tables = pd.read_html('/Users/Really/Stockyard/kabutan_kessan_tables.html', header=0)


# # (準備) 全テーブル内容の確認
# 14 以降はテーブル数が動的に変化する

#  0 主要指標情報 日経平均
#  1 主要指標情報 米ドル円
#  2 主要指標情報 ＮＹダウ (終値)
#  3 主要指標情報 上海総合 (終値)
#  4 検索窓
#  5 銘柄概要
#  6 銘柄概要
#  7 銘柄概要
#  8 銘柄概要
#  9 ＰＥＲ ＰＢＲ 利回り 信用倍率
# 10 通期業績
# 11 業績予想修正履歴 - 新規上場銘柄の場合は空
# 12 不明
# 13 修正履歴修正方向 - このテーブルの数が増減する
#  - 過去最高 【実績】 - 新規上場銘柄の場合はない
#  - 下期業績
#  - 第２四半期累計決算【実績】
#  - ３ヵ月業績の推移【実績】 - 新規上場銘柄の場合はない
#  - 財務 【実績】
#  - 日経平均チャート切り替え
# In[ ]:

# ページ上部の主要指標情報 1-4の各項目と同じところっぽい
tables[0]


# In[ ]:

# 主要指標情報 日経平均
tables[1]


# In[ ]:

# 主要指標情報 米ドル円
tables[2]


# In[ ]:

# 主要指標情報 ＮＹダウ (終値)
tables[3]


# In[ ]:

# 主要指標情報 上海総合 (終値)
tables[4]


# In[ ]:

# 検索窓
tables[5]


# In[ ]:

# 銘柄概要
tables[6]


# In[ ]:

# 銘柄概要
tables[7]


# In[ ]:

# 銘柄概要
tables[8]


# In[ ]:

# 銘柄概要
tables[9]


# In[ ]:

# ＰＥＲ ＰＢＲ 利回り 信用倍率
tables[10]


# In[ ]:

# 通期業績
tables[11]
# 株プロに無い項目: １株配


# In[ ]:

# 業績予想修正履歴
tables[12]


# In[ ]:

# 不明
tables[13]


# In[ ]:

# 修正履歴修正方向
tables[14]


# In[ ]:

tables[15]


# In[ ]:

tables[16]


# In[ ]:

tables[17]


# In[ ]:

tables[18]


# In[ ]:

tables[19]


# In[ ]:

tables[20]


# In[ ]:

tables[21]


# In[ ]:

tables[22]


# In[ ]:

tables[23]


# In[ ]:

tables[24]


# In[ ]:

tables[25]


# In[ ]:

tables[26]


# In[ ]:

tables[27]


# In[ ]:

tables[28]


# In[ ]:

tables[29]

