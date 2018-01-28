
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

# ６．発表日1990-08-01のデータはおかしい -> 1999-08-01, 2001-01-01 もおかしいと思う
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
    # user_agent='Mozilla/5.0 (iPhone; CPU iPhone OS 9_3_5 like Mac OS X) AppleWebKit/601.1.46 (KHTML, like Gecko) Version/9.0 Mobile/13G36 Safari/601.1')

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
increase_number = 2
#end_index = start_index + increase_number
end_index = len(code_list)

reading_code = code_list[start_index : end_index]
print(len(reading_code))
print(reading_code[-10:])
print('Next start from {0}'.format(start_index + increase_number))


# In[ ]:


reading_code = [3975, 3995, 7196, 7810, 9262]
reading_code


# # 上場日本株全銘柄の決算ページの連続読み込み

# ## get_html 関数 (retry 付き)

# In[ ]:


@retry(tries=5, delay=1, backoff=2)
def get_html(url):
    browser.open(url)
    assert '決算' in browser.parsed.title.string # 決算ページにいることを確認する
    stock_name = browser.select('.kobetsu_data_table1_meigara')[0].text.strip()  # モバイルサイトの場合はコメントアウト
    print('{0}: {1}'.format(code, stock_name))  # モバイルサイトの場合はコメントアウト
    result = browser.find()
    
    return result


# __モバイルサイト用__

# In[ ]:


@retry(tries=5, delay=1, backoff=2)
def get_html(url):
    browser.open(url)
    assert '決算' in browser.parsed.title.string # 決算ページにいることを確認する
    # stock_name = browser.select('.kobetsu_data_table1_meigara')[0].text.strip()  # モバイルサイトの場合はコメントアウト
    # print('{0}: {1}'.format(code, stock_name))  # モバイルサイトの場合はコメントアウト
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
    # print(code) # モバイルサイト用
    
    try:
        time.sleep(3 + np.random.randint(0, 3))
        
        url = 'https://kabutan.jp/stock/finance?code={0}&mode=k'.format(code)
        # url = 'https://s.kabutan.jp/stock/finance?code={0}&mode=k'.format(code)
        result = get_html(url)

        with open('/Users/Really/Stockyard/_kabutan_html/kabutan_{0}.html'.format(code), 'w') as write_html:
        # with open('/Users/Really/Stockyard/_kabutan_mobile_html/kabutan_{0}.html'.format(code), 'w') as write_html:
            write_html.write(str(result))
                    
    except Exception as e:
        logging.warning('{0} {1}: {2}'.format(dt.datetime.now().strftime('%Y-%m-%d %H:%M:%S'), code, e))
        failed.append(code)
        print('{0}: Failed in {1} at get_html'.format(index, code))
        print(e)


print('Failed in {0} stocks at get:'.format(len(failed)))
print(failed)

logging.info('{0} get_html Finished'.format(dt.datetime.now().strftime('%Y-%m-%d %H:%M:%S')))


# __モバイルサイト用__

# In[ ]:


# ロガー設定
start_time = dt.datetime.now()
logging.basicConfig(filename='get_kabutan_html_{0}.log'.format(start_time.strftime('%Y-%m-%d')), filemode='w', level=logging.INFO)
logging.info('{0} get_html Started'.format(start_time.strftime('%Y-%m-%d %H:%M:%S')))

failed = [] # 読み込みに失敗した銘柄のコードを書き込むリストを作成


# 連続読み込み書き込み
for index in range(len(reading_code)):
    code = reading_code[index]
    print(code) # モバイルサイト用
    
    try:
        time.sleep(3 + np.random.randint(0, 3))
        
        # url = 'https://kabutan.jp/stock/finance?code={0}&mode=k'.format(code)
        url = 'https://s.kabutan.jp/stock/finance?code={0}&mode=k'.format(code)
        result = get_html(url)

        # with open('/Users/Really/Stockyard/_kabutan_html/kabutan_{0}.html'.format(code), 'w') as write_html:
        with open('/Users/Really/Stockyard/_kabutan_mobile_html/kabutan_{0}.html'.format(code), 'w') as write_html:
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


code = 1743


# In[ ]:


# 保存した PC用 html からテーブルを読み込んでみる
tables = pd.read_html('/Users/Really/Stockyard/_kabutan_html/kabutan_{0}.html'.format(code), header=0)


# In[ ]:


for idx in range(len(tables)):
    print(idx)
    display(tables[idx])


# In[ ]:


# 保存した モバイル用 html からテーブルを読み込んでみる
mobile = pd.read_html('/Users/Really/Stockyard/_kabutan_mobile_html/kabutan_{0}.html'.format(code), header=0)


# In[ ]:


for idx in range(len(mobile)):
    print(idx)
    display(mobile[idx])


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


code_list[table_qty.index(max(table_qty))]
# 9101


# In[ ]:


code_list[table_qty.index(min(table_qty))]
# 3995


# __各テーブルの列数の確認__

# In[ ]:


for table_number in range(len(tables)):
    print('{0}: {1}'.format(table_number, len(tables[table_number].columns)))


# # 読み込み〜整形、連続処理

# In[ ]:


# ---- 保存した html ファイルからテーブル属性のみ読み込み、整形 ---- #

code = 3975 # 9262 # 7810 # 7196 # 3995 # 3975 # 3863 # 3480 # 1418 # 1408 # 1376 # 7203 # 1909

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
for idx, table in enumerate(mobile):
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
            bs_mobile_idx = idx

            
# ---- pl_table 通期業績 ---- #


# 全ての列項目がnullの行を除去
pl_table = pl_table[~pl_table.isnull().all(axis=1)].reset_index(drop=True)

# モバイル版の会計基準を結合、無い場合は空の列を作成
if len(pl_mobile) > 0:
    pl_table['会計基準'] = pl_mobile['会計基準']
else:
    pl_table['会計基準'] = ""

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
# pl_table['決算期'] = pl_table['決算期'].apply(lambda x: datetime.strptime(x, '%Y.%m').date()) # 日付ではないので文字列のままの方がいいかも？
pl_table['発表日'] = pl_table.loc[pl_table['発表日'].str.match('\d\d/\d\d/\d\d'), '発表日'].apply(lambda x: parse(x, yearfirst=True).date())
# pandasのTimestampへの型変換
pl_table['発表日'] = pd.to_datetime(pl_table['発表日'], format='%Y-%m-%d')
# pl_table['決算期'] = pd.to_datetime(pl_table['決算期'], format='%Y-%m-%d')

# 数値の列の数値以外の文字列 ('－' 等) を NaN に置換
num_col = ('売上高', '営業益', '経常益', '最終益', '１株益', '１株配')
for key in num_col:
    if pl_table[key].dtypes == object:
        pl_table.loc[~pl_table[key].str.replace(r'\.|\-', "").str.isnumeric(), key] = np.nan # .str を2回も使わないといけないのはなんだか。。。
        # pl_table.loc[pl_table[key].str.contains('－'), key] = np.nan # この書き方だと '－'  以外の文字列に対応できないので不安

# 型変換
# 辞書内包表記による一括変換
pl_table = pl_table.astype({x: float for x in ('売上高', '営業益', '経常益', '最終益', '１株益', '１株配')})

# 100万円単位換算
million_col = ('売上高', '営業益', '経常益', '最終益')
pl_table.loc[:, million_col] = pl_table.loc[:, million_col].apply(lambda x: x * 10 ** 6)

# 列の並び替え
pl_table = pl_table[['発表日', '決算期', '売上高', '営業益', '経常益', '最終益', '１株益', '１株配', '会計基準', '決算期変更']]


# ---- fc_table 業績予想 ---- #


# 業績予想データが無い場合、ダミーのデータフレームを作成
if len(fc_table.columns) < 9:
    fc_table = pd.DataFrame(index=[0], columns=range(14))

# 列名の変更
fc_table.columns = ['会計基準', '決算期', '発表日', 
                                   '結合修正方向', '売上高修正方向', '営業益修正方向', '経常益修正方向', '最終益修正方向', '修正配当修正方向', 
                                   '予想売上高', '予想営業益', '予想経常益', '予想最終益', '予想修正配当',]

# 不要行、不要列の削除、並び替え
fc_table = fc_table.ix[fc_table.index % 2 == 0, ['会計基準', '決算期', '予想売上高', '予想営業益', '予想経常益', '予想最終益', '予想修正配当', '発表日']].reset_index(drop=True)

# 全ての列項目がnullの行を除去
fc_table = fc_table[~fc_table.isnull().all(axis=1)].reset_index(drop=True)

# モバイル版の会計基準を代入、無い場合は空値を代入
if len(fc_mobile) > 0:
    fc_table['会計基準'] = fc_mobile['会計基準']
elif len(fc_table) > 0:
    fc_table['会計基準'] = ""

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
# fc_table['決算期'] = fc_table['決算期'].apply(lambda x: datetime.strptime(x, '%Y.%m').date()) # 日付ではないので文字列のままの方がいいかも？
fc_table['発表日'] = fc_table.loc[fc_table['発表日'].str.match('\d\d/\d\d/\d\d'), '発表日'].apply(lambda x: parse(x, yearfirst=True).date())
# pandasのTimestampへの型変換
fc_table['発表日'] = pd.to_datetime(fc_table['発表日'], format='%Y-%m-%d')
# fc_table['決算期'] = pd.to_datetime(fc_table['決算期'], format='%Y-%m-%d')

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
        # fc_table.loc[fc_table[key].str.contains('－'), key] = np.nan # この書き方だと '－'  以外の文字列に対応できないので不安

# 型変換
# 辞書内包表記による一括変換
fc_table = fc_table.astype({x: float for x in ('予想売上高', '予想営業益', '予想経常益', '予想最終益', '予想修正配当')})

# 100万円単位換算
million_col = ('予想売上高', '予想営業益', '予想経常益', '予想最終益')
fc_table.loc[:, million_col] = fc_table.loc[:, million_col].apply(lambda x: x * 10 ** 6)

# 列の並び替え
fc_table = fc_table[['発表日', '決算期', '予想売上高', '予想営業益', '予想経常益', '予想最終益', '予想修正配当', '分割併合', '会計基準', '決算期変更']]


# ---- qr_table 四半期業績 ---- #


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
        # qr_table.loc[qr_table[key].str.contains('－'), key] = np.nan # この書き方だと '－'  以外の文字列に対応できないので不安

# 型変換
# 辞書内包表記による一括変換
qr_table = qr_table.astype({x: float for x in ('Q売上高', 'Q営業益', 'Q経常益', 'Q最終益', 'Q１株益', 'Q売上営業損益率')})

# 100万円単位換算
million_col = ('Q売上高', 'Q営業益', 'Q経常益', 'Q最終益')
qr_table.loc[:, million_col] = qr_table.loc[:, million_col].apply(lambda x: x * 10 ** 6)

# 列の並び替え
qr_table = qr_table[['発表日', '決算期', 'Q期首', 'Q売上高', 'Q営業益', 'Q経常益', 'Q最終益', 'Q１株益', 'Q売上営業損益率', '会計基準', '決算期変更']]

# モバイル版のみ業績予想テーブルがない場合があるので、四半期業績の整形処理後に決算期が同期の四半期業績から会計基準を取得
# 3975で確認
if (len(fc_table) > 0) & (len(qr_table) > 0):
    if  fc_table.loc[0, '会計基準'] == "":
        for idx, end in fc_table['決算期'].iteritems():
            fc_table.loc[idx, '会計基準'] = qr_table.loc[qr_table['決算期'].apply(lambda x: x[:4]) == fc_table.loc[idx, '決算期'][:4], '会計基準'].values[0]


# ---- bs_table 財務 ---- #


# 財務実績データが無い場合、ダミーのデータフレームを作成
if len(bs_table) == 0:
    bs_table = pd.DataFrame(index=[0], columns=range(9))
    # 列名の変更
    bs_table.columns = ['発表日', '決算期', '１株純資産', '自己資本比率', '総資産', '自己資本', '剰余金', '有利子負債倍率', '会計基準']

# 全ての列項目がnullの行を除去
bs_table = bs_table[~bs_table.isnull().all(axis=1)].reset_index(drop=True)

# モバイル版のデータを結合、無い場合はスキップ
if (len(bs_table) == 0) & (len(bs_mobile) > 0):
    bs_table = pd.merge(bs_table, bs_mobile, how='outer')
    bs_table['決算期'] = mobile[bs_mobile_idx - 1]['決算期']
elif len(bs_mobile) > 0:
    bs_table['会計基準'] = bs_mobile['会計基準']

# 予想値と前期比の行を除去
bs_table['決算期'] = bs_table['決算期'].astype(str) # 決算期列が float 型になっている場合に備え str 型を明示
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
if len(pl_table) > 0:
    for idx, end in bs_table['決算期'].iteritems():
        if not end in pl_table['決算期'].values:
            bs_table = bs_table.drop(idx)

# 日付のパース、datetime.dateへの型変換、最終的に '－'  は NaT に置換される
# bs_table['決算期'] = bs_table['決算期'].apply(lambda x: datetime.strptime(x, '%Y.%m').date()) # 日付ではないので文字列のままの方がいいかも？
try:
    bs_table['発表日'] = pd.to_datetime(bs_table['発表日'], format='%Y-%m-%d')
except:
    bs_table['発表日'] = bs_table.loc[bs_table['発表日'].str.match('\d\d/\d\d/\d\d'), '発表日'].apply(lambda x: parse(x, yearfirst=True).date())
# pandasのTimestampへの型変換
bs_table['発表日'] = pd.to_datetime(bs_table['発表日'], format='%Y-%m-%d')
# bs_table['決算期'] = pd.to_datetime(bs_table['決算期'], format='%Y-%m-%d')

# 決算期の同じ年の月が通期業績と異なる場合があるので、通期業績の決算期に置換
# 決算期の変更があり、なおかつ決算期に「変」記載のない銘柄で確認 (1909)
if len(pl_table) > 0:
    for idx, end in bs_table['決算期'].iteritems():
        bs_table.loc[idx, '決算期'] = pl_table.loc[pl_table['決算期'].apply(lambda x: x[:4]) == bs_table.loc[idx, '決算期'][:4], '決算期'].values[0]

# 発表日の欠損値および異常値を通期業績の発表日に置換
if len(pl_table) > 0:
    for idx, date in bs_table['発表日'].iteritems():
        if (date != date) or (date <= pd.to_datetime('2001-01-01')):
            bs_table.loc[idx, '発表日'] = pl_table.loc[pl_table['決算期'] == bs_table.loc[idx, '決算期'], '発表日'].values[0]

# 決算期変更の欠損値を通期業績の値に置換
if len(pl_table) > 0:
    for idx, change in bs_table['決算期変更'].iteritems():
        if change == "":
            bs_table.loc[idx, '決算期変更'] = pl_table.loc[pl_table['決算期'] == bs_table.loc[idx, '決算期'], '決算期変更'].values[0]

# 数値の列の数値以外の文字列 ('－' 等) を NaN に置換
num_col = ('１株純資産', '自己資本比率', '総資産', '自己資本', '剰余金', '有利子負債倍率')
for key in num_col:
    if bs_table[key].dtypes == object:
        bs_table.loc[~bs_table[key].str.replace(r'\.|\-', "").str.isnumeric(), key] = np.nan # .str を2回も使わないといけないのはなんだか。。。
        # bs_table.loc[bs_table[key].str.contains('－'), key] = np.nan # この書き方だと '－'  以外の文字列に対応できないので不安

# 型変換
# 辞書内包表記による一括変換
bs_table = bs_table.astype({x: float for x in ('１株純資産', '自己資本比率', '総資産', '自己資本', '剰余金', '有利子負債倍率')})

# 100万円単位換算
million_col = ('総資産', '自己資本', '剰余金')
bs_table.loc[:, million_col] = bs_table.loc[:, million_col].apply(lambda x: x * 10 ** 6)

# 列の並び替え
bs_table = bs_table[['発表日', '決算期', '１株純資産', '自己資本比率', '総資産', '自己資本', '剰余金', '有利子負債倍率', '会計基準', '決算期変更']]


# In[ ]:


display(pl_table)
display(bs_table)
display(fc_table)
display(qr_table)
# display(bs_table)


# # 保存した html ファイルからテーブル属性のみ読み込み、整形

# ## 読み込み、作業用テーブルの作成

# In[ ]:


code = 9262 #7810 # 7196 # 3995 # 3975 # 3863 # 3480 # 1418 # 1408 # 1376 # 7203 # 1909


# In[ ]:


# 保存した html からテーブル属性を読み込み
tables = pd.read_html('/Users/Really/Stockyard/_kabutan_html/kabutan_{0}.html'.format(code), header=0)

# 列数が 5 以下のテーブルを削除
tables = list(filter(lambda x: len(x.columns) > 5, tables))


# In[ ]:


len(tables)


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


# In[ ]:


# 保存した モバイル用 html からテーブル属性を読み込み
mobile = pd.read_html('/Users/Really/Stockyard/_kabutan_mobile_html/kabutan_{0}.html'.format(code), header=0)


# In[ ]:


# 抽出用テーブルの作成
pl_mobile = pd.DataFrame()
fc_mobile = pd.DataFrame()
qr_mobile = pd.DataFrame()
bs_mobile = pd.DataFrame()

# 必要なテーブルの抽出
# リストを要素ごとに for で回す書き方
for idx, table in enumerate(mobile):
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
            bs_mobile_idx = idx


# In[ ]:


display(pl_table)
display(bs_table)
display(fc_table)
display(qr_table)
# display(bs_table)


# ## pl_table (tables[3]) 通期業績
# 株プロに無い項目: １株配
# In[ ]:


tables[3]


# In[ ]:


# 通期業績テーブルの抽出 (上書き)
pl_table = pd.DataFrame()
# リストを要素ごとに for で回す書き方
for idx, table in enumerate(tables):
    # 通期業績: profit and loss statement
    if len(table.columns) == 8: 
        if (table.columns[-2] == "１株配") & (pl_table.shape[1] == 0): 
            pl_table = table.copy()
            print('tables[{0}]'.format(idx))


# In[ ]:


pl_table


# In[ ]:


pl_mobile


# In[ ]:


# 全ての列項目がnullの行を除去
pl_table = pl_table[~pl_table.isnull().all(axis=1)].reset_index(drop=True)


# In[ ]:


# モバイル版の会計基準を結合、無い場合は空の列を作成
if len(pl_mobile) > 0:
    pl_table['会計基準'] = pl_mobile['会計基準']
else:
    pl_table['会計基準'] = ""


# In[ ]:


# 後で四半期業績の決算期作成に使うので予想値行削除前に保持しておく
pl_end = pl_table['決算期'][~pl_table['決算期'].str.contains('前期比')].apply(lambda x: x.split(' ')[-1])


# In[ ]:


# 予想値と前期比の行を除去
pl_table = pl_table[~((pl_table['決算期'].str.contains('予')) | (pl_table['決算期'].str.contains('前期比')))].reset_index(drop=True)


# In[ ]:


# 決算期変更列を新規作成、決算期列から決算期と決算期変更を抽出、代入
# 後で四半期業績の決算期作成に使うのでこのテーブルでは予想値行削除前に処理する
pl_table['決算期'] = pl_table['決算期'].astype(str) # 決算期列が float 型になっている場合に備え str 型を明示
pl_table['決算期変更'] = ""
for idx, end in pl_table['決算期'].iteritems():
    if '変' in end:
        pl_table.loc[idx, '決算期変更'] = '変更'
    pl_table.loc[idx, '決算期'] = end.split(' ')[-1]


# In[ ]:


# 日付のパース、datetime.dateへの型変換、最終的に '－'  は NaT に置換される
# pl_table['決算期'] = pl_table['決算期'].apply(lambda x: datetime.strptime(x, '%Y.%m').date()) # 日付ではないので文字列のままの方がいいかも？
pl_table['発表日'] = pl_table.loc[pl_table['発表日'].str.match('\d\d/\d\d/\d\d'), '発表日'].apply(lambda x: parse(x, yearfirst=True).date())
# pandasのTimestampへの型変換
pl_table['発表日'] = pd.to_datetime(pl_table['発表日'], format='%Y-%m-%d')
# pl_table['決算期'] = pd.to_datetime(pl_table['決算期'], format='%Y-%m-%d')


# In[ ]:


# 数値の列の数値以外の文字列 ('－' 等) を NaN に置換
num_col = ('売上高', '営業益', '経常益', '最終益', '１株益', '１株配')
for key in num_col:
    if pl_table[key].dtypes == object:
        pl_table.loc[~pl_table[key].str.replace(r'\.|\-', "").str.isnumeric(), key] = np.nan # .str を2回も使わないといけないのはなんだか。。。
        # pl_table.loc[pl_table[key].str.contains('－'), key] = np.nan # この書き方だと '－'  以外の文字列に対応できないので不安


# In[ ]:


# 型変換
# 辞書内包表記による一括変換
pl_table = pl_table.astype({x: float for x in ('売上高', '営業益', '経常益', '最終益', '１株益', '１株配')})


# In[ ]:


# 100万円単位換算
million_col = ('売上高', '営業益', '経常益', '最終益')
pl_table.loc[:, million_col] = pl_table.loc[:, million_col].apply(lambda x: x * 10 ** 6)


# In[ ]:


# 列の並び替え
pl_table = pl_table[['発表日', '決算期', '売上高', '営業益', '経常益', '最終益', '１株益', '１株配', '会計基準', '決算期変更']]


# In[ ]:


pl_table.dtypes


# In[ ]:


pl_table


# In[ ]:


kabupro.ix[(kabupro['証券コード'] == code) & (kabupro['会計基準'] == '米国基準') & (kabupro['決算期間'] == '通期'), 
           ['連結個別', '決算期', '期首', '売上高', '営業利益', '経常利益', '純利益', '一株当り純利益', '情報公開日 (更新日)']]


# In[ ]:


kabupro.columns


# ## fc_table (tables[4]) 業績予想

# In[ ]:


tables[4]


# In[ ]:


# 実体行
tables[4][tables[4].index % 2 == 0].reset_index(drop=True)


# In[ ]:


# 不要行
tables[4][tables[4].index % 2 != 0]


# In[ ]:


# 業績予想テーブルの抽出 (上書き)
fc_table = pd.DataFrame()
# リストを要素ごとに for で回す書き方
for idx, table in enumerate(tables):
    # 業績予想: forecast
    if len(table.columns) >= 8: 
        if (table.columns[1] == "修正日") & (fc_table.shape[1] == 0): 
            fc_table = table.copy()
            print(idx)


# In[ ]:


fc_table


# In[ ]:


fc_mobile


# In[ ]:


fc_table.columns


# In[ ]:


# 業績予想データが無い場合、ダミーのデータフレームを作成
if len(fc_table.columns) < 9:
    fc_table = pd.DataFrame(index=[0], columns=range(14))


# In[ ]:


# 列名の変更
fc_table.columns = ['会計基準', '決算期', '発表日', 
                                   '結合修正方向', '売上高修正方向', '営業益修正方向', '経常益修正方向', '最終益修正方向', '修正配当修正方向', 
                                   '予想売上高', '予想営業益', '予想経常益', '予想最終益', '予想修正配当',]


# In[ ]:


# 不要行、不要列の削除、並び替え
fc_table = fc_table.ix[fc_table.index % 2 == 0, ['会計基準', '決算期', '予想売上高', '予想営業益', '予想経常益', '予想最終益', '予想修正配当', '発表日']].reset_index(drop=True)


# In[ ]:


# 全ての列項目がnullの行を除去
fc_table = fc_table[~fc_table.isnull().all(axis=1)].reset_index(drop=True)


# In[ ]:


# モバイル版の会計基準を代入、無い場合は空値を代入
if len(fc_mobile) > 0:
    fc_table['会計基準'] = fc_mobile['会計基準']
elif len(fc_table) > 0:
    fc_table['会計基準'] = ""


# In[ ]:


# 実績は不要?
fc_table = fc_table.ix[fc_table['決算期'] != '実績'].reset_index(drop=True)


# In[ ]:


# 決算期の NaN 埋め
fc_table['決算期'] = fc_table['決算期'].fillna(method='ffill')


# In[ ]:


# 決算期変更列を新規作成、決算期列から決算期と決算期変更を抽出、代入
fc_table['決算期'] = fc_table['決算期'].astype(str) # 決算期列が float 型になっている場合に備え str 型を明示
fc_table['決算期変更'] = ""
for idx, end in fc_table['決算期'].iteritems():
    if '変' in end:
        fc_table.loc[idx, '決算期変更'] = '変更'
    fc_table.loc[idx, '決算期'] = end.split(' ')[-1]


# In[ ]:


# 日付のパース、datetime.dateへの型変換、最終的に '－'  は NaT に置換される
# fc_table['決算期'] = fc_table['決算期'].apply(lambda x: datetime.strptime(x, '%Y.%m').date()) # 日付ではないので文字列のままの方がいいかも？
fc_table['発表日'] = fc_table.loc[fc_table['発表日'].str.match('\d\d/\d\d/\d\d'), '発表日'].apply(lambda x: parse(x, yearfirst=True).date())
# pandasのTimestampへの型変換
fc_table['発表日'] = pd.to_datetime(fc_table['発表日'], format='%Y-%m-%d')
# fc_table['決算期'] = pd.to_datetime(fc_table['決算期'], format='%Y-%m-%d')


# In[ ]:


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


# In[ ]:


# 数値の列の数値以外の文字列 ('－' 等) を NaN に置換
num_col = ('予想売上高', '予想営業益', '予想経常益', '予想最終益', '予想修正配当')
for key in num_col:
    if fc_table[key].dtypes == object:
        fc_table.loc[~fc_table[key].str.replace(r'\.|\-', "").str.isnumeric(), key] = np.nan # .str を2回も使わないといけないのはなんだか。。。
        # fc_table.loc[fc_table[key].str.contains('－'), key] = np.nan # この書き方だと '－'  以外の文字列に対応できないので不安


# In[ ]:


# 型変換
# 辞書内包表記による一括変換
fc_table = fc_table.astype({x: float for x in ('予想売上高', '予想営業益', '予想経常益', '予想最終益', '予想修正配当')})


# In[ ]:


# 100万円単位換算
million_col = ('予想売上高', '予想営業益', '予想経常益', '予想最終益')
fc_table.loc[:, million_col] = fc_table.loc[:, million_col].apply(lambda x: x * 10 ** 6)


# In[ ]:


# 列の並び替え
fc_table = fc_table[['発表日', '決算期', '予想売上高', '予想営業益', '予想経常益', '予想最終益', '予想修正配当', '分割併合', '会計基準', '決算期変更']]


# In[ ]:


fc_table.dtypes


# In[ ]:


fc_table


# ## qr_table (tables[8?]) ３ヵ月業績の推移【実績】(過去5年 + 前年同期比) 累積ではなく差分
# 株プロに無い項目: 売上営業損益率 = 営業益 / 売上高?
# 不要かな？
# ちょっと株プロと見比べてみよう → １株益の値が揃わない
# 修正発表があった項目は上書きされてしまっていると思われる
# 修正の可能性を考えなければ累積の株プロよりこちらの方が使いやすいかも
# 株プロで差分を作成するべきか?
# 前年同期比はいらなそう
# In[ ]:


tables[8]


# In[ ]:


# 3ヶ月業績テーブルの抽出 (上書き)
qr_table = pd.DataFrame()
# リストを要素ごとに for で回す書き方
for idx, table in enumerate(tables):
    # 3ヶ月業績: quater
    if len(table.columns) == 8: 
        if (table.columns[-2] == "売上営業損益率") & (qr_table.shape[1] == 0): 
            qr_table = table.copy()
            print(idx)


# In[ ]:


qr_table


# In[ ]:


qr_mobile


# In[ ]:


qr_table.columns


# In[ ]:


# 全ての列項目がnullの行を除去
qr_table = qr_table[~qr_table.isnull().all(axis=1)].reset_index(drop=True)


# In[ ]:


# モバイル版の会計基準を結合
qr_table['会計基準'] = qr_mobile['会計基準']


# In[ ]:


# 予想値と前年同期比の行を除去
qr_table = qr_table[~((qr_table['決算期'].str.contains('予')) | (qr_table['決算期'].str.contains('前年同期比')))].reset_index(drop=True)


# In[ ]:


# 決算期変更列を新規作成、決算期列から決算期と決算期変更を抽出、代入
qr_table['決算期'] = qr_table['決算期'].astype(str) # 決算期列が float 型になっている場合に備え str 型を明示
qr_table['決算期変更'] = ""
for idx, end in qr_table['決算期'].iteritems():
    if '変' in end:
        qr_table.loc[idx, '決算期変更'] = '変更'
    qr_table.loc[idx, '決算期'] = end.split(' ')[-1]


# In[ ]:


# 列名の変更
qr_table.columns = ['Q期首', 'Q売上高', 'Q営業益', 'Q経常益', 'Q最終益', 'Q１株益', 'Q売上営業損益率', '発表日', '会計基準', '決算期変更']


# In[ ]:


# 日付のパース、datetime.dateへの型変換、最終的に '－'  は NaT に置換される
qr_table['Q期首'] = qr_table.loc[qr_table['Q期首'].str.match('\d\d.\d\d-\d\d'), 'Q期首'].apply(lambda x: 
                                                                                               parse(x.replace('-', '.'), yearfirst=True).date())
qr_table['発表日'] = qr_table.loc[qr_table['発表日'].str.match('\d\d/\d\d/\d\d'), '発表日'].apply(lambda x: parse(x, yearfirst=True).date())
# pandasのTimestampへの型変換
qr_table['Q期首'] = pd.to_datetime(qr_table['Q期首'], format='%Y-%m-%d')
qr_table['発表日'] = pd.to_datetime(qr_table['発表日'], format='%Y-%m-%d')


# In[ ]:


# 通期業績の決算期を参照して決算期列を追加
# 通期業績の予想値削除前に別名でキープした決算期シリーズを利用
for start_idx, start in qr_table['Q期首'].iteritems():
    for end in pl_end:
        if start < pd.to_datetime(end, format='%Y.%m') + offsets.MonthEnd():
            qr_table.loc[start_idx, '決算期'] = end
            break


# In[ ]:


# 数値の列の数値以外の文字列 ('－' 等) を NaN に置換
num_col = ('Q売上高', 'Q営業益', 'Q経常益', 'Q最終益', 'Q１株益', 'Q売上営業損益率')
for key in num_col:
    if qr_table[key].dtypes == object:
        qr_table.loc[~qr_table[key].str.replace(r'\.|\-', "").str.isnumeric(), key] = np.nan # .str を2回も使わないといけないのはなんだか。。。
        # qr_table.loc[qr_table[key].str.contains('－'), key] = np.nan # この書き方だと '－'  以外の文字列に対応できないので不安


# In[ ]:


# 型変換
# 辞書内包表記による一括変換
qr_table = qr_table.astype({x: float for x in ('Q売上高', 'Q営業益', 'Q経常益', 'Q最終益', 'Q１株益', 'Q売上営業損益率')})


# In[ ]:


# 100万円単位換算
million_col = ('Q売上高', 'Q営業益', 'Q経常益', 'Q最終益')
qr_table.loc[:, million_col] = qr_table.loc[:, million_col].apply(lambda x: x * 10 ** 6)


# In[ ]:


# 列の並び替え
qr_table = qr_table[['発表日', '決算期', 'Q期首', 'Q売上高', 'Q営業益', 'Q経常益', 'Q最終益', 'Q１株益', 'Q売上営業損益率', '会計基準', '決算期変更']]


# In[ ]:


# モバイル版のみ業績予想テーブルがない場合があるので、四半期業績の整形処理後に決算期が同期の四半期業績から会計基準を取得
# 3975で確認
if (len(fc_table) > 0) & (len(qr_table) > 0):
    if  fc_table.loc[0, '会計基準'] == "":
        for idx, end in fc_table['決算期'].iteritems():
            fc_table.loc[idx, '会計基準'] = qr_table.loc[qr_table['決算期'].apply(lambda x: x[:4]) == fc_table.loc[idx, '決算期'][:4], '会計基準'].values[0]


# In[ ]:


qr_table.dtypes


# In[ ]:


qr_table


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


# ## bs_table (tables[9?]) 財務 【実績】
# 株プロに無い項目: 自己資本比率, 自己資本, 剰余金, 有利子負債倍率

# 2000年以前の財務実績の発表日は全体的に信用できない
# 1998年は何らかの日付で固定 or 捨て、1999年と2000年は通期業績の発表日と同じにする
# 期間は株プロよりこちらの方が長い
# 修正発表があった項目は上書きされてしまっていると思われる
# In[ ]:


tables[9]


# In[ ]:


# 財務テーブルの抽出 (上書き)
bs_table = pd.DataFrame()
# リストを要素ごとに for で回す書き方
for idx, table in enumerate(tables):
    # 財務: balance sheet
    if len(table.columns) == 8: 
        if (table.columns[1] == "１株純資産") & (bs_table.shape[1] == 0): 
            bs_table = table
            print('tables[{0}]'.format(idx))


# In[ ]:


bs_table


# In[ ]:


bs_mobile


# In[ ]:


bs_table.columns


# In[ ]:


# 財務実績データが無い場合、ダミーのデータフレームを作成
if len(bs_table) == 0:
    bs_table = pd.DataFrame(index=[0], columns=range(9))
    # 列名の変更
    bs_table.columns = ['発表日', '決算期', '１株純資産', '自己資本比率', '総資産', '自己資本', '剰余金', '有利子負債倍率', '会計基準']


# In[ ]:


# 全ての列項目がnullの行を除去
bs_table = bs_table[~bs_table.isnull().all(axis=1)].reset_index(drop=True)


# In[ ]:


# モバイル版のデータを結合、無い場合はスキップ
if (len(bs_table) == 0) & (len(bs_mobile) > 0):
    bs_table = pd.merge(bs_table, bs_mobile, how='outer')
    bs_table['決算期'] = mobile[bs_mobile_idx - 1]['決算期']
elif len(bs_mobile) > 0:
    bs_table['会計基準'] = bs_mobile['会計基準']


# In[ ]:


# 予想値と前期比の行を除去
bs_table['決算期'] = bs_table['決算期'].astype(str) # 決算期列が float 型になっている場合に備え str 型を明示
bs_table = bs_table[~((bs_table['決算期'].str.contains('予')) | (bs_table['決算期'].str.contains('前期比')))].reset_index(drop=True)


# In[ ]:


# 決算期変更列を新規作成、決算期列から決算期と決算期変更を抽出、代入
bs_table['決算期'] = bs_table['決算期'].astype(str) # 決算期列が float 型になっている場合に備え str 型を明示
bs_table['決算期変更'] = ""
for idx, end in bs_table['決算期'].iteritems():
    if '変' in end:
        bs_table.loc[idx, '決算期変更'] = '変更'
    bs_table.loc[idx, '決算期'] = end.split(' ')[-1]


# In[ ]:


# 決算期が 'yyyy.mm' 表記ではない行は確定決算前と思われるので削除
bs_table = bs_table[bs_table['決算期'].str.contains('\d\d\d\d.\d\d')].reset_index(drop=True)


# In[ ]:


# 決算期が 1998.mm のデータは他のテーブルには無く、発表日も不自然なので行ごと削除
# bs_table = bs_table[~bs_table['決算期'].str.contains('1998.\d\d')].reset_index(drop=True)


# In[ ]:


# 通期業績には無い期間の行を削除
if len(pl_table) > 0:
    for idx, end in bs_table['決算期'].iteritems():
        if not end in pl_table['決算期'].values:
            bs_table = bs_table.drop(idx)


# In[ ]:


# 日付のパース、datetime.dateへの型変換、最終的に '－'  は NaT に置換される
# bs_table['決算期'] = bs_table['決算期'].apply(lambda x: datetime.strptime(x, '%Y.%m').date()) # 日付ではないので文字列のままの方がいいかも？
try:
    bs_table['発表日'] = pd.to_datetime(bs_table['発表日'], format='%Y-%m-%d')
except:
    bs_table['発表日'] = bs_table.loc[bs_table['発表日'].str.match('\d\d/\d\d/\d\d'), '発表日'].apply(lambda x: parse(x, yearfirst=True).date())
# pandasのTimestampへの型変換
bs_table['発表日'] = pd.to_datetime(bs_table['発表日'], format='%Y-%m-%d')
# bs_table['決算期'] = pd.to_datetime(bs_table['決算期'], format='%Y-%m-%d')


# In[ ]:


# 決算期の同じ年の月が通期業績と異なる場合があるので、通期業績の決算期に置換
# 決算期の変更があり、なおかつ決算期に「変」記載のない銘柄で確認 (1909)
if len(pl_table) > 0:
    for idx, end in bs_table['決算期'].iteritems():
        bs_table.loc[idx, '決算期'] = pl_table.loc[pl_table['決算期'].apply(lambda x: x[:4]) == bs_table.loc[idx, '決算期'][:4], '決算期'].values[0]


# In[ ]:


# 発表日の欠損値および異常値を通期業績の発表日に置換
if len(pl_table) > 0:
    for idx, date in bs_table['発表日'].iteritems():
        if (date != date) or (date <= pd.to_datetime('2001-01-01')):
            bs_table.loc[idx, '発表日'] = pl_table.loc[pl_table['決算期'] == bs_table.loc[idx, '決算期'], '発表日'].values[0]


# In[ ]:


# 決算期変更の欠損値を通期業績の値に置換
if len(pl_table) > 0:
    for idx, change in bs_table['決算期変更'].iteritems():
        if change == "":
            bs_table.loc[idx, '決算期変更'] = pl_table.loc[pl_table['決算期'] == bs_table.loc[idx, '決算期'], '決算期変更'].values[0]


# In[ ]:


# 数値の列の数値以外の文字列 ('－' 等) を NaN に置換
num_col = ('１株純資産', '自己資本比率', '総資産', '自己資本', '剰余金', '有利子負債倍率')
for key in num_col:
    if bs_table[key].dtypes == object:
        bs_table.loc[~bs_table[key].str.replace(r'\.|\-', "").str.isnumeric(), key] = np.nan # .str を2回も使わないといけないのはなんだか。。。
        # bs_table.loc[bs_table[key].str.contains('－'), key] = np.nan # この書き方だと '－'  以外の文字列に対応できないので不安


# In[ ]:


# 型変換
# 辞書内包表記による一括変換
bs_table = bs_table.astype({x: float for x in ('１株純資産', '自己資本比率', '総資産', '自己資本', '剰余金', '有利子負債倍率')})


# In[ ]:


# 100万円単位換算
million_col = ('総資産', '自己資本', '剰余金')
bs_table.loc[:, million_col] = bs_table.loc[:, million_col].apply(lambda x: x * 10 ** 6)


# In[ ]:


# 列の並び替え
bs_table = bs_table[['発表日', '決算期', '１株純資産', '自己資本比率', '総資産', '自己資本', '剰余金', '有利子負債倍率', '会計基準', '決算期変更']]


# In[ ]:


bs_table.dtypes


# In[ ]:


bs_table


# In[ ]:


bs_table


# In[ ]:


kabupro.ix[(kabupro['証券コード'] == code) & (kabupro['会計基準'] == '米国基準') & (kabupro['決算期間'] == '通期'), 
           ['連結個別', '決算期', '期首', '一株当り純資産', '総資産', '情報公開日 (更新日)']]


# In[ ]:


# 比較参照用
kabupro.columns


# ## tables[2] 銘柄概要

# In[ ]:


tables[2]


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


# # (準備) 単一銘柄の決算ページの取得

# In[ ]:


# 個別銘柄の決算ページを開く
code = 1301 # トヨタ

print('Navigating...', file=sys.stderr)
browser.open('https://kabutan.jp/stock/finance?code={0}&mode=k'.format(code))

# 決算ページにいることを確認する
assert '決算' in browser.parsed.title.string

print(browser.select('.kobetsu_data_table1_meigara')[0].text.strip())


# In[ ]:


# 個別銘柄の決算ページを開く
code = 1301 # トヨタ

print('Navigating...', file=sys.stderr)
browser.open('https://s.kabutan.jp/stock/finance?code={0}&mode=k'.format(code))

# 決算ページにいることを確認する
assert '決算' in browser.parsed.title.string

# print(browser.select('.kobetsu_data_table1_meigara')[0].text.strip())


# ## html 全体の取得と保存

# In[ ]:


kessan_html = browser.find()


# In[ ]:


kessan_html


# In[ ]:


tables = pd.read_html(str(kessan_html), header=0)


# In[ ]:


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

# In[ ]:


# 保存した html からテーブル属性を読み込み
tables = pd.read_html('/Users/Really/Stockyard/_kabutan_html/kabutan_{0}.html'.format(code), header=0)

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

