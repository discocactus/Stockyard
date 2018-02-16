
# coding: utf-8

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


# # ログイン。ログイン情報が出力に表示されてしまうのでそのままでGitHubに上げちゃダメ！ログインを済ませたら必ずすぐにクリア！

# In[ ]:


# sign-in
# 認証の情報
RS_ID = input('Name?')
RS_PASSWORD = input('Password?')

# RoboBrowserオブジェクトを作成する
browser = RoboBrowser(
    parser='html.parser', # Beatiful Soupで使用するパーサーを指定
    # Cookieが使用できないと表示されてログインできない問題を回避するため
    # 通常のブラウザーのUser-Agent(ここではFirefoxのもの)を使う
    user_agent='Mozilla/5.0 (Macintosh; Intel Mac OS X 10.10; rv:45.0) Gecko/20100101 Firefox/45.0')
    # user_agent='Mozilla/5.0 (iPhone; CPU iPhone OS 9_3_5 like Mac OS X) AppleWebKit/601.1.46 (KHTML, like Gecko) Version/9.0 Mobile/13G36 Safari/601.1')

# ログインページを開く
print('Navigating...', file=sys.stderr)
browser.open('https://www.rakuten-sec.co.jp/ITS/V_ACT_Login.html')

# 'ログイン | 楽天証券' ページにいることを確認する
assert 'ログイン | 楽天証券' in browser.parsed.title.string

# name="signIn" というサインインフォームを埋める。
# フォームのname属性の値はブラウザーの開発者ツールで確認できる。
form = browser.get_form(attrs={'name': 'loginform'})
form['loginid'] = RS_ID
form['passwd'] = RS_PASSWORD

# フォームを送信する。
# 正常にログインするにはRefererヘッダーとAccept-Languageヘッダーが必要な場合がある。
print('Signing in...', file=sys.stderr)
browser.submit_form(form)

# ログインに失敗する場合は、次の行のコメントを外してHTMLのソースを確認すると良い。
# print(browser.parsed.prettify())
print(browser.select('.client-name')[0].text.strip())


# In[ ]:


link_to_next = browser.get_link('国内株式') #「次へ」というテキストを持つリンクを取得する。
browser.follow_link(link_to_next)


# In[ ]:


link_to_next = browser.get_link('スーパースクリーナー') #「次へ」というテキストを持つリンクを取得する。
browser.follow_link(link_to_next)


# In[ ]:


print(browser.parsed.prettify())


# In[ ]:


@retry(tries=5, delay=1, backoff=2)
def get_html(url):
    browser.open(url)
    assert '決算' in browser.parsed.title.string # 決算ページにいることを確認する
    stock_name = browser.select('.kobetsu_data_table1_meigara')[0].text.strip()  # モバイルサイトの場合はコメントアウト
    print('{0}: {1}'.format(code, stock_name))  # モバイルサイトの場合はコメントアウト
    result = browser.find()
    
    return result


# In[ ]:


# 個別銘柄の決算ページを開く
code = 1301 # トヨタ

print('Navigating...', file=sys.stderr)
browser.open('https://kabutan.jp/stock/finance?code={0}&mode=k'.format(code))

# 決算ページにいることを確認する
assert '決算' in browser.parsed.title.string

print(browser.select('.kobetsu_data_table1_meigara')[0].text.strip())


# In[ ]:


https://member.rakuten-sec.co.jp/app/info_jp_prc_stock.do;BV_SessionID=A3145518759A80D1E6A514FC5259B5E2.42ad2c3e?eventType=init&infoInit=1&contentId=7&type=&sub_type=&local=&dscrCd=72030&marketCd=1&gmn=J&smn=01&lmn=01&fmn=01


# In[ ]:


kessan_html = browser.find()


# In[ ]:


tables = pd.read_html(str(kessan_html), header=0)

