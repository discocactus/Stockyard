
# coding: utf-8

# # import

# In[ ]:


import pandas as pd
import os
import sys
import time
import re
import pathlib
from selenium import webdriver
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.support.select import Select
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.chrome.options import Options
from retry import retry


# In[ ]:


# pandas の最大表示列数を設定 (max_rows で表示行数の設定も可能)
pd.set_option('display.max_columns', 30)


# In[ ]:


csv_path = "D:\stockyard\investing"


# # 'Show more' ボタン(Java Script)を手動で展開、名前を付けてページを保存

# In[ ]:


# 保存した PC用 html からテーブルを読み込んでみる
us_tables = pd.read_html(r"D:\stockyard\investing\United States Federal Reserve Interest Rate Decision.html", header=0)


# In[ ]:


for idx in range(len(us_tables)):
    print(idx)
    display(us_tables[idx])


# In[ ]:


# 保存した PC用 html からテーブルを読み込んでみる
ecb_tables = pd.read_html(r"D:\stockyard\investing\European Interest Rate Decision.html", header=0)


# In[ ]:


for idx in range(len(ecb_tables)):
    print(idx)
    display(ecb_tables[idx])


# In[ ]:


# 保存した PC用 html からテーブルを読み込んでみる
uk_tables = pd.read_html(r"D:\stockyard\investing\United Kingdom Interest Rate Decision.html", header=0)


# In[ ]:


for idx in range(len(uk_tables)):
    print(idx)
    display(uk_tables[idx])


# In[ ]:


# 保存した PC用 html からテーブルを読み込んでみる
jp_tables = pd.read_html(r"D:\stockyard\investing\Japan Interest Rate Decision.html", header=0)


# In[ ]:


for idx in range(len(jp_tables)):
    print(idx)
    display(jp_tables[idx])


# In[ ]:


# 保存した PC用 html からテーブルを読み込んでみる
aud_tables = pd.read_html(r"D:\stockyard\investing\Australia Interest Rate Decision.html", header=0)


# In[ ]:


for idx in range(len(aud_tables)):
    print(idx)
    display(aud_tables[idx])


# # selenium

# ## 連続処理スクリプト

# In[ ]:


# 2018-04-01 last ver.
# TODO エラーで止まった場合に chrome を終了して終わるように
%%writefile investing_com_script.py


# import
import pandas as pd
import os
import sys
import time
import re
from selenium import webdriver
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.support.select import Select
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.chrome.options import Options
from retry import retry

# パスの設定
# csv_path = "D:\stockyard\investing"
# list_file = "D:\stockyard\_csv\investing_page_list.csv"
csv_path = '/home/hideshi_honma/stockyard/investing'
list_file = '/home/hideshi_honma/stockyard/_csv/investing_page_list.csv'

# リストファイルの準備
if os.path.exists(list_file):
    page_list = pd.read_csv(list_file, index_col=0)
else:
    page_list = pd.DataFrame()

# ボタンをクリックする関数
# 読み込みのタイミング遅延によるエラーを回避するには効果ないかも?
@retry(tries=5, delay=1, backoff=2)
def click_btn(btn):
    btn.click()

# ヘッドレス Chrome の WebDriver オブジェクトを作成する
options = Options()
options.add_argument('--headless')
options.add_argument('--disable-gpu')
options.add_argument('--lang=ja')
driver = webdriver.Chrome(chrome_options=options)
wait = WebDriverWait(driver, 10)

# トップページを開く (ページ遷移によりログインポップアップを閉じるため)
driver.get('https://www.investing.com/')
print('Waiting for top page to be loaded...', file=sys.stderr)
time.sleep(2)
# タイトルに'Investing.com'が含まれていることを確認する
assert 'Investing.com' in driver.title

# テーブルの取得
for num in range(1311, 1801):
    url = 'https://www.investing.com/economic-calendar/-{0}'.format(num)
    driver.get(url)
    print('Waiting for contents {0} to be loaded...'.format(num), file=sys.stderr)
    # driver.refresh()
    time.sleep(2)

    page_url = driver.current_url
    print(page_url)
    # ページにテーブルが無ければスキップする
    # if 'No results found' not in driver.page_source:
    if not page_url.startswith('https://www.investing.com/economic-calendar/-'):
        page_title = driver.title
        if page_title == "":
            driver.refresh()
            time.sleep(2)
        print(page_title)
        page_num = re.search(r'[0-9]+$', page_url).group()
        print('page_num: {0}'.format(page_num))
        
        # ポップアップ対策
        result = driver.page_source
        if 'userDataPopup' in driver.page_source:
            if driver.find_element_by_css_selector('a.newButton:nth-child(1)').is_displayed():
                driver.find_element_by_css_selector('a.newButton:nth-child(1)').click()
                print('PopUp clicked')
                time.sleep(1)
                driver.save_screenshot('screenshot_clicked.png')
                with open('inv_{0}_clicked.html'.format(page_num), 'w', encoding='utf-8') as write_html:
                    write_html.write(str(result))
                driver.refresh()
                time.sleep(2)
        driver.save_screenshot('inv_screenshot.png')
        #with open('inv_{0}.html'.format(page_num), 'w', encoding='utf-8') as write_html:
        with open('inv_last.html', 'w', encoding='utf-8') as write_html:
            write_html.write(str(result))
 
        # 'Show more' ボタンがあれば押して続きを読み込む
        if 'showMoreReplies' in driver.page_source:
            show_more = driver.find_element_by_class_name('showMoreReplies')
            count = 1
            df_len = 0
            while True:
                if show_more.is_displayed():
                    wait.until(EC.element_to_be_clickable((By.CLASS_NAME, 'showMoreReplies')))
                    try:
                        click_btn(show_more)
                        print('page {0} show_more: {1}'.format(page_num, count))
                        count += 1
                        time.sleep(1)
                    except Exception as e:
                        print(e)
                        result = driver.page_source
                        with open('inv_clickFailed_{0}.html'.format(page_num), 'w', encoding='utf-8') as write_html:
                            write_html.write(str(result))
                        time.sleep(2)
                    # 存在しないはずのボタンを無限に押し続けたことがあるためその対策
                    # 実際に設定数以上のページがある可能性もあるので注意
                    if count > 210:
                        print('too much clicked: {0}'.format(page_num))
                        result = driver.page_source
                        with open('inv_tooMuchClicked_{0}.html'.format(page_num), 'w', encoding='utf-8') as write_html:
                            write_html.write(str(result))
                        break
                else:
                    break

        # テーブルを取得、保存
        df = pd.read_html(driver.page_source, header=0)[0]
        df.to_csv('{0}/{1}_{2}.csv'.format(csv_path, page_num.zfill(4), re.sub('[ /.]', '_', page_title)),
                 index=False)
        print('length: {0}'.format(len(df)))
        
        # ページリストに情報を追加
        page_info = pd.DataFrame({'title': page_title, 'url': page_url}, index=[page_num])
        page_list = page_list.append(page_info)
        page_list = page_list.drop_duplicates()
        page_list.to_csv(list_file)

    else:
        print('No data in this page.')

# driver を終了
driver.quit()

print('Finished.')


# In[ ]:


# こけた。その後、動作未確認
get_ipython().system('python investing_com_script.py')


# ## スクリプトを分割して動作確認、読み込み失敗分の再読み込みも

# In[ ]:


# import
import pandas as pd
import os
import sys
import time
import re
from selenium import webdriver
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.support.select import Select
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.chrome.options import Options
from retry import retry


# In[ ]:


# パスの設定
csv_path = "D:\stockyard\investing"
list_file = "D:\stockyard\_csv\investing_page_list.csv"
# csv_path = '/home/hideshi_honma/stockyard/investing'
# list_file = '/home/hideshi_honma/stockyard/_csv/investing_page_list.csv'


# In[ ]:


# リストファイルの準備
if os.path.exists(list_file):
    page_list = pd.read_csv(list_file, index_col=0)
else:
    page_list = pd.DataFrame()


# In[ ]:


page_list


# In[ ]:


# ボタンをクリックする関数
@retry(tries=5, delay=1, backoff=2)
def click_btn(btn):
    btn.click()


# In[ ]:


# ヘッドあり Chrome の WebDriver オブジェクトを作成する
driver = webdriver.Chrome()


# In[ ]:


# ヘッドレス Chrome の WebDriver オブジェクトを作成する
options = Options()
options.add_argument('--headless')
options.add_argument('--disable-gpu')
options.add_argument('--lang=ja')
driver = webdriver.Chrome(chrome_options=options)
wait = WebDriverWait(driver, 10)


# In[ ]:


# トップページを開く (ページ遷移によりログインポップアップを閉じるため)
driver.get('https://www.investing.com/')
print('Waiting for top page to be loaded...', file=sys.stderr)
time.sleep(2)
# タイトルに'Investing.com'が含まれていることを確認する
assert 'Investing.com' in driver.title


# In[ ]:


# 再読み込み用、失敗分.html のパスを指定
failed_dir = pathlib.Path("D:\stockyard\investing_html")
failed_dir


# In[ ]:


# 再読み込み用のページ番号リストを作成
failed = []
for file in failed_dir.iterdir():
    failed.append(int(re.findall('([0-9]+).html', str(file))[0]))
failed


# In[ ]:


# 1340、リストになぜか名前だけ抜けてる
failed.append(1340)
failed


# In[ ]:


# このページのみ当初の最大クリック回数210以上あった
# 週次 * 30年分 * 1回クリック6行で、260回前後と思われる
failed = [1652,]


# In[ ]:


# テーブルの取得
#for num in range(1311, 1801): # ページ番号指定
for num in failed: # 失敗分再読み込み用
    url = 'https://www.investing.com/economic-calendar/-{0}'.format(num)
    driver.get(url)
    print('Waiting for contents {0} to be loaded...'.format(num), file=sys.stderr)
    # driver.refresh()
    time.sleep(2)

    page_url = driver.current_url
    print(page_url)
    # ページにテーブルが無ければスキップする
    # if 'No results found' not in driver.page_source:
    if not page_url.startswith('https://www.investing.com/economic-calendar/-'):
        page_title = driver.title
        if page_title == "":
            driver.refresh()
            time.sleep(2)
        print(page_title)
        page_num = re.search(r'[0-9]+$', page_url).group()
        print('page_num: {0}'.format(page_num))
        
        # ポップアップ対策
        result = driver.page_source
        if 'userDataPopup' in driver.page_source:
            if driver.find_element_by_css_selector('a.newButton:nth-child(1)').is_displayed():
                driver.find_element_by_css_selector('a.newButton:nth-child(1)').click()
                print('PopUp clicked')
                time.sleep(1)
                driver.save_screenshot('screenshot_clicked.png')
                with open('inv_{0}_clicked.html'.format(page_num), 'w', encoding='utf-8') as write_html:
                    write_html.write(str(result))
                driver.refresh()
                time.sleep(2)
        driver.save_screenshot('inv_screenshot.png')
        #with open('inv_{0}.html'.format(page_num), 'w', encoding='utf-8') as write_html:
        with open('inv_last.html', 'w', encoding='utf-8') as write_html:
            write_html.write(str(result))
 
        # 'Show more' ボタンがあれば押して続きを読み込む
        if 'showMoreReplies' in driver.page_source:
            show_more = driver.find_element_by_class_name('showMoreReplies')
            count = 1
            df_len = 0
            while True:
                if show_more.is_displayed():
                    wait.until(EC.element_to_be_clickable((By.CLASS_NAME, 'showMoreReplies')))
                    try:
                        click_btn(show_more)
                        print('page {0} show_more: {1}'.format(page_num, count))
                        count += 1
                        time.sleep(1)
                    except Exception as e:
                        print(e)
                        result = driver.page_source
                        with open('inv_clickFailed_{0}.html'.format(page_num), 'w', encoding='utf-8') as write_html:
                            write_html.write(str(result))
                        time.sleep(2)
                    # 存在しないはずのボタンを無限に押し続けたことがあるためその対策
                    # 実際に設定数以上のページがある可能性もあるので注意
                    if count > 300: # 210
                        print('too much clicked: {0}'.format(page_num))
                        result = driver.page_source
                        with open('inv_tooMuchClicked_{0}.html'.format(page_num), 'w', encoding='utf-8') as write_html:
                            write_html.write(str(result))
                        break
                else:
                    break

        # テーブルを取得、保存
        df = pd.read_html(driver.page_source, header=0)[0]
        df.to_csv('{0}/{1}_{2}.csv'.format(csv_path, page_num.zfill(4), re.sub('[ /.]', '_', page_title)),
                 index=False)
        print('length: {0}'.format(len(df)))
        
        # ページリストに情報を追加
        page_info = pd.DataFrame({'title': page_title, 'url': page_url}, index=[page_num])
        page_list = page_list.append(page_info)
        page_list = page_list.drop_duplicates()
        page_list.to_csv(list_file)

    else:
        print('No data in this page.')


# In[ ]:


# driver を終了
driver.quit()

print('Finished.')


# In[ ]:


page_list


# In[ ]:


pd.read_csv(list_file, index_col=0)


# # GCEからアクセスした時に出るポップアップ対策

# In[ ]:


# ChromeのWebDriverオブジェクトを作成する
driver = webdriver.Chrome()


# In[ ]:


driver.get("D:\downloads\inv_49.html")


# In[ ]:


'showMoreReplies' in driver.page_source


# In[ ]:


'userDataPopup' in driver.page_source # How would you best describe yourself?


# In[ ]:


# a.newButton:nth-child(1) # Individual Investor
# a.orange:nth-child(2) # Institutional Investor
# .buttons > a:nth-child(3) # Financial Advisor
# a.newButton:nth-child(4) # Active Trader


# In[ ]:


driver.find_element_by_css_selector('a.newButton:nth-child(1)')


# In[ ]:


driver.find_element_by_css_selector('a.newButton:nth-child(1)').is_displayed()


# In[ ]:


driver.find_element_by_css_selector('a.newButton:nth-child(1)').click()


# In[ ]:


if 'userDataPopup' in driver.page_source:
    if driver.find_element_by_css_selector('a.newButton:nth-child(1)').is_displayed():
        driver.find_element_by_css_selector('a.newButton:nth-child(1)').click()
        time.sleep(2)


# In[ ]:


driver.refresh()


# In[ ]:


driver.quit()

