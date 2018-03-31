
# coding: utf-8

# # import

# In[ ]:


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

# ## step by step

# In[ ]:


# パスの設定
csv_path = "D:\stockyard\investing"
list_file = "D:\stockyard\_csv\investing_page_list.csv"


# In[ ]:


# リストファイルの準備
if os.path.exists(list_file):
    page_list = pd.read_csv(list_file, index_col=0)
else:
    page_list = pd.DataFrame()


# In[ ]:


# ChromeのWebDriverオブジェクトを作成する
driver = webdriver.Chrome()


# In[ ]:


# トップページを開く (ページ遷移によりログインポップアップを閉じるため)
driver.get('https://www.investing.com/')
print('Waiting for contents to be loaded...', file=sys.stderr)
time.sleep(1)
# タイトルに'Investing.com'が含まれていることを確認する
assert 'Investing.com' in driver.title


# In[ ]:


url = 'https://www.investing.com/economic-calendar/-32'


# In[ ]:


# 開く
driver.get(url)
print('Waiting for contents to be loaded...', file=sys.stderr)
time.sleep(2)  # 2秒間待つ


# In[ ]:


'No results found' not in driver.page_source


# In[ ]:


page_title = driver.title
print(page_title)


# In[ ]:


page_url = driver.current_url
print(page_url)


# In[ ]:


page_num = re.search(r'[0-9]+$', page_url).group()
print(page_num)


# In[ ]:


'showMoreReplies' in driver.page_source


# In[ ]:


@retry(tries=5, delay=1, backoff=2)
def click_btn(btn):
    btn.click()


# In[ ]:


wait = WebDriverWait(driver, 10)


# In[ ]:


show_more = driver.find_element_by_class_name('showMoreReplies')
while show_more.is_displayed():
    wait.until(EC.element_to_be_clickable((By.CLASS_NAME, 'showMoreReplies')))
    click_btn(show_more)
    time.sleep(1)


# In[ ]:


driver.refresh()


# In[ ]:


# テーブルを取得、保存
df = pd.read_html(driver.page_source, header=0)[0]
df.to_csv('{0}\{1}_{2}.csv'.format(csv_path, page_num.zfill(4), re.sub(' ', '_', page_title)),
         index=False)
print('length: {0}'.format(len(df)))


# In[ ]:


df.to_csv('{0}/{1}_{2}.csv'.format(csv_path, page_num.zfill(4), 'test'),
         index=False)


# In[ ]:


df


# In[ ]:


pd.read_csv('{0}\{1}_{2}.csv'.format(csv_path, page_num.zfill(4), re.sub(' ', '_', page_title)))


# In[ ]:


# ページリストに情報を追加
page_info = pd.DataFrame({'title': page_title, 'url': page_url}, index=[page_num])
page_list = page_list.append(page_info)


# In[ ]:


page_list


# In[ ]:


page_list.drop_duplicates()


# In[ ]:


page_list.to_csv('{0}\page_list.csv'.format(csv_path))


# In[ ]:


pd.read_csv(list_file, index_col=0)


# In[ ]:


driver.refresh()


# In[ ]:


driver.quit()


# ## 連続処理スクリプト

# In[ ]:


get_ipython().run_cell_magic('writefile', 'investing_com_script.py', '\n# import\nimport pandas as pd\nimport os\nimport sys\nimport time\nimport re\nfrom selenium import webdriver\nfrom selenium.webdriver.common.keys import Keys\nfrom selenium.webdriver.support.select import Select\nfrom selenium.webdriver.support.ui import WebDriverWait\nfrom selenium.webdriver.common.by import By\nfrom selenium.webdriver.support import expected_conditions as EC\nfrom selenium.webdriver.chrome.options import Options\nfrom retry import retry\n\n# パスの設定\ncsv_path = "D:\\stockyard\\investing"\nlist_file = "D:\\stockyard\\_csv\\investing_page_list.csv"\n# csv_path = \'/home/hideshi_honma/stockyard/investing\'\n# list_file = \'/home/hideshi_honma/stockyard/_csv/investing_page_list.csv\'\n\n# リストファイルの準備\nif os.path.exists(list_file):\n    page_list = pd.read_csv(list_file, index_col=0)\nelse:\n    page_list = pd.DataFrame()\n\n# ボタンをクリックする関数\n@retry(tries=5, delay=1, backoff=2)\ndef click_btn(btn):\n    btn.click()\n\n# ヘッドレス Chrome の WebDriver オブジェクトを作成する\noptions = Options()\noptions.add_argument(\'--headless\')\ndriver = webdriver.Chrome(chrome_options=options)\nwait = WebDriverWait(driver, 10)\n\n# トップページを開く (ページ遷移によりログインポップアップを閉じるため)\ndriver.get(\'https://www.investing.com/\')\nprint(\'Waiting for contents to be loaded...\', file=sys.stderr)\ntime.sleep(1)\n# タイトルに\'Investing.com\'が含まれていることを確認する\nassert \'Investing.com\' in driver.title\n\n# テーブルの取得\nfor num in range(31, 41):\n    url = \'https://www.investing.com/economic-calendar/-{0}\'.format(num)\n    driver.get(url)\n    print(\'Waiting for contents to be loaded...\', file=sys.stderr)\n    time.sleep(1)\n\n    # ページにテーブルが無ければスキップする\n    if \'No results found\' not in driver.page_source:\n        page_title = driver.title\n        print(page_title)\n        page_url = driver.current_url\n        print(page_url)\n        page_num = re.search(r\'[0-9]+$\', page_url).group()\n        print(\'page_num: {0}\'.format(page_num))\n\n        # \'Show more\' ボタンがあれば押して続きを読み込む\n        if \'showMoreReplies\' in driver.page_source:\n            show_more = driver.find_element_by_class_name(\'showMoreReplies\')\n            while show_more.is_displayed():\n                wait.until(EC.element_to_be_clickable((By.CLASS_NAME, \'showMoreReplies\')))\n                click_btn(show_more)\n                time.sleep(1)\n\n        # テーブルを取得、保存\n        df = pd.read_html(driver.page_source, header=0)[0]\n        df.to_csv(\'{0}/{1}_{2}.csv\'.format(csv_path, page_num.zfill(4), re.sub(\' \', \'_\', page_title)),\n                 index=False)\n        print(\'length: {0}\'.format(len(df)))\n        \n        # ページリストに情報を追加\n        page_info = pd.DataFrame({\'title\': page_title, \'url\': page_url}, index=[page_num])\n        page_list = page_list.append(page_info)\n        page_list.to_csv(list_file)\n\n# driver を終了\ndriver.quit()\n\nprint(\'Finished.\')')


# In[ ]:


get_ipython().system('python investing_com_script.py')


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

