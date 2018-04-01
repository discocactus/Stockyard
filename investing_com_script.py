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
# retry ではタイミングのずれによるエラーを回避できなさそうなので意味ないかも
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
for num in range(82, 101):
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
                time.sleep(2)
                driver.save_screenshot('screenshot_clicked.png')
                with open('inv_{0}_clicked.html'.format(page_num), 'w', encoding='utf-8') as write_html:
                    write_html.write(str(result))
        driver.save_screenshot('inv_screenshot.png')
        #with open('inv_{0}.html'.format(page_num), 'w', encoding='utf-8') as write_html:
        with open('inv_last.html', 'w', encoding='utf-8') as write_html:
            write_html.write(str(result))
 
        # 'Show more' ボタンがあれば押して続きを読み込む
        if 'showMoreReplies' in driver.page_source:
            show_more = driver.find_element_by_class_name('showMoreReplies')
            count = 1
            while True:
                if show_more.is_displayed():
                    wait.until(EC.element_to_be_clickable((By.CLASS_NAME, 'showMoreReplies')))
                    click_btn(show_more) # retry ではタイミングのずれによるエラーを回避できなさそうなので意味ないかも
                    print('page {0} show_more: {1}'.format(page_num, count))
                    count += 1
                    time.sleep(1)
                else:
                    break

        # テーブルを取得、保存
        df = pd.read_html(driver.page_source, header=0)[0]
        df.to_csv('{0}/{1}_{2}.csv'.format(csv_path, page_num.zfill(4), re.sub(' ', '_', page_title)),
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