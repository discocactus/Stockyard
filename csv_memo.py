
# coding: utf-8

# In[ ]:


import os
import csv
import locale
import re
import pandas as pd


# In[ ]:


# pandas の最大表示列数を設定 (max_rows で表示行数の設定も可能)
pd.set_option('display.max_columns', 30)


# In[ ]:


os.getcwd()


# In[ ]:


os.chdir(r'C:\Users\Really\GitHub\Stockyard')
os.getcwd()


# In[ ]:


try:
    with open('top_cities_3.csv', 'x', newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, ['rank', 'city', 'population'])
        writer.writeheader()
except:
    print('File exists: top_cities_3.csv')


# In[ ]:


pd.read_csv('top_cities_3.csv')


# In[ ]:


# 1行でも辞書をリストの中に入れなければいけないらしい
with open('top_cities_3.csv', 'a', newline='', encoding='utf-8') as f:
    writer = csv.DictWriter(f, ['rank', 'city', 'population'])
    writer.writerows([{'rank': 1, 'city': '上海', 'population': 24150000}])


# In[ ]:


with open('top_cities_3.csv', 'a', newline='', encoding='utf-8') as f:
    writer = csv.DictWriter(f, ['rank', 'city', 'population'])
    writer.writerows([
        {'rank': 2, 'city': 'カラチ', 'population': 23500000},
        {'rank': 3, 'city': '北京', 'population': 21516000},
    ])


# In[ ]:


with open('top_cities_3.csv', 'a', newline='', encoding='utf-8') as f:
    writer = csv.DictWriter(f, ['rank', 'city', 'population'])
    writer.writerows([
        {'rank': 4, 'city': '天津', 'population': 14722100},
        {'rank': 5, 'city': 'イスタンブル', 'population': 14160467},
    ])


# In[ ]:


locale.getpreferredencoding(False)


# In[ ]:


with open(r"C:\Users\Really\GitHub\Stockyard\scrapy_project\yahoo_fundamental.csv", 'r', encoding='utf-8') as f:
    reader = csv.reader(f)
    data = [x for x in reader]
print(data)


# In[ ]:


pd.read_csv(r"C:\Users\Really\GitHub\Stockyard\scrapy_project\yahoo_fundamental.csv")


# In[ ]:


d = {'code': '1380', '銘柄名': '(株)秋川牧園', 'PER': '(連) 34.77', 'PBR': '(連) 1.91', 'EPS': '\n(連) 20.39', 'BPS': '\n(連) 370.29'}
d


# In[ ]:


for key in d:
    d[key] = re.sub('\n', '', d[key])
d


# In[ ]:


d['EPS']

