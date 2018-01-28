
# coding: utf-8

# # import

# In[ ]:


import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import os
import re

import stock

get_ipython().run_line_magic('matplotlib', 'inline')


# In[ ]:


importlib.reload(stock)


# # _csvフォルダ内の価格データ一覧をリスト化

# In[ ]:


# _csvフォルダ内のファイル一覧をリスト化
csv_table = os.listdir('/Users/Really/Stockyard/_csv')
csv_table[:10], csv_table[-10:], len(csv_table)


# ## 価格データのファイル一覧を作成

# ### Regexオブジェクトを事前に作成する書き方

# In[ ]:


r = re.compile(r't_\d*.csv')
r


# In[ ]:


csv_table = [x for x in csv_table if r.match(x)]
csv_table[:10], csv_table[-10:], len(csv_table)


# ### Regexオブジェクトを事前作成しない書き方

# In[ ]:


csv_table = [i for i in csv_table if re.search(r't_\d*.csv', x)]
csv_table[:10], csv_table[-10:], len(csv_table)


# ## 上記のリスト内包表記について

# https://qiita.com/y__sama/items/a2c458de97c4aa5a98e7  

# ### 基本構文 [counter for counter in iterator]  

# In[ ]:


# 通常のリスト生成
extension_1 = []
for i in range(10):
    extension_1.append(i)
extension_1


# In[ ]:


# extension_1と同等のリストを内包表記で生成する場合は 
comprehension_1= [i for i in range(10)]
comprehension_1


# 先に [i for i in] だけ書いてから修飾することが多い  
# リスト内包表記はコードがすっきりするだけでなく速度面でも有利

# ### ifを含む場合(後置if)

# In[ ]:


extension_2 =[]
for i in range(10):
    if i%2==0:
        extension_2.append(i)
extension_2


# pythonには後置if文がありませんが、リスト内包表記に限っては（結果的にですが）書けます。  
# extension_2をリスト内包表記で書きなおすと下記のような感じです。  
# 結果的に後置ifの構文になっていますが、これは内包表記ではfor節の後にif節やfor節がつなげられるためです。  
# (コロンとインデントを省略できると思えばよい。)

# In[ ]:


comprehension_2 = [i for i in range(10) if i%2==0]
comprehension_2


# # 日付とデータの欠損埋め、加工データの追加、保存

# In[ ]:


for file_name in range(len(csv_table)):
    price_table = pd.read_csv('/Users/Really/Stockyard/_csv/{0}'.format(csv_table[file_name]), index_col='Date')
    price_table.index = pd.to_datetime(price_table.index)
    price_table = stock.complement_price(price_table)
    price_table = stock.add_processed_price(price_table)
    
    price_table.to_csv('/Users/Really/Stockyard/_csv_processed/{0}_p.csv'.format(csv_table[file_name].split('.')[0]))


# # プロットしてみる

# In[ ]:


df_price_fill['AdjClose'].plot()


# In[ ]:


df_price_fill['log_return_oc'].plot()

