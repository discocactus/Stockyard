
# coding: utf-8

# # import

# In[ ]:


import numpy as np
import pandas as pd
import pandas.tseries.offsets as offsets
import matplotlib.pyplot as plt
import os
import re
import importlib

import stock

get_ipython().run_line_magic('matplotlib', 'inline')


# In[ ]:


importlib.reload(stock)


# In[ ]:


# pandas の最大表示列数を設定 (max_rows で表示行数の設定も可能)
pd.set_option('display.max_columns', 30)


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


# # 調整後終値の計算

# ## regexやstrアクセサの動作確認

# In[ ]:


info = stock.get_yahoo_info()


# In[ ]:


info


# In[ ]:


# '分割: 1株 -> 2株'形式以外の情報が入っていた場合も想定
info.iloc[-1, 3] = 'なんちゃって'
info


# In[ ]:


info.loc[0, 'Open']


# In[ ]:


re.split(' ', info.loc[0, 'Open'])


# In[ ]:


re.split(' ', info.loc[0, 'Open'])[-1]


# In[ ]:


re.search('([0-9.]*)株', '分割: 1株 -> 0.1株').group()


# In[ ]:


re.findall('([0-9.]*)株', '分割: 1株 -> 0.1株')


# In[ ]:


re.findall('([0-9.]*)株', info.loc[0, 'Open'])[-1]


# In[ ]:


info['Open'].apply(lambda x: re.findall('-> ([0-9.]*)株', x))


# In[ ]:


info['Open'].str.extract('-> ([0-9.]*)株', expand=True)


# In[ ]:


info['High'] = info['Open'].str.extract('-> ([0-9.]*)株', expand=True).astype(float)
info


# In[ ]:


info.dtypes


# ## 本番

# In[ ]:


info_all = stock.get_yahoo_info()
# 分割・併合倍率を抽出、格納
info_all['s_rate'] = info_all['Open'].str.extract('-> ([0-9.]*)株', expand=True).astype(float)


# In[ ]:


info_all


# In[ ]:


info_all.groupby('Code').count()


# In[ ]:


# 計算前の AdjClose 列を残すバージョン
def calc_adj_close(code):
    info = stock.get_yahoo_info()
    price = stock.get_yahoo_price(code)
    
    if info['Code'].isin([code]).any():
        info['s_rate'] = info['Open'].str.extract('-> ([0-9.]*)株', expand=True).astype(float)
        info = info[info['Code'] == code].set_index('Date')

        price['s_rate'] = info['s_rate']
        price['s_rate'] = price['s_rate'].fillna(1)
        price['a_rate'] = 1.0
        # yahoo の正確な計算式は不明。誤差が発生する銘柄もある。桁数だけの問題ではなさそう。
        for i in reversed(range(len(price) - 1)):
            price['a_rate'][i] = price['a_rate'][i + 1] / price['s_rate'][i + 1]
        price['CalcClose'] = np.round(price['Close'] * price['a_rate'], 2)
        price = price[['Open', 'High', 'Low', 'Close', 'Volume', 'AdjClose', 'CalcClose']]
    else:
        print('code {0} has no info.'.format(code))
    
    return price


# In[ ]:


code = 1711


# In[ ]:


calc_adj_close(code)


# In[ ]:


a = calc_adj_close(code)
a.to_csv('calc.csv')


# In[ ]:


stock.calc_adj_close(code)


# In[ ]:


# 計算前の AdjClose 列、代入した分割率や算出した調整レートを残すバージョン
info = info_all[info_all['Code']==code].set_index('Date')

price = stock.get_yahoo_price(code)

price['s_rate'] = info['s_rate']
price['s_rate'] = price['s_rate'].fillna(1)
price['a_rate'] = 1.0

# yahoo の正確な計算式は不明。誤差が発生する銘柄もある。桁数だけの問題ではなさそう。
for i in reversed(range(len(price) - 1)):
    #price['a_rate'][i] = np.round(price['a_rate'][i + 1] / price['s_rate'][i + 1], 6)
    price['a_rate'][i] = price['a_rate'][i + 1] / price['s_rate'][i + 1]

price['CalcClose'] = np.round(price['Close'] * price['a_rate'], 2)


# In[ ]:


# 分割実施前後の期間を表示
for date in info.index:
    print(date.date())
    display(price[date + offsets.Day(-10) : date + offsets.Day(10)])


# In[ ]:


price


# In[ ]:


price.to_csv('calc_1491.csv')


# In[ ]:


price['2014-07-01':'2014-07-30']


# In[ ]:


price[:10]


# In[ ]:


price[-10:]

