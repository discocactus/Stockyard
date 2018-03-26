
# coding: utf-8

# # import & settings

# In[ ]:


import pandas as pd
import matplotlib.pyplot as plt
import wget
import pathlib
from retry import retry

get_ipython().run_line_magic('matplotlib', 'inline')


# In[ ]:


# pandas の最大表示列数を設定 (max_rows で表示行数の設定も可能)
pd.set_option('display.max_columns', 30)


# In[ ]:


stooq_path = pathlib.Path('D:\stockyard\stooq')

# 10年国債金利
10auy.b
10dey.b
10fry.b
10ity.b
10jpy.b
10uky.b
10usy.b

# 株式インデックス
^dax
^dji
^ftm
^nkx
^spx
^tpx #TOPIX

# 商品
xauusd
cl_c #Crude Oil WTI Cash
cl_f #Crude Oil WTI - NYMEX
rb_c_d #Gasoline_RBOB_Cash_Commodities_Cash
rb_f_d #Gasoline_RBOB_NYMEX_Commodities_Futures
rt_f_d #Gasoline_RBOB_F_NYMEX_Commodities Futures

# その他
vi_c #S&P 500 VIX Cash
vi_f #S&P 500 VIX - CBOE
# In[ ]:


for file in stooq_path.iterdir():
    print(file)


# # データのダウンロード

# In[ ]:


ticker = 'usdjpy'


# In[ ]:


url = 'https://stooq.com/q/d/l/?s={0}&i=d'.format(ticker)


# In[ ]:


wget.download(url=url, out=str(stooq_path))


# In[ ]:


help(wget)


# # csvからの読み込み

# In[ ]:


def read_stooq(ticker, stooq_path=stooq_path):
    result = pd.read_csv('{0}/{1}_d.csv'.format(stooq_path, ticker.replace('.', '_')), index_col=0)
    result.index = pd.to_datetime(result.index)

    return result


# In[ ]:


read_ticker = 'xauusd'


# In[ ]:


df = read_stooq(read_ticker)


# In[ ]:


df


# In[ ]:


df.dtypes


# In[ ]:


for i in range(len(df)-1):
    # 前行の日付との差を計算
    td = df.index[i+1] - df.index[i]
    # 28日未満ならその行番号と日付を出力してbreak
    if td.days < 28:
        start = df.index[i]
        print('{0}: {1}'.format(i, start))
        break


# In[ ]:


df[start:]


# In[ ]:


df['Close'][start:].plot()


# # 個別の変数に格納

# In[ ]:


us10 = df[start:]


# In[ ]:


jp10 = df[start:]


# In[ ]:


uk10 = df[start:]


# In[ ]:


usdjpy = df['2005-11-28':]


# In[ ]:


dj = df['2005-11-28':]


# In[ ]:


nk = df['2005-11-28':]


# In[ ]:


oil = df['2005-11-28':]


# In[ ]:


gold = df['2005-11-28':]


# # 終値を単一テーブルにまとめる

# In[ ]:


y10 = pd.concat([us10['Close']['2005-11-28':], jp10['Close']], axis=1)
y10.columns = ['us', 'jp']
y10 = y10.fillna(method='ffill')


# In[ ]:


y10 = pd.concat([y10, usdjpy['Close']], axis=1)
y10.columns = ['us', 'jp', 'usdjpy']
y10 = y10.fillna(method='ffill')


# In[ ]:


y10 = pd.concat([y10, dj['Close']], axis=1)
y10.columns = ['us', 'jp', 'usdjpy', 'dj']
y10 = y10.fillna(method='ffill')


# In[ ]:


y10 = pd.concat([y10, nk['Close']], axis=1)
y10.columns = ['us', 'jp', 'usdjpy', 'dj', 'nk']
y10 = y10.fillna(method='ffill')


# In[ ]:


y10 = pd.concat([y10, oil['Close']], axis=1)
y10.columns = ['us', 'jp', 'usdjpy', 'dj', 'nk', 'oil']
y10 = y10.fillna(method='ffill')


# In[ ]:


y10 = pd.concat([y10, gold['Close']], axis=1)
y10.columns = ['us', 'jp', 'usdjpy', 'dj', 'nk', 'oil', 'gold']
y10 = y10.fillna(method='ffill')


# In[ ]:


y10


# # プロットしてみる

# In[ ]:


((y10.us - y10.jp) / (y10.us[0] - y10.jp[0])).plot()


# In[ ]:


(y10.usdjpy / y10.usdjpy[0]).plot()


# In[ ]:


# 使用できる色の確認
import matplotlib
matplotlib.colors.cnames


# In[ ]:


plt.figure(figsize=(16, 9))
plt.plot(y10.index, (y10.us / y10.us[0]), color='pink', label='us')
plt.plot(y10.index, (y10.jp / y10.jp[0]), color='red', label='jp')
plt.plot(y10.index, ((y10.us - y10.jp) / (y10.us[0] - y10.jp[0])), color='magenta', label='yield_spread')
plt.plot(y10.index, (y10.usdjpy / y10.usdjpy[0]), color='limegreen', label='usdjpy')
plt.plot(y10.index, (y10.dj / y10.dj[0]), color='cornflowerblue', label='dj')
plt.plot(y10.index, (y10.nk / y10.nk[0]), color='blue', label='nk')
plt.plot(y10.index, (y10.oil / y10.oil[0]), color='brown', label='oil')
plt.plot(y10.index, (y10.gold / y10.gold[0]), color='orange', label='gold')
plt.legend(loc='upper left')
plt.show()


# In[ ]:


plot_start = '2012-01-02'


# In[ ]:


x = y10[plot_start:].index
plt.figure(figsize=(16, 9))
plt.plot(x, (y10.us[plot_start:] / y10.us[plot_start]), color='pink', label='us')
plt.plot(x, (y10.jp[plot_start:] / y10.jp[plot_start]), color='red', label='jp')
plt.plot(x, ((y10.us[plot_start:] - y10.jp[plot_start:]) / (y10.us[plot_start] - y10.jp[plot_start])), color='magenta', label='yield_spread')
plt.plot(x, (y10.usdjpy[plot_start:] / y10.usdjpy[plot_start]), color='limegreen', label='usdjpy')
plt.plot(x, (y10.dj[plot_start:] / y10.dj[plot_start]), color='cornflowerblue', label='dj')
plt.plot(x, (y10.nk[plot_start:] / y10.nk[plot_start]), color='blue', label='nk')
plt.plot(x, (y10.oil[plot_start:] / y10.oil[plot_start]), color='brown', label='oil')
plt.plot(x, (y10.gold[plot_start:] / y10.gold[plot_start]), color='orange', label='gold')
plt.legend(loc='upper left')
plt.show()

