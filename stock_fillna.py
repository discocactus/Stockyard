#!/usr/bin/env python3
# -*- coding: utf-8 -*-
#%%
# import
import numpy as np
import pandas as pd

#%%
# 'Date'列をインデックスに指定してCSVの読み込み、インデックスをdatetime型に変換
df_quote = pd.read_csv('/Users/Really/Stockyard/_csv/t_1758.csv', index_col='Date')
df_quote.index = pd.to_datetime(df_quote.index)
df_quote

df_quote.dtypes
df_quote.index

#%%
# 出来高ゼロの日はインデックスごと欠落しているので、ビジネスデイ(freq='B')のdatetime型インデックスにデータをあてはめる
# TODO 休日はどうするか要検討
df_quote_fill = pd.DataFrame(df_quote, index=pd.date_range(df_quote.index[0], df_quote.index[-1], freq='B'))

# 欠損データを補完する
# Volumeをゼロで補完
df_quote_fill.Volume = df_quote_fill.Volume.fillna(0)

# Close,AdjCloseは前日データで補完
df_quote_fill[['Close', 'AdjClose']] = df_quote_fill[['Close', 'AdjClose']].fillna(method='ffill') 

# 欠損Openを前日補完済み前日Closeで補完 (下の2つはやり方の違い、結果は同じ)
df_quote_fill['Open'] = df_quote_fill['Open'].fillna(df_quote_fill['Close'].shift(1))
# df_quote_fill['Open'][1:] = df_quote_fill['Open'][1:].where(~df_quote_fill['Open'][1:].isnull(),
#                   np.array(df_quote_fill['Close'][:-1]))

# High,Low,CloseはOpenで補完
df_quote_fill[['Open', 'High', 'Low']] = df_quote_fill[['Open', 'High', 'Low']].fillna(method='ffill', axis=1)

df_quote_fill.isnull().any() # 欠損値の有無を列単位で確認
df_quote_fill

#%%