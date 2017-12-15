
# coding: utf-8

# In[ ]:

import re
import itertools


# # 文字列の数字部分と数字じゃない部分の集まりで分割したい

# In[ ]:

re.findall(r'(\d+|\D+)', "ab123d45fgh67")


# In[ ]:

[''.join(it) for _, it in itertools.groupby("ab123d45fgh67", str.isdigit)]

