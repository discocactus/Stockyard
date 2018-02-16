
# coding: utf-8

# # import

# In[ ]:


import os
import csv
import locale
import re
import pandas as pd
from datetime import datetime


# In[ ]:


# pandas の最大表示列数を設定 (max_rows で表示行数の設定も可能)
pd.set_option('display.max_columns', 30)


# # memo

# In[ ]:


df = pd.read_csv(r"C:\Users\Really\GitHub\Stockyard\scrapy_project\yahoo_fundamental.csv")
df


# In[ ]:


df = df.sort_values(['date', 'code', 'get'])
df


# In[ ]:


df = df.drop_duplicates(['date', 'code'], keep='last').reset_index(drop=True)
df


# In[ ]:


df.to_csv('test.csv', index=False)


# In[ ]:


test = pd.read_csv('test.csv')
test


# In[ ]:


d = {'code': '1380', '銘柄名': '(株)秋川牧園', 'PER': '(連) 34.77', 'PBR': '(連) 1.91', 'EPS': '\n(連) 20.39', 'BPS': '\n(連) 370.29'}
d


# In[ ]:


for key in d:
    d[key] = re.sub('\n', '', d[key])
d


# In[ ]:


datetime.now().strftime("%Y/%m/%d %H:%M:%S")


# # 新規プロジェクトの生成

# In[ ]:


get_ipython().system('scrapy startproject scrapy_project')


# In[ ]:


get_ipython().system('tree scrapy_project')


# # プロジェクトのディレクトリに移動、新規spiderの生成

# In[ ]:


os.getcwd()


# In[ ]:


# os.chdir('/Users/Really/Stockyard/scrapy_project')
os.chdir(r"C:\Users\Really\GitHub\Stockyard\scrapy_project")
os.getcwd()


# In[ ]:


get_ipython().system('scrapy genspider yahoo_stock stocks.finance.yahoo.co.jp/stocks/qi/')


# # 個々のファイルの編集
settings.py に記述追加USER_AGENT = 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_12_5) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.36'
DOWNLOAD_DELAY = 1items.py
# In[ ]:


# -*- coding: utf-8 -*-

# Define here the models for your scraped items
#
# See documentation in:
# http://doc.scrapy.org/en/latest/topics/items.html

import scrapy


class ScrapyProjectItem(scrapy.Item):
    # define the fields for your item here like:
    # name = scrapy.Field()
    pass


class yahoo_fundamental(scrapy.Item):
    """
    個々の銘柄の指標を表すItem
    """

    code = scrapy.Field()
    symbol_name = scrapy.Field()
    per = scrapy.Field()
    pbr = scrapy.Field()
    eps = scrapy.Field()
    bps = scrapy.Field()


# In[ ]:


yahoo_stock.py


# In[ ]:


# -*- coding: utf-8 -*-
# エンコーディング宣言は Python2 用なので削除してもよい

import scrapy

from scrapy_project.items import yahoo_fundamental


class YahooStockSpider(scrapy.Spider):
    name = 'yahoo_stock'
    allowed_domains = ['stocks.finance.yahoo.co.jp/stocks']
    start_urls = (
        'http://stocks.finance.yahoo.co.jp/stocks/qi/',
    )

    def parse(self, response):
        """
        銘柄一覧ページから個々の銘柄へのリンクを抜き出してたどる
        """
        # listTable > table > tbody > tr:nth-child(2) > td.center.yjM > a
        # print(response.css('td.center.yjM a::attr("href")').extract())
        for url in response.css('td.center.yjM a::attr("href")').extract():
            yield scrapy.Request(response.urljoin(url), self.parse_fundamental, dont_filter=True)

    def parse_fundamental(self, response):
        """
        個々の銘柄ページでの処理
        """
        item = yahoo_fundamental()  # yahoo_fundamental オブジェクトを作成
        item['code'] = response.css('#stockinf dt').xpath('string()').extract_first() # 銘柄コード
        item['symbol_name'] = response.css('.symbol').xpath('string()').extract_first() # 銘柄名
        item['per'] = response.css('#rfindex strong').xpath('string()').extract()[4] # PER
        item['pbr'] = response.css('#rfindex strong').xpath('string()').extract()[5] # PBR
        item['eps'] = response.css('#rfindex strong').xpath('string()').extract()[6] # EPS
        item['bps'] = response.css('#rfindex strong').xpath('string()').extract()[7] # BPS
        yield item  # Itemをyieldして、データを抽出する


# In[ ]:


get_ipython().run_cell_magic('writefile', 'yahoo_stock_crawl.py', '\n# -*- coding: utf-8 -*-\n# エンコーディング宣言は Python2 用なので削除してもよい\n\nfrom scrapy.spiders import CrawlSpider, Rule\nfrom scrapy.linkextractors import LinkExtractor\n\nfrom scrapy_project.items import yahoo_fundamental\n\n\nclass yahoo_stock_crawl_spider(CrawlSpider):\n    name = \'yahoo_stock_crawl\'\n    allowed_domains = [\'stocks.finance.yahoo.co.jp\']\n    start_urls = (\n        \'http://stocks.finance.yahoo.co.jp/stocks/qi/\',\n    )\n\n    # リンクをたどるためのルールのリスト\n    # https://stocks.finance.yahoo.co.jp/stocks/detail/?code=1301\n    rules = (\n        # 試験的に一覧の9ページ目まで。末尾の \\d$ を \\d+$ に変えれば10ページ以降も辿れるはず\n        Rule(LinkExtractor(allow=r\'/stocks/qi/\\?&p=\\d$\')),\n        Rule(LinkExtractor(allow=r\'/stocks/detail/\\?code=\\d+$\'), callback=\'parse_fundamental\'),\n    )\n\n    \n    def parse_fundamental(self, response):\n        """\n        個々の銘柄ページでの処理\n        """\n        item = yahoo_fundamental()  # yahoo_fundamental オブジェクトを作成\n        item[\'code\'] = response.css(\'#stockinf dt\').xpath(\'string()\').extract_first() # 銘柄コード\n        item[\'symbol_name\'] = response.css(\'.symbol\').xpath(\'string()\').extract_first() # 銘柄名\n        item[\'per\'] = response.css(\'#rfindex strong\').xpath(\'string()\').extract()[4] # PER\n        item[\'pbr\'] = response.css(\'#rfindex strong\').xpath(\'string()\').extract()[5] # PBR\n        item[\'eps\'] = response.css(\'#rfindex strong\').xpath(\'string()\').extract()[6] # EPS\n        item[\'bps\'] = response.css(\'#rfindex strong\').xpath(\'string()\').extract()[7] # BPS\n        yield item  # Itemをyieldして、データを抽出する')

