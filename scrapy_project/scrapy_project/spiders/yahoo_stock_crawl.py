
# -*- coding: utf-8 -*-
# エンコーディング宣言は Python2 用なので削除してもよい

from scrapy.spiders import CrawlSpider, Rule
from scrapy.linkextractors import LinkExtractor

from scrapy_project.items import yahoo_fundamental


class yahoo_stock_crawl_spider(CrawlSpider):
    name = 'yahoo_stock_crawl'
    allowed_domains = ['stocks.finance.yahoo.co.jp']
    start_urls = (
        'http://stocks.finance.yahoo.co.jp/stocks/qi/',
    )

    # リンクをたどるためのルールのリスト
    # https://stocks.finance.yahoo.co.jp/stocks/detail/?code=1301
    rules = (
        # 試験的に一覧の9ページ目まで。末尾の \d$ を \d+$ に変えれば10ページ以降も辿れるはず
        Rule(LinkExtractor(allow=r'/stocks/qi/\?&p=[1-2]$')),
        Rule(LinkExtractor(allow=r'/stocks/detail/\?code=\d+$'), callback='parse_fundamental'),
    )

    
    def parse_fundamental(self, response):
        """
        個々の銘柄ページでの処理
        """
        item = yahoo_fundamental()  # yahoo_fundamental オブジェクトを作成
        item['code'] = response.css('#stockinf dt').xpath('string()').extract_first() # 銘柄コード
        item['銘柄名'] = response.css('.symbol').xpath('string()').extract_first() # 銘柄名
        item['PER'] = response.css('#rfindex strong').xpath('string()').extract()[4] # PER
        item['PBR'] = response.css('#rfindex strong').xpath('string()').extract()[5] # PBR
        item['EPS'] = response.css('#rfindex strong').xpath('string()').extract()[6] # EPS
        item['BPS'] = response.css('#rfindex strong').xpath('string()').extract()[7] # BPS
        yield item  # Itemをyieldして、データを抽出する


"""
日時
コード
銘柄名

前日終値 Previous Close
始値
高値
安値
出来高 Volume
売買代金
値幅制限

時価総額 Market Cap
発行済株式数
配当利回り Forward Dividend & Yield ?
1株配当
PER
PBR
EPS
BPS
最低購入代金
単元株数
年初来高値
年初来安値

信用買残
前週比
信用売残
前週比
貸借倍率
"""