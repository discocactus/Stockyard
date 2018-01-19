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
