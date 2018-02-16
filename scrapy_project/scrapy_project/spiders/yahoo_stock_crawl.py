
# -*- coding: utf-8 -*-
# エンコーディング宣言は Python2 用なので削除してもよい

from scrapy.spiders import CrawlSpider, Rule
from scrapy.linkextractors import LinkExtractor
import re
from datetime import datetime

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
        # 試験的に一覧の9ページ目(または指定ページ)まで。末尾の \d$ (or [1-2]$) を \d+$ に変えれば10ページ以降も辿れるはず
        Rule(LinkExtractor(allow=r'/stocks/qi/\?&p=[1-2]$')),
        Rule(LinkExtractor(allow=r'/stocks/detail/\?code=\d+$'), callback='parse_fundamental'),
    )

    
    def parse_fundamental(self, response):
        """
        個々の銘柄ページでの処理
        """
        item = yahoo_fundamental()  # yahoo_fundamental オブジェクトを作成

        item['date'] = response.css('#stockinf span').xpath('string()').extract()[0] # date
        item['code'] = response.css('#stockinf dt').xpath('string()').extract_first() # 銘柄コード
        item['name'] = response.css('#stockinf h1').xpath('string()').extract_first() # 銘柄名

        item['p_close'] = response.css('#detail strong').xpath('string()').extract()[2] # 前日終値
        item['open'] = response.css('#detail strong').xpath('string()').extract()[3] # 始値
        item['high'] = response.css('#detail strong').xpath('string()').extract()[4] # 高値
        item['low'] = response.css('#detail strong').xpath('string()').extract()[5] # 安値
        item['close'] = response.css('#stockinf td').xpath('string()').extract()[1] # 終値
        item['volume'] = response.css('#detail strong').xpath('string()').extract()[6] # 出来高
        item['売買代金'] = response.css('#detail strong').xpath('string()').extract()[7] # 売買代金
        item['値幅制限'] = response.css('#detail strong').xpath('string()').extract()[8] # 値幅制限

        item['時価総額_百万円'] = response.css('#rfindex strong').xpath('string()').extract()[0] # 時価総額(百万円)
        item['発行済株式数'] = response.css('#rfindex strong').xpath('string()').extract()[1] # 発行済株式数
        item['配当利回り'] = response.css('#rfindex strong').xpath('string()').extract()[2] # 配当利回り
        item['配当'] = response.css('#rfindex strong').xpath('string()').extract()[3] # 1株配当
        item['per'] = response.css('#rfindex strong').xpath('string()').extract()[4] # PER
        item['pbr'] = response.css('#rfindex strong').xpath('string()').extract()[5] # PBR
        item['eps'] = response.css('#rfindex strong').xpath('string()').extract()[6] # EPS
        item['bps'] = response.css('#rfindex strong').xpath('string()').extract()[7] # BPS
        item['最低購入代金'] = response.css('#rfindex strong').xpath('string()').extract()[8] # 最低購入代金
        item['単元株数'] = response.css('#rfindex strong').xpath('string()').extract()[9] # 単元株数
        item['年初来高値'] = response.css('#rfindex strong').xpath('string()').extract()[10] # 年初来高値
        item['年初来安値'] = response.css('#rfindex strong').xpath('string()').extract()[11] # 年初来安値

        item['信用買残'] = response.css('#margin strong').xpath('string()').extract()[0] # 信用買残
        item['信用買残前週比'] = response.css('#margin strong').xpath('string()').extract()[1] # 信用買残前週比
        item['信用売残'] = response.css('#margin strong').xpath('string()').extract()[3] # 信用売残
        item['信用売残前週比'] = response.css('#margin strong').xpath('string()').extract()[4] # 信用売残前週比
        item['貸借倍率'] = response.css('#margin strong').xpath('string()').extract()[2] # 貸借倍率

        item['get'] = datetime.now().strftime("%Y/%m/%d %H:%M:%S") # 取得日時

        for key in item:
            item[key] = re.sub('\n', '', item[key])

        print('\n - print item - \n')
        for key in item:
            print('{0}: {1}'.format(key, item[key]))
        print('\n - print item end - \n')

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
