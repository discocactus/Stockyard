
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
        # Rule(LinkExtractor(allow=r'/stocks/qi/\?&p=\d+$')),
        Rule(LinkExtractor(allow=r'/stocks/detail/\?code=\d+$'), callback='parse_fundamental'),
    )

    
    def parse_fundamental(self, response):
        """
        個々の銘柄ページでの処理
        """
        item = yahoo_fundamental()  # yahoo_fundamental オブジェクトを作成

        # item['date'] = response.css('#stockinf span').xpath('string()').extract()[0] # date
        item['date'] = response.css('._37FKL945 *::text').extract()[1] # date
        item['code'] = response.css('#industry *::text').extract()[0] # 銘柄コード
        item['name'] = response.css('.DL5lxuTC *::text').extract()[0] # 銘柄名

        item['p_close'] = response.css('._2Yx3YP9V > div:nth-child(1) > ul:nth-child(2) > li:nth-child(1) > dl:nth-child(1) > dd:nth-child(2) > span:nth-child(1) > span:nth-child(1) > span:nth-child(1) *::text').extract()[0] # 前日終値
        item['open'] = response.css('._2Yx3YP9V > div:nth-child(1) > ul:nth-child(2) > li:nth-child(2) > dl:nth-child(1) > dd:nth-child(2) > span:nth-child(1) > span:nth-child(1) > span:nth-child(1) *::text').extract()[0] # 始値
        item['high'] = response.css('._2Yx3YP9V > div:nth-child(1) > ul:nth-child(2) > li:nth-child(3) > dl:nth-child(1) > dd:nth-child(2) > span:nth-child(1) > span:nth-child(1) > span:nth-child(1) *::text').extract()[0] # 高値
        item['low'] = response.css('._2Yx3YP9V > div:nth-child(1) > ul:nth-child(2) > li:nth-child(4) > dl:nth-child(1) > dd:nth-child(2) > span:nth-child(1) > span:nth-child(1) > span:nth-child(1) *::text').extract()[0] # 安値
        item['close'] = response.css('.nOmR5zWz *::text').extract()[0] # 終値
        item['volume'] = response.css('._2Yx3YP9V > div:nth-child(1) > ul:nth-child(2) > li:nth-child(5) > dl:nth-child(1) > dd:nth-child(2) > span:nth-child(1) > span:nth-child(1) > span:nth-child(1) *::text').extract()[0] # 出来高
        item['売買代金'] = response.css('._2Yx3YP9V > div:nth-child(1) > ul:nth-child(2) > li:nth-child(6) > dl:nth-child(1) > dd:nth-child(2) > span:nth-child(1) > span:nth-child(1) > span:nth-child(1) *::text').extract()[0] # 売買代金
        item['値幅制限'] = response.css('._2Yx3YP9V > div:nth-child(1) > ul:nth-child(2) > li:nth-child(7) > dl:nth-child(1) > dd:nth-child(2) > span:nth-child(1) > span:nth-child(1) > span:nth-child(1) *::text').extract()[0]
 # 値幅制限

        item['時価総額_百万円'] = response.css('.PQ9Z_PS3 > li:nth-child(1) > dl:nth-child(1) > dd:nth-child(2) > span:nth-child(1) > span:nth-child(1) > span:nth-child(1) *::text').extract()[0] # 時価総額(百万円)
        item['発行済株式数'] = response.css('.PQ9Z_PS3 > li:nth-child(2) > dl:nth-child(1) > dd:nth-child(2) > span:nth-child(1) > span:nth-child(1) > span:nth-child(1) *::text').extract()[0] # 発行済株式数
        item['配当利回り'] = response.css('.PQ9Z_PS3 > li:nth-child(3) > dl:nth-child(1) > dd:nth-child(2) > span:nth-child(1) > span:nth-child(1) > span:nth-child(1) *::text').extract()[0] # 配当利回り
        item['配当'] = response.css('.PQ9Z_PS3 > li:nth-child(4) > dl:nth-child(1) > dd:nth-child(2) *::text').extract()[0] # 1株配当
        item['per'] = response.css('.PQ9Z_PS3 > li:nth-child(5) > dl:nth-child(1) > dd:nth-child(2) *::text').extract()[1] # PER
        item['pbr'] = response.css('.PQ9Z_PS3 > li:nth-child(6) > dl:nth-child(1) > dd:nth-child(2) *::text').extract()[1] # PBR
        item['eps'] = response.css('.PQ9Z_PS3 > li:nth-child(7) > dl:nth-child(1) > dd:nth-child(2) > span:nth-child(1) > span:nth-child(1) > span:nth-child(2) *::text').extract()[0] # EPS
        item['bps'] = response.css('.PQ9Z_PS3 > li:nth-child(8) > dl:nth-child(1) > dd:nth-child(2) > span:nth-child(1) > span:nth-child(1) > span:nth-child(2) *::text').extract()[0] # BPS
        item['最低購入代金'] = response.css('.PQ9Z_PS3 > li:nth-child(9) > dl:nth-child(1) > dd:nth-child(2) > span:nth-child(1) > span:nth-child(1) > span:nth-child(1) *::text').extract()[0] # 最低購入代金
        item['単元株数'] = response.css('.PQ9Z_PS3 > li:nth-child(10) > dl:nth-child(1) > dd:nth-child(2) > span:nth-child(1) > span:nth-child(1) > span:nth-child(1) *::text').extract()[0] # 単元株数
        item['年初来高値'] = response.css('.PQ9Z_PS3 > li:nth-child(11) > dl:nth-child(1) > dd:nth-child(2) > span:nth-child(1) > span:nth-child(1) > span:nth-child(1) *::text').extract()[0] # 年初来高値
        item['年初来安値'] = response.css('.PQ9Z_PS3 > li:nth-child(12) > dl:nth-child(1) > dd:nth-child(2) > span:nth-child(1) > span:nth-child(1) > span:nth-child(1) *::text').extract()[0] # 年初来安値

        item['信用買残'] = response.css('li.diLxNiWT:nth-child(1) > dl:nth-child(1) > dd:nth-child(2) > span:nth-child(1) > span:nth-child(1) > span:nth-child(1) *::text').extract()[0] # 信用買残
        item['信用買残前週比'] = response.css('li.diLxNiWT:nth-child(2) > dl:nth-child(1) > dd:nth-child(2) > span:nth-child(1) > span:nth-child(1) > span:nth-child(1) *::text').extract()[0] # 信用買残前週比
        item['信用売残'] = response.css('li.diLxNiWT:nth-child(3) > dl:nth-child(1) > dd:nth-child(2) > span:nth-child(1) > span:nth-child(1) > span:nth-child(1) *::text').extract()[0] # 信用売残
        item['信用売残前週比'] = response.css('li.diLxNiWT:nth-child(4) > dl:nth-child(1) > dd:nth-child(2) > span:nth-child(1) > span:nth-child(1) > span:nth-child(1) *::text').extract()[0] # 信用売残前週比
        item['貸借倍率'] = response.css('li.diLxNiWT:nth-child(5) > dl:nth-child(1) > dd:nth-child(2) > span:nth-child(1) > span:nth-child(1) > span:nth-child(1) *::text').extract()[0] # 貸借倍率

        item['get'] = datetime.now().strftime("%Y/%m/%d %H:%M:%S") # 取得日時

        item['date'] = re.sub('[（）]', '', item['date'])
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
