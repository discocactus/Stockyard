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


    date = scrapy.Field()
    code = scrapy.Field()
    name = scrapy.Field()

    detail = scrapy.Field()
    #p_close = scrapy.Field()
    #open = scrapy.Field()
    #high = scrapy.Field()
    #low = scrapy.Field()
    #close = scrapy.Field()
    #volume = scrapy.Field()
    #売買代金 = scrapy.Field()
    #値幅制限 = scrapy.Field()

    reference = scrapy.Field()
    #時価総額_百万円 = scrapy.Field()
    #発行済株式数 = scrapy.Field()
    #配当利回り = scrapy.Field()
    #配当 = scrapy.Field()
    #per = scrapy.Field()
    #pbr = scrapy.Field()
    #eps = scrapy.Field()
    #bps = scrapy.Field()
    #最低購入代金 = scrapy.Field()
    #単元株数 = scrapy.Field()
    #年初来高値 = scrapy.Field()
    #年初来安値 = scrapy.Field()

    margin = scrapy.Field()
    #信用買残 = scrapy.Field()
    #信用買残前週比 = scrapy.Field()
    #信用売残 = scrapy.Field()
    #信用売残前週比 = scrapy.Field()
    #貸借倍率 = scrapy.Field()

    get = scrapy.Field()
