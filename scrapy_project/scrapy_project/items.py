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
    銘柄名 = scrapy.Field()
    PER = scrapy.Field()
    PBR = scrapy.Field()
    EPS = scrapy.Field()
    BPS = scrapy.Field()
