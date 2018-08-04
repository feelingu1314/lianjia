# -*- coding: utf-8 -*-

# Define here the models for your scraped items
#
# See documentation in:
# https://doc.scrapy.org/en/latest/topics/items.html

from scrapy import Field, Item


class SoldItem(Item):
    # define the fields for your item here like:
    # name = scrapy.Field()

    房源链接 = Field()

    # 基本信息
    标题 = Field()
    成交时间 = Field()
    成交总价 = Field()
    成交单价 = Field()
    挂牌价格 = Field()
    成交周期 = Field()
    调价次数 = Field()
    带看 = Field()
    关注 = Field()
    浏览 = Field()

    基本属性 = Field()
    交易属性 = Field()
    房源特色 = Field()
    小区概况 = Field()

    城市 = Field()
    initial_time = Field()