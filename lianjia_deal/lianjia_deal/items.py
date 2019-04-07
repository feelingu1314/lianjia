# -*- coding: utf-8 -*-

# Define here the models for your scraped items
#
# See documentation in:
# https://doc.scrapy.org/en/latest/topics/items.html

from scrapy import Field, Item


class LianjiaDealItem(Item):
    # define the fields for your item here like:
    # name = scrapy.Field()

    # info
    # label = Field()
    houseInfo = Field()
    positionInfo = Field()
    priceQuote = Field()  # -->float
    priceDeal = Field()  # -->float
    priceDealUnit = Field()  # -->float
    dateDeal = Field()
    dateQuote = Field()
    periodDeal = Field()
    priceModifyCount = Field()
    community = Field()
    room = Field()
    area = Field()
    code = Field()
    codeComm = Field()
    link = Field()
    focus = Field()
    watch = Field()
    visit = Field()
    city = Field()
    date = Field()
    timestamp = Field()
