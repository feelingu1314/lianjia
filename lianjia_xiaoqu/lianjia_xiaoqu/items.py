# -*- coding: utf-8 -*-

# Define here the models for your scraped items
#
# See documentation in:
# https://doc.scrapy.org/en/latest/topics/items.html

from scrapy import Item, Field


class LianjiaXiaoquItem(Item):
    # define the fields for your item here like:
    # name = scrapy.Field()

    label = Field()  # string
    desc = Field()  # string
    focus = Field()  # int
    community = Field()  # string
    codeComm = Field()  # string
    age = Field()  # int
    priceUnit = Field()  # float
    sale = Field()  # int
    deal = Field()  # int
    rent = Field()  # int
    district = Field()  # string
    position = Field()  # string
    link = Field()  # string
    city = Field()  # string
    date = Field()  # string
    timestamp = Field()  # string

