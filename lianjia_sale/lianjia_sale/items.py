# -*- coding: utf-8 -*-

# Define here the models for your scraped items
#
# See documentation in:
# https://doc.scrapy.org/en/latest/topics/items.html

# import scrapy
from scrapy import Item, Field


class LianjiaSaleItem(Item):
    # define the fields for your item here like:
    # name = scrapy.Field()

    # info
    label = Field()  # -->string
    priceTotal = Field()  # -->float
    priceUnit = Field()  # -->float
    # downPaymentMin = Field()  # -->float
    community = Field()
    district = Field()
    position = Field()
    loop = Field()
    code = Field()
    codeComm = Field()
    orient = Field()
    area = Field()
    room = Field()  # -->string
    focus = Field()
    watch = Field()
    release = Field()
    link = Field()
    city = Field()  # -->string
    date = Field()
    timestamp = Field()