# -*- coding: utf-8 -*-

# Define here the models for your scraped items
#
# See documentation in:
# https://doc.scrapy.org/en/latest/topics/items.html

# import scrapy
from scrapy.item import Item, Field


class SellItem(Item):
    # define the fields for your item here like:
    # name = scrapy.Field()

    # 基本信息
    标题 = Field()
    关键字 = Field()
    总价 = Field()  # (float)
    单价 = Field()  # (float)
    最低首付 = Field()  # (float)
    小区 = Field()
    区县 = Field()
    位置 = Field()
    环线信息 = Field()
    建造时间 = Field()
    链家编号 = Field()

    基本属性 = Field()
    交易属性 = Field()
    房源特色 = Field()
    户型分间 = Field()
    房源热度 = Field()
    小区概况 = Field()
    房源链接 = Field()
    城市 = Field()
    initial_time = Field()
