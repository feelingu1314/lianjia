# -*- coding: utf-8 -*-

# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://doc.scrapy.org/en/latest/topics/item-pipeline.html
import pymongo
from datetime import datetime, date
from scrapy.exceptions import DropItem


class LianjiaDealPipeline(object):
    def process_item(self, item, spider):
        item['area'] = float(item['area'][:-2])
        item['houseInfo'] = '+'.join([i.strip() for i in item['houseInfo'].split('|')])
        item['priceQuote'] = float(item['priceQuote']) if item['priceQuote'] not in ['暂无数据'] else 0
        item['priceDeal'] = float(item['priceDeal']) if item['priceDeal'] not in ['暂无数据'] and item['priceDeal'] else 0
        item['priceDealUnit'] = float(item['priceDealUnit'])/10000 if item['priceDealUnit'] not in ['暂无数据'] and \
                                                                     item['priceDealUnit'] else 0
        item['dateDeal'] = item['dateDeal'].replace('.', '-')[:-3]
        item['dateQuote'] = item['dateQuote'].strip()
        item['periodDeal'] = int(item['periodDeal']) if item['periodDeal'] not in ['暂无数据'] else 0
        item['priceModifyCount'] = int(item['priceModifyCount'])
        item['focus'] = int(item['focus']) if item['focus'] not in ['暂无数据'] else 0
        item['watch'] = int(item['watch']) if item['watch'] not in ['暂无数据'] else 0
        item['visit'] = int(item['visit']) if item['visit'] not in ['暂无数据'] else 0
        item['date'] = date.today().strftime('%Y-%m-%d')
        item['timestamp'] = datetime.now().strftime('%H:%M:%S')
        return item


class DuplicatesPipeline(object):

    def __init__(self):
        self.ids_seen = set()

    def process_item(self, item, spider):
        if item['code'] in self.ids_seen:
            raise DropItem("duplicate: %s" % item['code'])
        else:
            self.ids_seen.add(item['code'])
            return item


class MongoPipeline(object):

    collection_name = 'sh_chengjiao'

    def __init__(self, mongo_uri, mongo_db):
        self.mongo_uri = mongo_uri
        self.mongo_db = mongo_db

    @classmethod
    def from_crawler(cls, crawler):
        return cls(
            mongo_uri=crawler.settings.get('MONGO_URI'),
            mongo_db=crawler.settings.get('MONGO_DATABASE', 'items')
        )

    def open_spider(self, spider):
        self.client = pymongo.MongoClient(self.mongo_uri)
        self.db = self.client[self.mongo_db]

    def close_spider(self, spider):
        self.client.close()

    def process_item(self, item, spider):
        if self.db[self.collection_name].count({'code': item['code']}) > 0:
            raise DropItem("duplicate: %s" % item['code'])
        else:
            self.db[self.collection_name].insert_one(dict(item))
        return item