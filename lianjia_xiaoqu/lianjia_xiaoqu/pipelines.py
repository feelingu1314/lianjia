# -*- coding: utf-8 -*-

# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://doc.scrapy.org/en/latest/topics/item-pipeline.html
import pymongo
from datetime import datetime, date
from scrapy.exceptions import DropItem


class LianjiaXiaoquPipeline(object):
    def process_item(self, item, spider):
        item['priceUnit'] = float(
            item['priceUnit']) / 10000 if item['priceUnit'] not in ['暂无数据', '暂无'] and item['priceUnit'] else 0
        item['sale'] = int(item['sale']) if item['sale'] not in ['暂无数据', '暂无'] and item['sale'] else 0
        item['deal'] = int(item['deal'][5:-1]) if item['deal'] not in ['暂无数据'] and item['deal'] else 0
        item['rent'] = int(
            item['rent'][0:-5]) if item['rent'] not in ['暂无数据'] and item['rent'] else 0
        item['age'] = ''.join(item['age'].strip().split())
        item['age'] = int(
            item['age'][1:-3]) if item['age'] not in ['暂无数据', '暂无信息', '未知年建成'] and item['age'] else 0
        item['label'] = item['label'] if item['label'] else ''
        item['focus'] = int(item['focus']) if item['focus'] not in [
            '暂无数据'] and item['focus'] else 0
        item['date'] = date.today().strftime('%Y-%m-%d')
        item['timestamp'] = datetime.now().strftime('%H:%M:%S')
        return item


class DuplicatesPipeline(object):

    def __init__(self):
        self.ids_seen = set()

    def process_item(self, item, spider):
        if item['codeComm'] in self.ids_seen:
            raise DropItem("Duplicate item found: %s" % item['codeComm'])
        else:
            self.ids_seen.add(item['codeComm'])
            return item


class MongoPipeline(object):

    collection_name = '{}_xiaoqu'

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
        if self.db[self.collection_name.format(item['city'])].count({'date': item['date'], 'code': item['code']}) > 0:
            raise DropItem("duplicate: %s" % item['codeComm'])
        else:
            self.db[self.collection_name.format(item['city'])].insert_one(dict(item))
        return item