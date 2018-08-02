# -*- coding: utf-8 -*-

# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://doc.scrapy.org/en/latest/topics/item-pipeline.html
# from scrapy.utils.project import get_project_settings
import pymongo
import datetime
import redis

class SellPipeline(object):
    def process_item(self, item, spider):
        for k, v in item.items():
            if k != '小区概况' and isinstance(v, str):
                item[k] = v.strip()
        if item['小区概况']['小区名'] is None:
            item['小区概况']['小区名'] = 'None'
        if item['总价'] in ['', '暂无数据'] or item['总价'] is None:
            item['总价'] = float(0)
        else:
            item['总价'] = float(item['总价'])
        if item['单价'] in ['', '暂无数据'] or item['单价'] is None:
            item['单价'] = float(0)
        else:
            item['单价'] = float(item['单价'])
        item['最低首付'] = float(item['最低首付'].strip()[2:-1])
        if item['建造时间'][:4] == '未知年建':
            item['建造时间'] = 0
        else:
            item['建造时间'] = int(item['建造时间'][:4])
        item['环线信息'] = item['环线信息'].replace('\xa0', '')
        if item['户型分间']:
            temp = dict()
            for i in range(0, len(item['户型分间']), 4):
                temp[item['户型分间'][i]] = item['户型分间'][i+1:i+4]
            item['户型分间'] = temp
        else:
            item['户型分间'] = 'None'

        item['initial_time'] = datetime.datetime.now().date().strftime('%Y-%m-%d')

        return item


class DBPipeline(object):

    def __init__(self, mongo_uri, mongo_db, mongo_collection, redis_uri):
        self.mongo_uri = mongo_uri
        self.mongo_db = mongo_db
        self.mongo_collection = mongo_collection
        self.redis_uri = redis_uri

    @classmethod
    def from_crawler(cls, crawler):
        return cls(
            mongo_uri = crawler.settings.get('MONGO_URI'),
            mongo_db = crawler.settings.get('MONGO_DB'),
            mongo_collection= crawler.settings.get('MONGO_COLLECTION'),
            redis_uri = crawler.settings.get('REDIS_INFO_URI')
        )

    def open_spider(self, spider):
        self.redis_pool = redis.ConnectionPool.from_url(self.redis_uri)
        self.redis_client = redis.StrictRedis(connection_pool=self.redis_pool)
        self.mongo_client = pymongo.MongoClient(self.mongo_uri)
        self.mongo_db = self.mongo_client[self.mongo_db]

    def process_item(self, item, spider):
        # name = item.__class__.__name__
        name = item['initial_time']
        redis_key_id = item['链家编号']
        redis_key_totalPrice = 'LianjiaSell:{}:totalPrice'.format(item['链家编号'])
        redis_key_unitPrice = 'LianjiaSell:{}:unitPrice'.format(item['链家编号'])
        redis_key_mixPayment = 'LianjiaSell:{}:mixPayment'.format(item['链家编号'])
        redis_key_follower = 'LianjiaSell:{}:follower'.format(item['链家编号'])
        redis_key_day7visitor = 'LianjiaSell:{}:day7visitor'.format(item['链家编号'])
        redis_key_day30vistor = 'LianjiaSell:{}:day30visitor'.format(item['链家编号'])

        # redis中插入变量数据
        self.redis_client.zadd(redis_key_totalPrice, item['总价'], name)
        self.redis_client.zadd(redis_key_unitPrice, item['单价'], name)
        self.redis_client.zadd(redis_key_mixPayment, item['最低首付'], name)
        self.redis_client.zadd(redis_key_follower, item['房源热度']['关注人数'], name)
        self.redis_client.zadd(redis_key_day7visitor, item['房源热度']['七天带看'], name)
        self.redis_client.zadd(redis_key_day30vistor, item['房源热度']['三十天带看'], name)

        # 使用'链家编号'判断是否在redis中存在,存在则不做操作,不存在则插入id并且在mongodb中写入数据
        if self.redis_client.sismember('LianjiaSell:id', redis_key_id):
            return item
        else:
            self.redis_client.sadd('LianjiaSell:id', redis_key_id)
            # redis数据插入后从item中去除相关变量, 再写入mongodb中
            for key in ['总价', '单价', '最低首付', '房源热度']:
                item.pop(key)
            self.mongo_db[self.mongo_collection].update_one({'链家编号':dict(item)['链家编号']},{'$set':dict(item)},True)
            return item

    def close_spider(self, spider):
        self.mongo_client.close()
        # self.redis_client.shutdown()