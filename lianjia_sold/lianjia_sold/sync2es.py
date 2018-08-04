#! Lianjia_Sold/sync2es.py
# synchronize data in MongoDB to ElasticSearch with updating item

from pymongo import MongoClient
from datetime import datetime
from uuid import uuid1
from elasticsearch import Elasticsearch
from elasticsearch.helpers import bulk
# import json


class MongoSyncEs(object):
    def __init__(self):
        self.es_node = '198.181.46.127:9200'
        self.es_index = 'crawler.sold.v2'
        self.es_type = 'info'
        self.mongo_uri = 'mongodb://mongo:mongo2018@140.143.237.148:27020/?replicaSet=rs-27020'
        self.mongo_db = 'scrapy-lianjia_sold'
        self.count = 0
        self.query = datetime.now().date().strftime('%Y-%m-%d')

    def connection_mongo(self):
        conn = MongoClient(self.mongo_uri)
        db = conn[self.mongo_db]
        return db

    def connection_es(self):
        es = Elasticsearch([self.es_node])
        if not es.indices.exists(index=self.es_index): es.indices.create(index=self.es_index)
        return es

    def mongo_data_process(self, data):
        # format data collected from mongo
        if data['浏览'] == 'NA':
            data['浏览'] = 0
        if data['挂牌价格'] == 'NA':
            data['挂牌价格'] = 0
        if data['基本属性']['建成年代'] == '未知':
            data['基本属性']['建成年代'] = 0
        else:
            data['基本属性']['建成年代'] = int(data['基本属性']['建成年代'])
        if data['基本属性']['建筑面积']:
            data['基本属性']['建筑面积'] = float(data['基本属性']['建筑面积'][:-1])
        else:
            data['基本属性']['建筑面积'] = float(0)
        if not data['基本属性']['产权年限'] == '未知':
            data['基本属性']['产权年限'] = int(data['基本属性']['产权年限'][:-1])
        else:
            data['基本属性']['产权年限'] = 0
        if not data['小区概况']['年代'] == '未知':
            data['小区概况']['年代'] = int(data['小区概况']['年代'][:-1])
        else:
            data['小区概况']['年代'] = 0
        if data['小区概况']['楼栋总数']:
            data['小区概况']['楼栋总数'] = int(data['小区概况']['楼栋总数'][:-1])
        if data['成交时间']:
            data['成交时间'] = data['成交时间'].replace('.', '-')

        return data

    def es_data_create(self, data):
        doc = {
            '_op_type': 'create',
            '_index': self.es_index,
            '_type': self.es_type,
            '_id': uuid1(),
            '_source': self.mongo_data_process(data)
        }
        yield doc

    def es_pipeline_datetime(self, es):
        id = 1
        es.ingest.put_pipeline(
            id=id,
            body={
                "description": "crawler.lianjia",
                "processors": [
                    {
                        "date": {
                            "field": "initial_time",
                            "target_field": "@timestamp",
                            "formats": ["Y-M-d H:m:s"],
                            "timezone": "Asia/Shanghai"
                        }
                    }
                ]
            }
        )
        return id

    def start(self):
        db = self.connection_mongo()
        es = self.connection_es()
        with open('sync_data.txt', 'a') as f:
            f.write('+++{}\n'.format(datetime.now()))
            for collection in ['sh-sold', 'su-sold']:
                cursor = db[collection].find({'initial_time':{'$regex':self.query}},
                                             {'_id':0,'基本属性.套内面积':0,'基本属性.供暖方式':0,'小区概况.hid':0,
                                              '小区概况.rid':0,'小区概况.其他户型':0,'小区概况.在售链接':0,'小区概况.成交链接':0,
                                              '小区概况.小区详情':0,'小区概况.出租链接':0})
                for data in cursor:
                    bulk(es, self.es_data_create(data), pipeline=self.es_pipeline_datetime(es))
                    self.count += 1
                    f.write(data.get('房源链接')+'\n')
            f.write('+++total data: {}\n'.format(self.count))


task = MongoSyncEs()
task.start()
