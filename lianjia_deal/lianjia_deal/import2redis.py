# import link from MongoDB to Redis "lianjia_deal:link"
# run it before spider running

import redis
import time
from pymongo import MongoClient


def connect_mongo(db, collection):
    return db[collection].find({}, {'_id': 0, 'link': 1})


def connect_redis():
    pass


if __name__ == '__main__':
    # MongoDB
    # mongo_uri = 'mongodb://106.13.38.208:27017'
    mongo_uri = 'mongodb://127.0.0.1:27017'
    mongo_db = 'lianjia'
    collection = 'sh_chengjiao'
    client = MongoClient(mongo_uri)
    db = client[mongo_db]

    # Redis
    # redis_uri = 'redis://106.13.38.208:6379/1'
    redis_uri = 'redis://127.0.0.1:6379/1'
    set_name = 'lianjia_deal:link'
    redis_pool = redis.ConnectionPool.from_url(redis_uri)
    redis_client = redis.StrictRedis(connection_pool=redis_pool)

    start_time = time.time()
    for i in db[collection].find({}, {'_id': 0, 'link': 1}).batch_size(1000):
        redis_client.sadd(set_name, i['link'])
    client.close()
    redis_pool.disconnect()
    end_time = time.time()
    print(end_time-start_time)