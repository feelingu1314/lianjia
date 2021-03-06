# import link from MongoDB to Redis "lianjia_xiaoqu:link"
# run it before spider running for looking up link crawled one day ago

import redis
import time
from pymongo import MongoClient


if __name__ == '__main__':
    # MongoDB
    mongo_uri = 'mongodb://lianjia:lianjia@127.0.0.1:27017/lianjia'
    mongo_db = 'lianjia'
    collection = ['sh_xiaoqu', 'su_xiaoqu']
    client = MongoClient(mongo_uri)
    db = client[mongo_db]

    # Redis
    redis_uri = 'redis://:houwei2019@127.0.0.1:6379/1'
    set_name_from = 'lianjia_xiaoqu_day:link'
    set_name_to = 'lianjia_xiaoqu:link'
    redis_pool = redis.ConnectionPool.from_url(redis_uri)
    redis_client = redis.StrictRedis(connection_pool=redis_pool)

    start_time = time.time()
    redis_client.delete(set_name_from)
    redis_client.delete(set_name_to)
    for col in collection:
        for i in db[col].find({}, {'_id': 0, 'community': 1, 'link': 1}).batch_size(1000):
            redis_client.sadd(set_name_from, '%s+%s' % (i['community'], i['link']))
    client.close()
    redis_pool.disconnect()
    end_time = time.time()
    print(end_time-start_time)