# -*- coding: utf-8 -*-

# Scrapy settings for house project
#
# For simplicity, this file contains only settings considered important or
# commonly used. You can find more settings consulting the documentation:
#
#     https://doc.scrapy.org/en/latest/topics/settings.html
#     https://doc.scrapy.org/en/latest/topics/downloader-middleware.html
#     https://doc.scrapy.org/en/latest/topics/spider-middleware.html

BOT_NAME = 'lianjia_sell'

SPIDER_MODULES = ['lianjia_sell.spiders']
NEWSPIDER_MODULE = 'lianjia_sell.spiders'

# MongoDB Connection
MONGO_URI = 'mongodb://192.168.1.59:27017'
MONGO_DB = 'lianjia'

# ElasticSearch Connection
ES_NODE = '192.168.1.59:9200'
ES_INDEX = 'lianjia.ershoufang.sell'
ES_TYPE = 'info'

# Redis Connection
REDIS_URI = 'redis://192.168.1.59:6379/2'

# Crawl responsibly by identifying yourself (and your website) on the user-agent
USER_AGENT = 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/65.0.3325.181 Safari/537.36'

# Obey robots.txt rules
ROBOTSTXT_OBEY = False

# Configure maximum concurrent requests performed by Scrapy (default: 16)
CONCURRENT_REQUESTS = 16

# Configure a delay for requests for the same website (default: 0)
# See https://doc.scrapy.org/en/latest/topics/settings.html#download-delay
# See also autothrottle settings and docs
#DOWNLOAD_DELAY = 3
# The download delay setting will honor only one of:
#CONCURRENT_REQUESTS_PER_DOMAIN = 16
#CONCURRENT_REQUESTS_PER_IP = 16

# Disable cookies (enabled by default)
#COOKIES_ENABLED = False

# Disable Telnet Console (enabled by default)
TELNETCONSOLE_ENABLED = False

# Override the default request headers:
DEFAULT_REQUEST_HEADERS = {
   'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8',
   'Accept-Encoding': 'gzip, deflate, br',
   'Accept-Language': 'zh-CN,zh;q=0.9',
   'Cache-Control': 'max-age=0',
   'Connection': 'keep-alive',
   # 'Cookie': 'TY_SESSION_ID=9cc78549-d450-46fb-9fec-f338fc511391; lianjia_uuid=654aa765-73ba-42e2-8938-057aca4d3453; Hm_lvt_9152f8221cb6243a53c83b956842be8a=1533277472; _smt_uid=5b63f520.16263e4b; UM_distinctid=164fe7585fb3ca-085482bfcfee19-47e1039-e1000-164fe7585fc4cc; _jzqc=1; _ga=GA1.2.1130217923.1533277476; all-lj=26155dc0ee17bc7dec4aa8e464d36efd; _qzjc=1; _jzqx=1.1533455264.1533474327.2.jzqsr=sh%2Elianjia%2Ecom|jzqct=/ershoufang/.jzqsr=sh%2Elianjia%2Ecom|jzqct=/chengjiao/; select_city=310000; lianjia_ssid=e1e7116b-ab9b-4f8f-a541-3c7373658cde; TY_SESSION_ID=521545cd-98d6-4392-96db-352fcd8ed617; CNZZDATA1253492439=394747494-1533275316-%7C1534317251; CNZZDATA1254525948=1562899366-1533275875-%7C1534320110; CNZZDATA1255633284=706590168-1533273914-%7C1534316969; CNZZDATA1255604082=263584560-1533273007-%7C1534320688; _jzqa=1.1371755447818459000.1533277473.1533524547.1534322104.6; _jzqckmp=1; Hm_lpvt_9152f8221cb6243a53c83b956842be8a=1534322106; _gid=GA1.2.208427177.1534322106; _qzja=1.1022939229.1533278401750.1533524546533.1534322104427.1534322104427.1534322106323.0.0.0.25.6; _qzjb=1.1534322104427.2.0.0.0; _qzjto=2.1.0; _jzqb=1.2.10.1534322104.1',
   # 'Host': 'sh.lianjia.com',
   'Upgrade-Insecure-Requests': 1,
   'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/67.0.3396.99 Safari/537.36',
}

# Enable or disable spider middlewares
# See https://doc.scrapy.org/en/latest/topics/spider-middleware.html
#SPIDER_MIDDLEWARES = {
#    'house.middlewares.HouseSpiderMiddleware': 543,
#}

# Enable or disable downloader middlewares
# See https://doc.scrapy.org/en/latest/topics/downloader-middleware.html
DOWNLOADER_MIDDLEWARES = {
   # 'Lianjia_Sell.middlewares.LianjiaSellDownloaderMiddleware': 543,
   'lianjia_sell.middlewares.FilterDownloaderMiddleware':500,

}

# Enable or disable extensions
# See https://doc.scrapy.org/en/latest/topics/extensions.html
#EXTENSIONS = {
#    'scrapy.extensions.telnet.TelnetConsole': None,
#}

# Configure item pipelines
# See https://doc.scrapy.org/en/latest/topics/item-pipeline.html
ITEM_PIPELINES = {
   'lianjia_sell.pipelines.SellPipeline': 300,
   'lianjia_sell.pipelines.MongoPipeline': 350,
   'lianjia_sell.pipelines.ElasticSearchPipeline': 400,

}

# Enable and Configure log
LOG_LEVEL = 'INFO'

# Enable and configure the AutoThrottle extension (disabled by default)
# See https://doc.scrapy.org/en/latest/topics/autothrottle.html
#AUTOTHROTTLE_ENABLED = True
# The initial download delay
#AUTOTHROTTLE_START_DELAY = 5
# The maximum download delay to be set in case of high latencies
#AUTOTHROTTLE_MAX_DELAY = 60
# The average number of requests Scrapy should be sending in parallel to
# each remote server
#AUTOTHROTTLE_TARGET_CONCURRENCY = 1.0
# Enable showing throttling stats for every response received:
#AUTOTHROTTLE_DEBUG = False

# Enable and configure HTTP caching (disabled by default)
# See https://doc.scrapy.org/en/latest/topics/downloader-middleware.html#httpcache-middleware-settings
#HTTPCACHE_ENABLED = True
#HTTPCACHE_EXPIRATION_SECS = 0
#HTTPCACHE_DIR = 'httpcache'
#HTTPCACHE_IGNORE_HTTP_CODES = []
#HTTPCACHE_STORAGE = 'scrapy.extensions.httpcache.FilesystemCacheStorage'
