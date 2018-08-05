# -*- coding: utf-8 -*-

# Scrapy settings for lianjia_sold project
#
# For simplicity, this file contains only settings considered important or
# commonly used. You can find more settings consulting the documentation:
#
#     https://doc.scrapy.org/en/latest/topics/settings.html
#     https://doc.scrapy.org/en/latest/topics/downloader-middleware.html
#     https://doc.scrapy.org/en/latest/topics/spider-middleware.html

BOT_NAME = 'lianjia_sold'

SPIDER_MODULES = ['lianjia_sold.spiders']
NEWSPIDER_MODULE = 'lianjia_sold.spiders'

# MongoDB Connection
MONGO_URI = 'mongodb://192.168.1.59:27017'
MONGO_DB = 'lianjia'

# ElasticSearch Connection
ES_NODE = '192.168.1.59:9200'
ES_INDEX = 'lianjia.ershoufang.sold'
ES_TYPE = 'info'

# Redis Connection
REDIS_URI = 'redis://192.168.1.59:6379/1'

# Crawl responsibly by identifying yourself (and your website) on the user-agent
USER_AGENT = 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/65.0.3325.181 Safari/537.36'

# Obey robots.txt rules
ROBOTSTXT_OBEY = False

# Configure maximum concurrent requests performed by Scrapy (default: 16)
CONCURRENT_REQUESTS = 16

# Configure a delay for requests for the same website (default: 0)
# See https://doc.scrapy.org/en/latest/topics/settings.html#download-delay
# See also autothrottle settings and docs
DOWNLOAD_TIMEOUT = 10
# DOWNLOAD_DELAY = 0
# DOWNLOADER_CLIENT_TLS_METHOD = "TLSv1.0"
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
   # 'Host': 'sh.lianjia.com',
   'Upgrade-Insecure-Requests': 1,
   'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/67.0.3396.99 Safari/537.36',
}

# Enable or disable spider middlewares
# See https://doc.scrapy.org/en/latest/topics/spider-middleware.html
#SPIDER_MIDDLEWARES = {
#    'Lianjia_Sold.middlewares.LianjiaSoldSpiderMiddleware': 543,
#}

# Enable or disable downloader middlewares
# See https://doc.scrapy.org/en/latest/topics/downloader-middleware.html
DOWNLOADER_MIDDLEWARES = {
   # 'Lianjia_Sold.middlewares.LianjiaSoldDownloaderMiddleware': 543,
    'lianjia_sold.middlewares.FilterDownloaderMiddleware': 500,
   # 'Lianjia_Sold.middlewares.IgnoreDuplicateDownloaderMiddleware': 500,
}

# Enable or disable extensions
# See https://doc.scrapy.org/en/latest/topics/extensions.html
#EXTENSIONS = {
#    'scrapy.extensions.telnet.TelnetConsole': None,
#}

# Configure item pipelines
# See https://doc.scrapy.org/en/latest/topics/item-pipeline.html
ITEM_PIPELINES = {
   'lianjia_sold.pipelines.ItemPipeline': 300,
   'lianjia_sold.pipelines.MongoPipeline': 350,
   'lianjia_sold.pipelines.ElasticSearchPipeline': 400,
}

# Enable and configure log
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
