# -*- coding: utf-8 -*-
import scrapy
from lianjia_xiaoqu import LianjiaXiaoquItem


class ShSpider(scrapy.Spider):
    name = 'sh'
    allowed_domains = ['sh.lianjia.com']
    start_url = 'https://sh.lianjia.com'
    # start_urls = ['http://sh.lianjia.com/']

    def start_requests(self):
        yield scrapy.Request(url=self.start_url + '/xiaoqu/rs/', callback=self.parse, dont_filter=True)

    def parse(self, response):
        # parsing by page
        links = response.xpath('/html/body/div[4]/div[1]/ul/li/div[1]/div[1]/a/@href').getall()
        if links:
            for idx, link in enumerate(links, 1):
                item = LianjiaXiaoquItem()
                item['codeComm'] = response.xpath('/html/body/div[4]/div[1]/ul/li[%d]/@data-id' % idx).get()
                item['community'] = response.xpath('/html/body/div[4]/div[1]/ul/li[%d]/div[1]/div[1]/a/text()' % idx).get()
                item['priceUnit'] = response.xpath('/html/body/div[4]/div[1]/ul/li[%d]/div[2]/div[1]/div[1]/span/text()' % idx).get()
                item['sale'] = response.xpath('/html/body/div[4]/div[1]/ul/li[%d]/div[2]/div[2]/a/span/text()' % idx).get()
                item['deal'] = response.xpath('/html/body/div[4]/div[1]/ul/li[%d]/div[1]/div[2]/a[1]/text()' % idx).get()  # i[-3:-1]
                item['rent'] = response.xpath('/html/body/div[4]/div[1]/ul/li[%d]/div[1]/div[2]/a[2]/text()' % idx).get()  # i[0:-5]
                item['district'] = response.xpath('/html/body/div[4]/div[1]/ul/li[%d]/div[1]/div[3]/a[1]/text()' % idx).get()
                item['position'] = response.xpath('/html/body/div[4]/div[1]/ul/li[%d]/div[1]/div[3]/a[2]/text()' % idx).get()
                item['age'] = response.xpath('/html/body/div[4]/div[1]/ul/li[%d]/div[1]/div[3]/text()[4]' % idx).get()  # strip(), i[:4] -> int
                item['label'] = response.xpath('/html/body/div[4]/div[1]/ul/li[%d]/div[1]/div[5]/span/text()' % idx).get() \
                    if response.xpath('/html/body/div[4]/div[1]/ul/li[%d]/div[1]/div[5]/span/text()' % idx).get() else ''
                item['link'] = link
                item['city'] = 'sh'
                yield scrapy.Request(url=link, callback=self.parse_detail, meta={'item': item}, dont_filter=True)
                # yield scrapy.Request(url=link, callback=self.parse_detail, dont_filter=True)

        curPage = response.xpath('/html/body/div[4]/div[1]/div[3]/div[2]/div/@page-data').re('"curPage":(.*?)}')
        urlPage = response.xpath('/html/body/div[4]/div[1]/div[3]/div[2]/div/@page-url').get()
        if (curPage and urlPage) and int(curPage[0]) < 100:
            # print('parse_nextPage:', self.start_url+urlPage.replace('{page}', str(int(curPage[0])+1)))
            yield scrapy.Request(url=self.start_url+urlPage.replace('{page}', str(int(curPage[0])+1)), callback=self.parse,
                                 dont_filter=True)

    def parse_location(self, response):
        # parsing by location
        page = response.meta.get('page', 'ignore')
        if page == 'index':
            districtLinks = response.xpath('/html/body/div[3]/div/div[1]/dl[2]/dd/div[1]/div/a/@href').getall()
            for link in districtLinks:
                yield scrapy.Request(url=self.start_url+link, callback=self.parse_location, dont_filter=True)
        else:
            locLinks = response.xpath('/html/body/div[3]/div/div[1]/dl[2]/dd/div[1]/div[2]/a/@href').getall()
            for link in locLinks:
                yield scrapy.Request(url=self.start_url+link, callback=self.parse, dont_filter=True)

    def parse_metro(self, response):
        # parsing by metro
        page = response.meta.get('page', 'ignore')
        if page == 'index':
            lineLinks = response.xpath('/html/body/div[3]/div/div[1]/dl[2]/dd/div[2]/div/a/@href').getall()
            for link in lineLinks:
                yield scrapy.Request(url=self.start_url+link, callback=self.parse_metro, dont_filter=True)
        else:
            stationLinks = response.xpath('/html/body/div[3]/div/div[1]/dl[2]/dd/div[2]/div[2]/a/@href').getall()
            for link in stationLinks:
                yield scrapy.Request(url=self.start_url+link, callback=self.parse, dont_filter=True)

    def parse_detail(self, response):
        item = response.meta['item']
        item['desc'] = response.xpath('/html/body/div[4]/div/div[1]/div/text()').get()
        item['focus'] = response.xpath('/html/body/div[4]/div/div[2]/div/span/text()').get()
        yield item