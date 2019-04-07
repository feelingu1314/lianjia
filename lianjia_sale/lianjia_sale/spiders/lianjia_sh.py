# -*- coding: utf-8 -*-
import scrapy
import re
from lianjia_sale.items import LianjiaSaleItem


class LianjiaShSpider(scrapy.Spider):
    name = 'lianjia_sh'
    allowed_domains = ['sh.lianjia.com']
    start_url = 'https://sh.lianjia.com'
    # start_urls = ['http://sh.lianjia.com/']

    def start_requests(self):
        yield scrapy.Request(url=self.start_url+'/ershoufang/rs/', callback=self.parse, dont_filter=True)
        yield scrapy.Request(url=self.start_url+'/ershoufang/rs/', callback=self.parse_location, meta={'page': 'index'}, dont_filter=True)
        yield scrapy.Request(url=self.start_url+'/ershoufang/rs/', callback=self.parse_metro, meta={'page': 'index'}, dont_filter=True)

    def parse(self, response):
        # parsing by page
        links = response.xpath('//*[@id="content"]/div[1]/ul/li/a/@href').getall()
        if links:
            for idx, link in enumerate(links, 1):
                item = LianjiaSaleItem()
                info = response.xpath('//*[@id="content"]/div[1]/ul/li[%d]/div[1]/div[4]/text()' % idx).get().split('/')
                item['focus'] = re.search('\d+', info[0])[0]
                item['watch'] = re.search('\d+', info[1])[0]
                item['release'] = info[2]
                item['link'] = link
                # print('parse_detailPage:', link)
                yield scrapy.Request(url=link, callback=self.parse_detail, meta={'item': item}, dont_filter=True)

        curPage = response.xpath('//*[@id="content"]/div[1]/div[8]/div[2]/div/@page-data').re('"curPage":(.*?)}')
        urlPage = response.xpath('//*[@id="content"]/div[1]/div[8]/div[2]/div/@page-url').get()
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
        item['label'] = response.xpath('/html/body/div[3]/div/div/div[1]/div/text()').get()
        item['priceTotal'] = response.xpath('/html/body/div[5]/div[2]/div[2]/span[1]/text()').get()
        item['priceUnit'] = response.xpath('/html/body/div[5]/div[2]/div[2]/div[1]/div[1]/span/text()').get()
        item['community'] = response.xpath('/html/body/div[5]/div[2]/div[4]/div[1]/a[1]/text()').get()
        item['codeComm'] = response.xpath('/html/body/div[5]/div[2]/div[4]/div[1]/a[1]/@href').re('\d+')
        item['district'] = response.xpath('/html/body/div[5]/div[2]/div[4]/div[2]/span[2]/a[1]/text()').get()
        item['position'] = response.xpath('/html/body/div[5]/div[2]/div[4]/div[2]/span[2]/a[2]/text()').get()
        item['loop'] = response.xpath('/html/body/div[5]/div[2]/div[4]/div[2]/span[2]/text()[2]').get()
        item['code'] = response.xpath('/html/body/div[5]/div[2]/div[4]/div[4]/span[2]/text()').get()
        item['orient'] = response.xpath('/html/body/div[5]/div[2]/div[3]/div[2]/div[1]/text()').get()
        item['area'] = response.xpath('/html/body/div[5]/div[2]/div[3]/div[3]/div[1]/text()').get()
        item['room'] = response.xpath('/html/body/div[5]/div[2]/div[3]/div[1]/div[1]/text()').get()
        item['city'] = 'sh'
        yield item