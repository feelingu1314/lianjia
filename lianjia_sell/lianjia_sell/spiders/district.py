# -*- coding: utf-8 -*-
import json
import scrapy
from lianjia_sell.items import SellItem


class DistrictSpider(scrapy.Spider):
    name = 'district'
    allowed_domains = ['www.lianjia.com']

    def start_requests(self):
        # load lianjia-ershoufang website url
        yield scrapy.Request(url='https://su.lianjia.com/ershoufang/rs/', meta={'city': 'su', 'referer': '1st'},
                             callback=self.parse, dont_filter=True)
        yield scrapy.Request(url='https://sh.lianjia.com/ershoufang/rs/', meta={'city': 'sh', 'referer': '1st'},
                             callback=self.parse, dont_filter=True)

    def parse(self, response):
        print('url:{}, meta:{}'.format(response.url, response.request.meta.items()))
        meta = response.request.meta['city']

        if response.request.meta.get('referer', 'other') == '1st':
            districts = response.css(
                'div.position > dl:nth-child(2) > dd > div:nth-child(1) > div > a::attr(href)'
            ).extract()  # 区域查询(list)
            for district in districts:
                url = 'https://{}.lianjia.com{}'.format(meta, district)
                yield scrapy.Request(url=url, meta={'city': meta, 'district': 'location'}, callback=self.parse, dont_filter=True)

        if response.request.meta.get('district', 'other') == 'location':
            print('meta-->location')
            locations = response.css(
                'body > div:nth-child(11) > div > div.position > dl:nth-child(2) > dd > div:nth-child(1) >'
                'div:nth-child(2) > a::attr(href)'
            ).extract()
            for location in locations:
                url = 'https://{}.lianjia.com{}'.format(meta, location)
                yield scrapy.Request(url=url, meta={'city': meta, 'page': 'index'}, callback=self.parse, dont_filter=True)

        if response.request.meta.get('page', 'other') == 'index':
            print('meta-->index')
            urls = response.css('.leftContent ul li .info.clear')
            for url in urls:
                one_page_url = url.css('.title a::attr(href)').extract_first()
                yield scrapy.Request(url=one_page_url, meta={'city': meta}, callback=self.parse_one, dont_filter=True)

            page_url = response.css(
                'div.leftContent > div.contentBottom.clear > div.page-box.fr > div::attr(page-url)'
            ).extract_first()
            cur_page = response.css(
                'div.leftContent > div.contentBottom.clear > div.page-box.fr > div::attr(page-data)'
            ).extract_first()

            if page_url and cur_page:
                if json.loads(cur_page)['curPage'] < json.loads(cur_page)['totalPage']:
                    page_url = page_url.replace('page','').format(json.loads(cur_page)['curPage']+1)
                    next_page_url = 'https://{}.lianjia.com{}'.format(meta, page_url)
                    yield scrapy.Request(url=next_page_url, meta={'city': meta, 'page': 'index'}, callback=self.parse, dont_filter=True)

    def parse_one(self, response):
        item = SellItem()
        meta = response.request.meta['city']

        house_type = response.css(
            'div.transaction > div.content > ul > li:nth-child(4) > span:nth-child(2)::text'
        ).extract_first()
        if house_type in ['普通住宅', '新式里弄', '老公寓', '别墅']:
            # 基本信息
            title = response.css('body > div.sellDetailHeader > div > div > div.title > h1::text').extract_first()
            sub = response.css('body > div.sellDetailHeader > div > div > div.title > div.sub::text').extract_first()
            total_price = response.css('div.overview > div.content > div.price '
                                     '> span.total::text').extract_first()  # 总价(float)
            unit_price = response.css('div.overview > div.content > div.price > div.text > div.unitPrice '
                                      '> span::text').extract_first()  # 单价(float)
            min_down_payment = response.css('body > div.overview > div.content > div.price > div.text > div.tax '
                                            '> span.taxtext > span:nth-child(1)::text').extract_first()  # 最低首付(float)
            community_name = response.css('div.overview > div.content > div.aroundInfo > div.communityName '
                                     '> a.info::text').extract_first()  # 小区
            district = response.css('div.overview > div.content > div.aroundInfo > div.areaName > span.info '
                                     '> a:nth-child(1)::text').extract_first()  # 区县
            location = response.css('div.overview > div.content > div.aroundInfo > div.areaName > span.info '
                                    '> a:nth-child(2)::text').extract_first()  # 位置
            loop_info = response.css(
                    'div.overview > div.content > div.aroundInfo > div.areaName > span.info::text').extract()[1]  # 环线
            build_time = response.css(
                'div.overview > div.content > div.houseInfo > div.area > div.subInfo::text').extract_first()  # 建造时间
            lj_id = response.css(
                'div.overview > div.content > div.aroundInfo > div.houseRecord > span.info::text').extract_first()  # 链家编号

            # 交易属性
            item['交易属性'] = {}
            trade_attr_items = response.css(
                '#introduction > div > div > div.transaction > div.content > ul > li > span.label::text'
            ).extract()# 标签键
            for i, trade_attr_key in enumerate(trade_attr_items, 1):
                trade_attr_value = response.css(
                    '#introduction > div > div > div.transaction > div.content > ul > li:nth-child({}) > span:nth-child(2)::text'.format(i)
                ).extract_first().strip()
                item['交易属性'].update({trade_attr_key:trade_attr_value})

            # 房源特色
            feature_label_key = response.css(
                'div.m-content > div.box-l > div:nth-child(2) > div > div.tags.clear > div.name::text'
            ).extract_first()  # 标签键
            feature_label_value = response.css(
                'div.m-content > div.box-l > div:nth-child(2) > div > div.tags.clear > div.content > a::text'
            ).extract()  # 标签值
            if feature_label_value:
                item['房源特色'] = {feature_label_key: feature_label_value}
            else:
                item['房源特色'] = {}
            feature_items = response.css(
                'body > div.m-content > div.box-l > div:nth-child(2) > div > div.baseattribute.clear > div.name::text'
            ).extract()  # 房源特色的条目名
            if feature_label_value:
                start = 2  # nth-child的起始值
            else:
                start = 1
            for i, feature_item_key in enumerate(feature_items, start):
                feature_item_value = response.css(
                    'div.m-content > div.box-l > div:nth-child(2) > div > div:nth-child({}) > div.content::text'.format(
                        i)
                ).extract_first().strip()
                item['房源特色'].update({feature_item_key: feature_item_value})

            # 基本属性
            item['基本属性'] = {}
            base_attr_items = response.css(
                '#introduction > div > div > div.base > div.content > ul > li > span::text'
            ).extract()  # 基本属性的条目名
            for i, base_attr_key in enumerate(base_attr_items, 1):
                base_attr_value = response.css(
                    '#introduction > div > div > div.base > div.content > ul > li:nth-child({})::text'.format(i)
                ).extract_first().strip()
                item['基本属性'].update({base_attr_key:base_attr_value})

            # 户型分间
            model_details = response.css('#infoList > div.row > div.col::text').extract()  # 户型分间详细

            # location parameter
            hid = response.css(
                'div.sellDetailHeader > div > div > div.btnContainer::attr(data-lj_action_housedel_id)').extract_first()  # data-lj_action_housedel_id
            rid = response.css(
                'div.sellDetailHeader > div > div > div.btnContainer::attr(data-lj_action_resblock_id)').extract_first()  # data-lj_action_resblock_id

            # 关注情况
            follower = response.css('#favCount::text').extract_first()  # 关注人数(int)

            item['标题'] = title
            item['关键字'] = sub
            item['总价'] = total_price
            item['单价'] = unit_price
            item['最低首付'] = min_down_payment
            item['小区'] = community_name
            item['区县'] = district
            item['位置'] = location
            item['环线信息'] = loop_info
            item['建造时间'] = build_time
            item['链家编号'] = lj_id
            item['城市'] = meta
            item['房源链接'] = response.url
            item['户型分间'] = model_details
            item['房源热度'] = {'关注人数':int(follower)}
            item['小区概况'] = {'hid':hid, 'rid':rid}

            if hid and rid:
                house_stat_url = 'https://{0}.lianjia.com/ershoufang/housestat?hid={1}&rid={2}'.format(meta, rid, hid)  # request时参数错位
                request = scrapy.Request(url=house_stat_url, meta={'city':meta}, callback=self.parse_location, dont_filter=True)
                request.meta['item'] = item
                yield request

    def parse_location(self, response):
        item = response.meta['item']
        jsonresponse = json.loads(response.body_as_unicode())
        location = jsonresponse['data']['resblockPosition']  # 经度,纬度
        avg_price = jsonresponse['data']['resblockCard']['unitPrice']  # 小区均价
        build_num = jsonresponse['data']['resblockCard']['buildNum']  # 楼栋数
        frame_num = jsonresponse['data']['resblockCard']['frameNum']  # 户型数
        build_type = jsonresponse['data']['resblockCard']['buildType']  # 建筑类型
        build_age = jsonresponse['data']['resblockCard']['buildYear']  # 建筑年代
        name = jsonresponse['data']['resblockCard']['name']  # 小区名
        sell_num = jsonresponse['data']['resblockCard']['sellNum']  # 在售二手
        rent_num = jsonresponse['data']['resblockCard']['rentNum']  # 出租房源
        frame_url = jsonresponse['data']['resblockCard']['frameUrl']  # 户型推荐
        view_url = jsonresponse['data']['resblockCard']['viewUrl']  # 小区详情

        item['小区概况'].update({'小区名': name, '均价': float(avg_price), '年代': build_age, '类型': build_type,
                             '楼栋总数': build_num, '户型总数': frame_num, '在售': sell_num,
                             '出租': rent_num, '户型推荐': frame_url, '小区详情': view_url, '经度': float(location.split(',')[0]),
                             '纬度': float(location.split(',')[1])})

        meta = response.request.meta['city']
        house_see_url = 'https://{0}.lianjia.com/ershoufang/houseseerecord?id={1}'.format(meta, item['小区概况']['rid'])
        request = scrapy.Request(url=house_see_url,meta={'city':meta}, callback=self.parse_see, dont_filter=True)
        request.meta['item'] = item
        yield request

    def parse_see(self, response):
        item = response.meta['item']
        jsonresponse = json.loads(response.body_as_unicode())
        day7_visit = jsonresponse['data']['thisWeek']
        day30_visit = jsonresponse['data']['totalCnt']
        item['房源热度'].update({'七天带看':int(day7_visit), '三十天带看':int(day30_visit)})
        yield item
