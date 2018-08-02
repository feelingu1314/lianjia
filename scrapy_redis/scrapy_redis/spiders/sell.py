# -*- coding: utf-8 -*-
import scrapy
import json
from scrapy.linkextractors import LinkExtractor
from scrapy.spiders import Rule
from scrapy_redis.spiders import RedisCrawlSpider, RedisSpider
from lianjia_redis.items import SellItem


class SellSpider(RedisCrawlSpider):
    name = 'sell'
    allow_domains = ['sh.lianjia.com', 'su.lianjia.com']

    redis_key = 'SellSpider:start_urls'
    # start_urls = ['http://www.lianjia.com/']

    rules = (
        Rule(LinkExtractor(allow=('/ershoufang/\d{12}.html',)), callback='parse_one', follow=False),
        Rule(LinkExtractor(allow=('/ershoufang/',)), follow=True),
    )

    def parse_one(self, response):
        item = SellItem()
        meta = response.url[8:10]

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
            item['房源链接'] = response.url
            item['户型分间'] = model_details
            item['房源热度'] = {'关注人数':int(follower)}
            item['小区概况'] = {'hid':hid, 'rid':rid}
            item['城市'] = meta

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
