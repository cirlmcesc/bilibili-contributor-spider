# -*- coding: utf-8 -*-

# Define here the models for your scraped items
#
# See documentation in:
# https://doc.scrapy.org/en/latest/topics/items.html

import scrapy


class VideoItem(scrapy.Item):
    comment = scrapy.Field()
    typeid = scrapy.Field()
    play = scrapy.Field()
    pic = scrapy.Field()
    subtitle = scrapy.Field()
    description = scrapy.Field()
    copyright = scrapy.Field()
    title = scrapy.Field()
    review = scrapy.Field()
    author = scrapy.Field()
    mid = scrapy.Field()
    created = scrapy.Field()
    length = scrapy.Field()
    video_review = scrapy.Field()
    favorites = scrapy.Field()
    aid = scrapy.Field()
    hide_click = scrapy.Field()
    typename = scrapy.Field()
    share = scrapy.Field()
