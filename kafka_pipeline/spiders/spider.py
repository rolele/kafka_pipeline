# -*- coding: utf-8 -*-

import datetime
import socket

from scrapy.loader.processors import MapCompose, Join
from scrapy.linkextractors import LinkExtractor
from scrapy.spiders import CrawlSpider, Rule
from scrapy.loader import ItemLoader

from kafka_pipeline.items import PropertiesItem


class SpiderSpider(CrawlSpider):
    name = "spider"
    allowed_domains = ["web.local"]
    # Start on the first index page
    start_urls = (
        'http://web.local:9312/properties/index_00000.html',
    )

    # Rules for horizontal and vertical crawling
    rules = (
        #Rule(LinkExtractor()),
        Rule(LinkExtractor(restrict_xpaths='//*[contains(@class,"next")]')),
        Rule(LinkExtractor(restrict_xpaths='//*[@itemprop="url"]'),
             callback='parse_item')
    )

    def parse_item(self, response):
        """ This function parses a property page.

        @url http://web:9312/properties/property_000000.html
        @returns items 1
        @scrapes title price description address image_urls
        @scrapes url project spider server date
        """

        # Create the loader using the response
        l = ItemLoader(item=PropertiesItem(), response=response)

        # Load fields using XPath expressions
        l.add_xpath('title', '//*[@itemprop="name"][1]/text()')
        l.add_xpath('price', './/*[@itemprop="price"][1]/text()')
        l.add_xpath('description', '//*[@itemprop="description"][1]/text()')
        l.add_xpath('address', '//*[@itemtype="http://schema.org/Place"][1]/text()')
        l.add_xpath('image_urls', '//*[@itemprop="image"][1]/@src')

        # Housekeeping fields
        l.add_value('url', response.url)
        l.add_value('project', self.settings.get('BOT_NAME'))
        l.add_value('spider', self.name)

        return l.load_item()
