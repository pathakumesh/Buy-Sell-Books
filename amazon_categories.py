# -*- coding: utf-8 -*-
import glob
import os
import csv
import json
import scrapy
import datetime
import pytz
from scrapy.crawler import CrawlerProcess
import logging as log

class ExtractItem(scrapy.Item):
    # define the fields for your item here like:
    category = scrapy.Field()
    url = scrapy.Field()
    pass    

class AmazonCategorySpider(scrapy.Spider):
    name = "amazon_category_spider"
    allowed_domains = ["amazon.com"]
    
    def start_requests(self):
        amazon_url = "https://www.amazon.com/s/ref=sr_nr_i_0?srs=9187220011&fst=as%3Aoff&rh=i%3Atradein-aps%2Ck%3Atextbooks%2Ci%3Astripbooks&keywords=textbooks&ie=UTF8&qid=1534695454"
        yield scrapy.Request(
            url= amazon_url,
            callback=self.parse,
            dont_filter = True
        )

    def parse(self, response):
        
        categories = response.xpath('//li[@class="s-ref-indent-one"]/following-sibling::ul[1]/div//span/a[@class="a-link-normal s-ref-text-link"]/@href').extract()
        # print(categories)
        if categories:
            for category_link in categories:
                if 'https://' not in category_link:
                    category_link = "https://www.amazon.com" + category_link
                
                yield scrapy.Request(
                    url= category_link,
                    callback=self.parse,
                    dont_filter = True
                )
        else:
            category_block = response.xpath('//span[@id="s-result-count"]/span/a/text()').extract()
            category = ':'.join(category_block)
            # if not category and 'To discuss automated access to Amazon data please contact' in response.text:
            if not category:
                yield scrapy.Request(
                    url= response.url,
                    callback=self.parse,
                    dont_filter = True
                )
            else:
                item = ExtractItem()
                item['category'] = category
                item['url'] = response.url
                yield item
            

def run_spider(no_of_threads, request_delay, timeout, no_of_retries):

    log.info('[+++++] Starting with Threads:{}, Timeout:{}, Retries:{} and Delay between Request: {}'
            .format(no_of_threads, timeout,no_of_retries,request_delay)
            )

    rotating_proxy_list = 'helper_files/proxy.txt'
    settings = {"DOWNLOADER_MIDDLEWARES":{
                            'scrapy.downloadermiddlewares.useragent.UserAgentMiddleware': None,
                            'scrapy_fake_useragent.middleware.RandomUserAgentMiddleware': 400,
                            'scrapy.downloadermiddlewares.retry.RetryMiddleware': 90,
                            'rotating_proxies.middlewares.RotatingProxyMiddleware': 610,
                            'rotating_proxies.middlewares.BanDetectionMiddleware': 620,
                },
                'ITEM_PIPELINES':{
                            'pipelines_amazon_category.ExtractPipeline': 300,
                },
                'DOWNLOAD_DELAY': request_delay,
                'DOWNLOAD_TIMEOUT': timeout,
                'CONCURRENT_REQUESTS':no_of_threads,
                'CONCURRENT_REQUESTS_PER_DOMAIN': no_of_threads,
                'RETRY_TIMES':no_of_retries,
                'RANDOM_UA_PER_PROXY': True,
                'ROTATING_PROXY_PAGE_RETRY_TIMES':no_of_retries,
                # 'RETRY_HTTP_CODES':[403, 429],
                'ROTATING_PROXY_LIST_PATH':rotating_proxy_list,
                'ROTATING_PROXY_BAN_POLICY': 'pipelines_amazon_category.BanPolicy',
                'LOG_ENABLED':False,

    }
    # -----------------------Run without Proxy----------------------------------------
    # settings = {
    #             'ITEM_PIPELINES':{
    #                         'pipelines.ExtractPipeline': 300,
    #             },
    #             'USER_AGENT': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_11_6) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/65.0.3325.181 Safari/537.36',
    #             'DOWNLOAD_DELAY': request_delay,
    #             'RETRY_TIMES':10,
    #             'RETRY_HTTP_CODES':[429],
    #             'LOG_ENABLED':False
    # }

    process = CrawlerProcess(settings)
    process.crawl(AmazonCategorySpider)

    process.start()

if __name__ == '__main__':
    no_of_threads = 80
    request_delay = 0.01
    timeout = 45
    no_of_retries = 10
    run_spider(no_of_threads, request_delay, timeout, no_of_retries)

