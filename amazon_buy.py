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
# log.getLogger('scrapy.core.engine').propagate = False
# log.getLogger('scrapy.core.scraper').propagate = False
# log.getLogger('scrapy.statscollectors').propagate = True
# log.getLogger('root').propagate = False

# log.getLogger('rotating_proxies').propagate = False

INPUT_PATH = "/home/FM/results/Trade_In/Amazon_ISBN/*"
# INPUT_PATH = '/Users/PathakUmesh/Programming_stuffs/NIGAM/ISBN/*'

class ExtractItem(scrapy.Item):
    # define the fields for your item here like:
    title = scrapy.Field()
    isbn10 = scrapy.Field()
    isbn13 = scrapy.Field()
    
    purchase_cost = scrapy.Field()
    shipping_cost = scrapy.Field()
    processing_cost = scrapy.Field()
    net_buying_cost = scrapy.Field()
    # url_buy = scrapy.Field()
    pass    

class AmazonBuySpider(scrapy.Spider):
    name = "amazon_buy_spider"
    allowed_domains = ["amazon.com"]

    def __init__(self, no_of_retries):
        self.no_of_retries = no_of_retries

    def read_isbns(self):
        list_of_files = glob.glob(INPUT_PATH)
        file_path = max(list_of_files, key=os.path.getctime)
        
        # file_path = '/Users/PathakUmesh/Programming_stuffs/NIGAM/ISBN/Amazon_Trade-In_ISBN_19Aug2018_19hr23min.csv'

        log.info('[+++++] Starting from input file path: /home/FM/results/Trade_In/Amazon_ISBN/')
        log.info('[+++++] Starting with input file name: {}'.format(file_path.rsplit('/',1)[-1]))
        
        isbns = list()
        with open(file_path, 'r') as csvfile:
            csvreader = csv.reader(csvfile)
            for i, buy_row in enumerate(csvreader):
                if i == 0:
                    continue
                isbns.append({
                    'title': str(buy_row[0]),
                    'isbn10': str(buy_row[1]),
                    'isbn13': str(buy_row[2]),
                })            
        
        return isbns
        
    def format_isbn(self, isbn, length):
        prefix = ''
        for i in range(len(isbn), length):
            prefix += '0'
        return prefix + isbn
    
    def start_requests(self):
        isbns = self.read_isbns()
        isbn_url = "https://www.amazon.com/gp/offer-listing/{}"
        for page_number in range(len(isbns)):
            isbn10 = isbns[page_number]['isbn10']
            
            if isbn10 and len(isbn10) < 10:
                isbn10 = self.format_isbn(isbn10, 10)
            
            yield scrapy.Request(
                url= isbn_url.format(isbn10),
                callback=self.parse,
                dont_filter = True,
                meta = {
                    'input_item': isbns[page_number],
                    'retry': 0,
                }
            )

    def parse(self, response):
        try:
            input_item = response.meta['input_item']
            retry = response.meta['retry']
            table_data = response.xpath('//div[@class="a-row a-spacing-mini olpOffer"]')
            extracted_data = list()
            for row in table_data:
                if row.xpath('div/div/span[contains(text(), "Used")]'):
                    purchase_cost = row.xpath('div[@class="a-column a-span2 olpPriceColumn"]/span/text()').extract_first()
                    if purchase_cost:
                        purchase_cost = purchase_cost.strip()
                    shipping_cost = row.xpath('div[@class="a-column a-span2 olpPriceColumn"]//span[@class="olpShippingPrice"]/text()').extract_first()
                    extracted_data.append({
                        'shipping_cost': float(shipping_cost.replace('$', '').replace(',', '')) if shipping_cost else 0,
                        'purchase_cost': float(purchase_cost.replace('$', '').replace(',', '')),
                    })
            if not extracted_data and retry < self.no_of_retries:
                log.info('[+++] Retrying for url {} retry no: {}'.format(response.url, retry+1))
                yield scrapy.Request(
                    url=response.url,
                    callback=self.parse,
                    dont_filter=True,
                    meta={
                        'input_item': input_item,
                        'retry': retry + 1,
                    }
                )
            else:
                item = ExtractItem()
                item['title'] = input_item['title']
                item['isbn10'] = input_item['isbn10']
                item['isbn13'] = input_item['isbn13']
                
                item['purchase_cost'] = 'N/A'
                item['shipping_cost'] = 'N/A'
                item['processing_cost'] = 'N/A'
                item['net_buying_cost'] = 'N/A'
                
                if extracted_data:
                    final_price = min(extracted_data, key=lambda x:x['purchase_cost'])

                    purchase_cost = final_price['purchase_cost']
                    shipping_cost = final_price['shipping_cost']
                    
                    item['purchase_cost'] = purchase_cost
                    item['shipping_cost'] = shipping_cost

                    item['processing_cost'] = 2
                    item['net_buying_cost'] = float('%.2f' % (float(purchase_cost)  + shipping_cost + 2))
                
                log.info('[+++] Status code {} for url {}'.format(response.status, response.url))
                yield item            
        except Exception as ex:
            print('Exception:', str(ex))
            print(response.url)
            pass



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
                            'pipelines_amazon_buy.ExtractPipeline': 300,
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
                'ROTATING_PROXY_BAN_POLICY': 'pipelines_amazon_buy.BanPolicy',
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
    process.crawl(AmazonBuySpider, no_of_retries = no_of_retries)

    process.start()

if __name__ == '__main__':
    no_of_threads = 80
    request_delay = 0.01
    timeout = 45
    no_of_retries = 10
    run_spider(no_of_threads, request_delay, timeout, no_of_retries)

