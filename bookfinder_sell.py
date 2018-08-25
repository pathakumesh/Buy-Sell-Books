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
    vendor_sell = scrapy.Field()
    selling_price = scrapy.Field()
    url_sell = scrapy.Field()
    pass    

class BookScouterSellSpider(scrapy.Spider):
    name = "book_scouter_sell_spider"
    allowed_domains = ["bookscouter.com"]

    def read_isbns(self):
        list_of_files = glob.glob(INPUT_PATH)
        file_path = max(list_of_files, key=os.path.getctime)

        # file_path = '/Users/PathakUmesh/Programming_stuffs/NIGAM/ISBN/Amazon_Trade-In_ISBN_19Aug2018_09hr36min.csv'

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
        
    def start_requests(self):
        isbns = self.read_isbns()
        isbn_url = "https://www.bookfinder.com/buyback/affiliate/{}.mhtml"

        headers = {
            'Referer': "https://www.bookfinder.com/buyback"
        }

        for page_number in range(len(isbns)):
            yield scrapy.Request(
                url= isbn_url.format(isbns[page_number]['isbn13']),
                callback=self.parse,
                headers =headers,
                dont_filter = True
            )

    def parse(self, response):
        try:
            json_data = json.loads(response.text)
            title = json_data['title']
            isbn10 = json_data['isbn10']
            isbn13 = json_data['isbn13']
            offers = json_data['offers']
            for _,offer_detail in offers.items():
                if offer_detail.get('offer'):
                    item = ExtractItem()
                    book_price = float(offer_detail['offer'])
                    vendor_sell = offer_detail['bookstore_display']
                    url_sell = offer_detail['url']

                    item['title'] = title
                    item['isbn10'] = isbn10
                    item['isbn13'] = isbn13
                    item['vendor_sell'] = vendor_sell
                    item['selling_price'] = book_price 
                    item['url_sell'] = url_sell
                    yield item
            log.info('[+++] Status code {} for url {}'.format(response.status, response.url))
        except:
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
                            'pipelines_bookfinder_sell.ExtractPipeline': 300,
                },
                'USER_AGENT': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_11_6) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/65.0.3325.181 Safari/537.36',
                'DOWNLOAD_DELAY': request_delay,
                'DOWNLOAD_TIMEOUT': timeout,
                'CONCURRENT_REQUESTS':no_of_threads,
                'CONCURRENT_REQUESTS_PER_DOMAIN': no_of_threads,
                'RETRY_TIMES':no_of_retries,
                'ROTATING_PROXY_PAGE_RETRY_TIMES':no_of_retries,
                'RANDOM_UA_PER_PROXY': True,
                # 'RETRY_HTTP_CODES':[403, 429],
                'ROTATING_PROXY_LIST_PATH':rotating_proxy_list,
                'ROTATING_PROXY_BAN_POLICY': 'pipelines_bookfinder_sell.BanPolicy',
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
    process.crawl(BookScouterSellSpider)

    process.start()

if __name__ == '__main__':
    no_of_threads = 80
    request_delay = 0.01
    timeout = 45
    no_of_retries = 10
    run_spider(no_of_threads, request_delay, timeout, no_of_retries)

