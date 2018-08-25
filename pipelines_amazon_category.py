import csv
import os
import glob
import pytz
import datetime
import logging as log
from scrapy import signals
from scrapy.exporters import CsvItemExporter
from rotating_proxies.policy import BanDetectionPolicy


OUTPUT_PATH = "/home/FM/results/Category_Indexing"
# OUTPUT_PATH = '/Users/PathakUmesh/Programming_stuffs/NIGAM/CATEGORY'

class ExtractPipeline(object):
    def __init__(self):
        self.files = {}
        self.extracted_categories = list()
        if not os.path.exists(OUTPUT_PATH):
            os.makedirs(OUTPUT_PATH)
        utc_time = datetime.datetime.utcnow()
        tz_info = pytz.timezone('Asia/Kolkata')
        utc = pytz.utc
        time_local = utc.localize(utc_time).astimezone(tz_info)
        self.start_formatted_time = time_local.strftime('%d%b%Y_%Hhr%Mmin')
        self.file_name = '{}/Amazon_Categories_{}.csv'.format(OUTPUT_PATH, self.start_formatted_time)
        self.get_category_map()
        self.export_fields = ['category', 'url']


    def get_category_map(self):
        self.category_map = {}
        category_path = '{}/*'.format(OUTPUT_PATH)
        list_of_files = glob.glob(category_path)
        if not list_of_files:
            return
        file_path = max(list_of_files, key=os.path.getctime)
        log.info('[+++++] Current latest category file is: {}'.format(file_path))
        with open(file_path, 'r') as csvfile:
            csvreader = csv.reader(csvfile)
            for i, buy_row in enumerate(csvreader):
                if i == 0:
                    continue
                if not self.category_map.get(str(buy_row[0])):
                    self.category_map.update({str(buy_row[0]):str(buy_row[1])})


    @classmethod
    def from_crawler(cls, crawler):
        pipeline = cls()
        crawler.signals.connect(pipeline.spider_opened, signals.spider_opened)
        crawler.signals.connect(pipeline.spider_closed, signals.spider_closed)
        return pipeline

    def spider_opened(self, spider):
        self.start_time = datetime.datetime.utcnow()
        tz_info = pytz.timezone('Asia/Kolkata')
        utc = pytz.utc
        time_local = utc.localize(self.start_time).astimezone(tz_info)
        log.info('[+++++] Starting Time (IST;Asia-Mumbai): {}'.format(time_local.strftime('%b %d, %Y @%Hhr %Mmin %Ssec')))
        output_file = open(self.file_name, 'w+b')
        self.files[spider] = output_file
        self.exporter = CsvItemExporter(output_file,fields_to_export = self.export_fields)
        self.exporter.start_exporting()

    def spider_closed(self, spider):
        log.info('[+++] Finished fetching; now adding missing urls from previous file')
        for category, url in self.category_map.items():
            item = {'category': category, 'url': url}
            self.exporter.export_item(item)
            log.info('[+++] {}'.format(category))
        log.info('[+++] Added {} urls from previous file'.format(len(self.category_map)))
        self.exporter.finish_exporting()
        output_file = self.files.pop(spider)
        output_file.close()
        
        utc_time = datetime.datetime.utcnow()
        tz_info = pytz.timezone('Asia/Kolkata')
        utc = pytz.utc
        started_time = utc.localize(self.start_time).astimezone(tz_info)
        finished_time = utc.localize(utc_time).astimezone(tz_info)

        self.finish_formatted_time = finished_time.strftime('%d%b%Y_%Hhr%Mmin')

        new_name = self.file_name.replace(self.start_formatted_time, self.finish_formatted_time)
        os.rename(self.file_name, new_name)

        log.info('[+++++] Output file path: /home/FM/results/Trade_In/Amazon_Buy')
        log.info('[+++++] Output file name: {}'.format(new_name.rsplit('/',1)[-1]))


        time_taken = self.strfdelta(utc_time-self.start_time, "{hours} hours {minutes} minutes {seconds} seconds")
        log.info('[+++++] Starting Time (IST;Asia-Mumbai): {}'.format(started_time.strftime('%b %d, %Y @%Hhr %Mmin %Ssec')))
        log.info('[+++++] Finished Time (IST;Asia-Mumbai): {}'.format(finished_time.strftime('%b %d, %Y @%Hhr %Mmin %Ssec')))
        log.info('[+++++] Total Time Taken: {}'.format(time_taken))

    def process_item(self, item, spider):
        category = item['category']
        if category not in self.extracted_categories:
            self.extracted_categories.append(category)
            self.exporter.export_item(item)

        if self.category_map.get(category):
            self.category_map.pop(category)    
        return item

    def strfdelta(self, tdelta, fmt):
        d = dict()
        d["hours"], rem = divmod(tdelta.seconds, 3600)
        d["minutes"], d["seconds"] = divmod(rem, 60)
        return fmt.format(**d)

class BanPolicy(BanDetectionPolicy):
    def response_is_ban(self, request, response):
        # use default rules, but also consider HTTP 200 responses
        # a ban if there is 'captcha' word in response body.
        # ban = super(BanPolicy, self).response_is_ban(request, response)
        # ban = ban or response.status == 429
        # return ban

        return response.status == 429

    def exception_is_ban(self, request, exception):
        # override method completely: don't take exceptions in account
        return None