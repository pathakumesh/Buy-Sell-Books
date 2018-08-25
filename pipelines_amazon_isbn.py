# -*- coding: utf-8 -*-
import os
import csv
import pytz
import glob
import datetime
import logging as log
from scrapy import signals
from shutil import copyfile
from scrapy.exporters import CsvItemExporter
from rotating_proxies.policy import BanDetectionPolicy


RAW_OUTPUT_PATH       =   "/home/FM/results/Trade_In/Amazon_ISBN_Raw"
FILTERED_OUTPUT_PATH  =   "/home/FM/results/Trade_In/Amazon_ISBN"
MASTER_DB_PATH        =   "/home/FM/results/Master_DB/Amazon_ISBN"

RAW_OUTPUT_PATH         =   '/Users/PathakUmesh/Programming_stuffs/NIGAM/ISBN_RAW'
FILTERED_OUTPUT_PATH    =   '/Users/PathakUmesh/Programming_stuffs/NIGAM/ISBN'
MASTER_DB_PATH          =   '/Users/PathakUmesh/Programming_stuffs/NIGAM/ISBN_MASTER'

class ExtractPipeline(object):
    def __init__(self):
        self.files = {}
        self.extracted_isbns = list()
        self.master_isbns = list()
        if not os.path.exists(RAW_OUTPUT_PATH):
            os.makedirs(RAW_OUTPUT_PATH)
        if not os.path.exists(FILTERED_OUTPUT_PATH):
            os.makedirs(FILTERED_OUTPUT_PATH)
        if not os.path.exists(MASTER_DB_PATH):
            os.makedirs(MASTER_DB_PATH)

        utc_time = datetime.datetime.utcnow()
        tz_info = pytz.timezone('Asia/Kolkata')
        utc = pytz.utc
        time_local = utc.localize(utc_time).astimezone(tz_info)
        self.start_formatted_time = time_local.strftime('%d%b%Y_%Hhr%Mmin')
        self.file_name = {
            'raw':'{}/Amazon_Trade-In_ISBN_Raw_{}.csv'.format(RAW_OUTPUT_PATH, self.start_formatted_time),
            'filtered':'{}/Amazon_Trade-In_ISBN_{}.csv'.format(FILTERED_OUTPUT_PATH, self.start_formatted_time),
            'master': self.get_master_file()
        }

        self.export_fields = ['title', 'isbn10', 'isbn13']

    @classmethod
    def from_crawler(cls, crawler):
        pipeline = cls()
        crawler.signals.connect(pipeline.spider_opened, signals.spider_opened)
        crawler.signals.connect(pipeline.spider_closed, signals.spider_closed)
        return pipeline

    def get_master_file(self):
        new_file_path = '{}/Amazon_Master_DB_ISBN_{}.csv'.format(MASTER_DB_PATH, self.start_formatted_time)
        
        list_of_files = glob.glob('{}/*'.format(MASTER_DB_PATH))
        if list_of_files:
            existing_file_path = max(list_of_files, key=os.path.getctime)
            copyfile(existing_file_path, new_file_path)
            self.read_master_isbns(new_file_path)
        return new_file_path
        
    
    def read_master_isbns(self, file_path):
        with open(file_path, 'r', encoding = 'utf-8') as csvfile:
            csvreader = csv.reader(csvfile)
            for i, row in enumerate(csvreader):
                if i == 0:
                    continue
                self.master_isbns.append(str(row[2]))

    def spider_opened(self, spider):
        self.start_time = datetime.datetime.utcnow()
        tz_info = pytz.timezone('Asia/Kolkata')
        utc = pytz.utc
        time_local = utc.localize(self.start_time).astimezone(tz_info)
        log.info('[+++++] Starting Time (IST;Asia-Mumbai): {}'.format(time_local.strftime('%b %d, %Y @%Hhr %Mmin %Ssec')))
        
        self.exporters = {}        
        for category in ['raw', 'filtered' ,'master']:
            if category == 'master':
                output_file = open(self.file_name[category], 'a+b')
            else:         
                output_file = open(self.file_name[category], 'w+b')
            
            exporter = CsvItemExporter(output_file, fields_to_export = self.export_fields)
            exporter.start_exporting()
            self.exporters[category] = exporter

    def spider_closed(self, spider):
        for exporter in self.exporters.values(): 
            exporter.finish_exporting()
        

        utc_time = datetime.datetime.utcnow()
        tz_info = pytz.timezone('Asia/Kolkata')
        utc = pytz.utc
        started_time = utc.localize(self.start_time).astimezone(tz_info)
        finished_time = utc.localize(utc_time).astimezone(tz_info)

        self.finish_formatted_time = finished_time.strftime('%d%b%Y_%Hhr%Mmin')

        file_path = {'raw': RAW_OUTPUT_PATH,
                     'filtered':FILTERED_OUTPUT_PATH,
                     'master': MASTER_DB_PATH
                    }
        
        for category in ['raw', 'filtered', 'master']:
            new_name = self.file_name[category].replace(self.start_formatted_time, self.finish_formatted_time)
            os.rename(self.file_name[category], new_name)

            log.info('[+++++] Output {} file path: {}'.format(category, file_path[category]))
            log.info('[+++++] Output {} file name: {}'.format(category, new_name.rsplit('/',1)[-1]))


        time_taken = self.strfdelta(utc_time-self.start_time, "{hours} hours {minutes} minutes {seconds} seconds")
        log.info('[+++++] Starting Time (IST;Asia-Mumbai): {}'.format(started_time.strftime('%b %d, %Y @%Hhr %Mmin %Ssec')))
        log.info('[+++++] Finished Time (IST;Asia-Mumbai): {}'.format(finished_time.strftime('%b %d, %Y @%Hhr %Mmin %Ssec')))
        log.info('[+++++] Total Time Taken: {}'.format(time_taken))

    def process_item(self, item, spider):
        self.exporters['raw'].export_item(item)
        
        if item['isbn13'] not in self.extracted_isbns:
            self.exporters['filtered'].export_item(item)
            self.extracted_isbns.append(item['isbn13'])
        
        if item['isbn13'] not in self.master_isbns:
            self.exporters['master'].export_item(item)
            self.master_isbns.append(item['isbn13'])
            
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