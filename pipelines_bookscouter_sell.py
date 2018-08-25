import os
import pytz
import datetime
import logging as log
from scrapy import signals
from scrapy.exporters import CsvItemExporter
from rotating_proxies.policy import BanDetectionPolicy


# OUTPUT_PATH = "/home/nigam_parikh_scp/FM/results/Bookscouter/Trade-In/Sell_Pricing"
OUTPUT_PATH = '/Users/PathakUmesh/Programming_stuffs/NIGAM/SELLING'

class ExtractPipeline(object):
    def __init__(self):
        self.files = {}
        
        if not os.path.exists(OUTPUT_PATH):
            os.makedirs(OUTPUT_PATH)
        utc_time = datetime.datetime.utcnow()
        tz_info = pytz.timezone('Asia/Kolkata')
        utc = pytz.utc
        time_local = utc.localize(utc_time).astimezone(tz_info)
        self.file_name = '{}/Bookscouter_Trade-In_{}.csv'.format(OUTPUT_PATH,time_local.strftime('%d%b%Y_%Hhr%Mmin'))

        self.export_fields = ['title', 'isbn10','isbn13', 'vendor', 'selling_price']

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
        self.exporter.finish_exporting()
        output_file = self.files.pop(spider)
        output_file.close()

        log.info('[+++++] Output file path: /home/nigam_parikh_scp/FM/results/Bookscouter/Trade-In/Sell_Pricing')
        log.info('[+++++] Output file name: {}'.format(self.file_name.rsplit('/',1)[-1]))
        utc_time = datetime.datetime.utcnow()
        tz_info = pytz.timezone('Asia/Kolkata')
        utc = pytz.utc
        started_time = utc.localize(self.start_time).astimezone(tz_info)
        finished_time = utc.localize(utc_time).astimezone(tz_info)


        time_taken = self.strfdelta(utc_time-self.start_time, "{hours} hours {minutes} minutes {seconds} seconds")
        log.info('[+++++] Starting Time (IST;Asia-Mumbai): {}'.format(started_time.strftime('%b %d, %Y @%Hhr %Mmin %Ssec')))
        log.info('[+++++] Finished Time (IST;Asia-Mumbai): {}'.format(finished_time.strftime('%b %d, %Y @%Hhr %Mmin %Ssec')))
        log.info('[+++++] Total Time Taken: {}'.format(time_taken))

    def process_item(self, item, spider):        
        self.exporter.export_item(item)
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

