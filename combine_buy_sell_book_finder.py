# -*- coding: utf-8 -*-
import glob
import os
import json
import datetime
import pytz
import re
import csv

BUY_PATH = "/home/FM/results/Trade_In/Amazon_Buy/*"
SELL_PATH = "/home/FM/results/Trade_In/Bookfinder_Sell/*"
COMBINE_PATH = "/home/FM/results/Trade_In/Combine_Amazon Buy_Bookfinder Sell"

# BUY_PATH = '/Users/PathakUmesh/Programming_stuffs/NIGAM/BUYING/*'
# SELL_PATH = '/Users/PathakUmesh/Programming_stuffs/NIGAM/SELLING/*'
# COMBINE_PATH = '/Users/PathakUmesh/Programming_stuffs/NIGAM/COMBINE'




def get_file(path):
    list_of_files = glob.glob(path)
    file_path = max(list_of_files, key=os.path.getctime)
    return file_path

def combine_buy_and_sell():
    buy_file_name = get_file(BUY_PATH)
    # buy_file_name = "/Users/PathakUmesh/Programming_stuffs/NIGAM/BUYING/Amazon_Trade-In_Buy_Pricing_19Aug2018_14hr08min.csv"

    print('---------------------------------------------------------------------------------------------------------------------')
    print('Taking buying details from file {}'.format(buy_file_name))
    print('---------------------------------------------------------------------------------------------------------------------')
    
    sell_file_name = get_file(SELL_PATH)
    # sell_file_name = "/Users/PathakUmesh/Programming_stuffs/NIGAM/SELLING/Bookfinder_Trade-In_Sell_Pricing_19Aug2018_14hr24min.csv"

    print('---------------------------------------------------------------------------------------------------------------------')
    print('Taking selling details from file {}'.format(sell_file_name))
    print('---------------------------------------------------------------------------------------------------------------------')
    

    if not os.path.exists(COMBINE_PATH):
        os.makedirs(COMBINE_PATH)
    
    utc_time = datetime.datetime.utcnow()
    tz_info = pytz.timezone('Asia/Kolkata')
    utc = pytz.utc
    time_local = utc.localize(utc_time).astimezone(tz_info)
    
    combine_file_name = '{}/Combine_Amazon Buy_Bookfinder Sell_{}.csv'.format(COMBINE_PATH,time_local.strftime('%d%b%Y_%Hhr%Mmin'))
    combine_filtered_file_name = '{}/Combine_Amazon Buy_Bookfinder Sell_Filtered_{}.csv'.format(COMBINE_PATH,time_local.strftime('%d%b%Y_%Hhr%Mmin'))
    
    combine_csvfile =  open(combine_file_name, 'w')
    combine_filtered_csvfile =  open(combine_filtered_file_name, 'w')
    
    fieldnames = ['Title', 'ISBN-10', 'ISBN-13','Purchase Cost', 'Shipping Cost',
                  'Processing Cost', 'Net Buying Cost','Vendor', 'Selling Price',
                  'URL_Sell', 'ROI Amt', 'ROI %']
    writer1 = csv.DictWriter(combine_csvfile, fieldnames=fieldnames)
    writer1.writeheader()

    writer2 = csv.DictWriter(combine_filtered_csvfile, fieldnames=fieldnames)
    writer2.writeheader()
    print('---------------------------------------------------------------------------------------------------------------------')
    print('Writing output to file {}'.format(combine_file_name))
    print('---------------------------------------------------------------------------------------------------------------------')

    print('---------------------------------------------------------------------------------------------------------------------')
    print('Writing filtered output to file {}'.format(combine_filtered_file_name))
    print('---------------------------------------------------------------------------------------------------------------------')




    with open(buy_file_name, 'r') as csvfile_buy:
        csvreader_buy = csv.reader(csvfile_buy)
        for i, buy_row in enumerate(csvreader_buy):
            if i == 0 or buy_row[6] == 'N/A':
                continue
            with open(sell_file_name, 'r') as csvfile_sell:
                csvreader_sell = csv.reader(csvfile_sell)
                for j, sell_row in enumerate(csvreader_sell):
                    if j == 0 or sell_row[4] == 'N/A':
                        continue
                    if str(buy_row[1]) == sell_row[1]:
                        writer1.writerow({'Title': str(buy_row[0]),
                            'ISBN-10': str(buy_row[1]),
                            'ISBN-13': str(buy_row[2]),
                            'Purchase Cost': str(buy_row[3]),
                            'Shipping Cost': str(buy_row[4]),
                            'Processing Cost': str(buy_row[5]),
                            'Net Buying Cost': str(buy_row[6]),
                            'Vendor': sell_row[3],
                            'Selling Price': sell_row[4],
                            'URL_Sell': sell_row[5],
                            'ROI Amt': '%.2f' % (float(sell_row[4]) - float(buy_row[6])),
                            'ROI %': '%.2f' % ((float(sell_row[4]) - float(buy_row[6]))* 100/float(buy_row[6]))
                        })
                        if (float(sell_row[4]) - float(buy_row[6])) >= 10:
                            writer2.writerow({'Title': str(buy_row[0]),
                                'ISBN-10': str(buy_row[1]),
                                'ISBN-13': str(buy_row[2]),
                                'Purchase Cost': str(buy_row[3]),
                                'Shipping Cost': str(buy_row[4]),
                                'Processing Cost': str(buy_row[5]),
                                'Net Buying Cost': str(buy_row[6]),
                                'Vendor': sell_row[3],
                                'Selling Price': sell_row[4],
                                'URL_Sell': sell_row[5],
                                'ROI Amt': '%.2f' % (float(sell_row[4]) - float(buy_row[6])),
                                'ROI %': '%.2f' % ((float(sell_row[4]) - float(buy_row[6]))* 100/float(buy_row[6]))
                            })


if __name__ == '__main__':
    combine_buy_and_sell()