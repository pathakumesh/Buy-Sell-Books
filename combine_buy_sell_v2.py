# -*- coding: utf-8 -*-
import glob
import os
import json
import datetime
import pytz
import re
import csv


BUY_PATH = "/home/nigam_parikh_scp/FM/results/Bookscouter/Trade-In/Buy_Pricing/"
SELL_PATH = "/home/nigam_parikh_scp/FM/results/Bookscouter/Trade-In/Sell_Pricing/"
COMBINE_PATH = "/home/nigam_parikh_scp/FM/results/Combine"

# BUY_PATH = '/Users/PathakUmesh/Programming_stuffs/NIGAM/BUYING/*'
# SELL_PATH = '/Users/PathakUmesh/Programming_stuffs/NIGAM/SELLING/*'
# COMBINE_PATH = '/Users/PathakUmesh/Programming_stuffs/NIGAM/COMBINE'




def get_file(path):
    list_of_files = glob.glob(path)
    file_path = max(list_of_files, key=os.path.getctime)
    return file_path

def combine_buy_and_sell():
    buy_file_name = get_file(BUY_PATH)

    print('---------------------------------------------------------------------------------------------------------------------')
    print('Taking buying details from file {}'.format(buy_file_name))
    print('---------------------------------------------------------------------------------------------------------------------')
    
    sell_file_name = get_file(SELL_PATH)

    print('---------------------------------------------------------------------------------------------------------------------')
    print('Taking selling details from file {}'.format(sell_file_name))
    print('---------------------------------------------------------------------------------------------------------------------')
    

    if not os.path.exists(COMBINE_PATH):
        os.makedirs(COMBINE_PATH)
    
    utc_time = datetime.datetime.utcnow()
    tz_info = pytz.timezone('Asia/Kolkata')
    utc = pytz.utc
    time_local = utc.localize(utc_time).astimezone(tz_info)
    
    combine_file_name = '{}/Combine_Trade-In_{}.csv'.format(COMBINE_PATH,time_local.strftime('%d%b%Y_%Hhr%Mmin'))
    combine_filtered_file_name = '{}/Combine_Trade-In_Filtered_{}.csv'.format(COMBINE_PATH,time_local.strftime('%d%b%Y_%Hhr%Mmin'))
    
    combine_csvfile =  open(combine_file_name, 'w')
    combine_filtered_csvfile =  open(combine_filtered_file_name, 'w')
    
    fieldnames = ['Title', 'ISBN-10', 'ISBN-13', 'URL_Buy', 'Condition', 'Vendor',
                  'Purchase Cost', 'Shipping Cost', 'Processing Cost', 'Net Buying Cost',
                  'Selling Price','ROI Amt', 'ROI %']
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
            if i == 0:
                continue
            with open(sell_file_name, 'r') as csvfile_sell:
                csvreader_sell = csv.reader(csvfile_sell)
                for j, sell_row in enumerate(csvreader_sell):
                    if j == 0:
                        continue
                    if str(buy_row[1]) == sell_row[1]:
                        writer1.writerow({'Title': str(buy_row[0]),
                            'ISBN-10': str(buy_row[1]),
                            'ISBN-13': str(buy_row[2]),
                            'Condition': str(buy_row[3]),
                            'Vendor': str(buy_row[4]),
                            'Purchase Cost': str(buy_row[5]),
                            'Shipping Cost': str(buy_row[6]),
                            'Processing Cost': str(buy_row[7]),
                            'Net Buying Cost': str(buy_row[8]),
                            'URL_Buy': str(buy_row[9]),
                            'Selling Price': sell_row[4],
                            'ROI Amt': '%.2f' % (float(sell_row[4]) - float(buy_row[8])),
                            'ROI %': '%.2f' % ((float(sell_row[4]) - float(buy_row[8]))* 100/float(buy_row[8]))
                        })
                        if (float(sell_row[4]) - float(buy_row[8])) >= 10:
                            writer2.writerow({'Title': str(buy_row[0]),
                                'ISBN-10': str(buy_row[1]),
                                'ISBN-13': str(buy_row[2]),
                                'Condition': str(buy_row[3]),
                                'Vendor': str(buy_row[4]),
                                'Purchase Cost': str(buy_row[5]),
                                'Shipping Cost': str(buy_row[6]),
                                'Processing Cost': str(buy_row[7]),
                                'Net Buying Cost': str(buy_row[8]),
                                'URL_Buy': str(buy_row[9]),
                                'Selling Price': sell_row[4],
                                'ROI Amt': '%.2f' % (float(sell_row[4]) - float(buy_row[8])),
                                'ROI %': '%.2f' % ((float(sell_row[4]) - float(buy_row[8]))* 100/float(buy_row[8]))
                            })


if __name__ == '__main__':
    combine_buy_and_sell()

