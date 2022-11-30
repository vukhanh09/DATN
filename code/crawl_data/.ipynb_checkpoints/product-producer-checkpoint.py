from tiki_crawler import Tiki
import time
import requests
import json
import re
from tqdm import tqdm
import pandas as pd
import warnings
warnings.filterwarnings("ignore")
import argparse

from kafka import KafkaProducer
from json import dumps
from kafka import KafkaConsumer
from json import loads


parser = argparse.ArgumentParser()
parser.add_argument('--start_page', type=int, default=1)
parser.add_argument('--end_page', type=int, default=51)
parser.add_argument('--start_category', type=int, default=0)
parser.add_argument('--end_category', type=int, default=24)
parser.add_argument('--topic', type=str, default='Product')
args = parser.parse_args()



if __name__ == '__main__':
    # init tiki crawler
    tiki = Tiki()
    
    # init kafka producer
    producer = KafkaProducer(bootstrap_servers=['kafka-1:9092','kafka-2:9092','kafka-3:9092'],
                         value_serializer=lambda x: 
                         dumps(x).encode('utf-8'))

    for i in range(args.start_category, args.end_category, 1):
        category, category_id = tiki.cat_info[i]
        print(f'crawl category: {category}')
        dict_check = {}
        for page_index in tqdm(range(args.start_page, args.end_page)):
            time.sleep(0.5)
            category_page_url = tiki.api_get_id_product.format(category_id, page_index, category)

            list_product_ids = tiki.get_list_id_product(category_page_url)
            
            print("Number of product id:", len(list_product_ids))

            if len(list_product_ids) > 0:
                for pid,spid in tqdm(list_product_ids):
                    key_valid = f'{pid}_{spid}'
                    
                    if key_valid not in dict_check:
                        dict_check[key_valid] = 1
                        
                    time.sleep(0.1)
                    if args.topic == 'Product':
                        product_url = tiki.api_product.format(pid,spid)
                        try:
                            item = tiki.get_information_product(product_url)
                            other_seller_list = item['other_sellers']
                            
                            
                            # get other seller
                            if len(other_seller_list) > 0:
                                for other_seller in other_seller_list:
                                    spid_new = int(other_seller['product_id'])
                                    
                                    key_valid_new = f'{pid}_{spid_new}'
                                    
                                    if key_valid_new not in dict_check:
                                        dict_check[key_valid_new] = 1
                                        list_product_ids.append([pid,spid_new])

                            if item != 0:
                                producer.send(args.topic, value=item)
                        except Exception as e:
                            print(e)
                            continue
                    elif args.topic == 'Comment':
                        try:
                            review_url = tiki.api_review.format(1,spid,pid)
                            paging = tiki.get_max_paging_review(review_url)
                            if paging != 0:
                                for i in range(1,paging+1):
                                    time.sleep(0.1)
                                    try:
                                        comments = tiki.get_review(review_url)
                                        if comments != 0:
                                            for item in comments:
                                                producer.send(args.topic, value=item)
                                    except Exception as e:
                                        print(e)
                                        continue
                        except Exception as e:
                            print(e)
                            continue
                

                
                