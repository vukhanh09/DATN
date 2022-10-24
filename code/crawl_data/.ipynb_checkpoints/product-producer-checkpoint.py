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
    producer = KafkaProducer(bootstrap_servers=['kafka-1:9092'],
                         value_serializer=lambda x: 
                         dumps(x).encode('utf-8'))

    for i in range(args.start_category, args.end_category, 1):
        category, category_id = tiki.cat_info[i]
        print(f'crawl category: {category}')
        for page_index in tqdm(range(args.start_page, args.end_page)):
            time.sleep(0.5)
            category_page_url = tiki.api_get_id_product.format(category_id, page_index, category)

            list_product_ids = tiki.get_list_id_product(category_page_url)

            if len(list_product_ids) > 0:
                for pid,spid in list_product_ids:
                    time.sleep(0.1)
                    product_url = tiki.api_product.format(pid,spid)
                    item = tiki.get_information_product(product_url)
                    data = {}
                    data['id'] = item['id']
                    data['master_id'] = item['master_id']
                    try:
                        if item != 0:
                            producer.send(args.topic, value=data)
                    except Exception as e:
                        print(e)
                        continue
                

                
                