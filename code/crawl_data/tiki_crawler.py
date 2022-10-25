import requests
import json
import re
from tqdm import tqdm
import pandas as pd
import warnings
warnings.filterwarnings("ignore")



class Tiki:
    def __init__(self):
        self.api_categories_info = 'https://api.tiki.vn/shopping/v2/widgets/home-category-tab-bar?trackity_id=37b45be6-0e16-e23b-5be5-5a95e6d3f508'
        
        self.api_get_id_product = 'https://tiki.vn/api/personalish/v1/blocks/listings?limit=40&include=advertisement&aggregations=2&trackity_id=98ad34b4-6944-2947-2f4a-ff17de7e2a42&category={}&page={}&urlKey={}'
        
        self.api_product = 'https://tiki.vn/api/v2/products/{}?platform=web&spid={}'
        
        self.headers={
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) "
                          "Chrome/100.0.4896.127 Safari/537.36 "
        }
        
        
        self.api_review = 'https://tiki.vn/api/v2/reviews?limit=20&include=comments,contribute_info,attribute_vote_summary&sort=score%7Cdesc,id%7Cdesc,stars%7Call&page={}&spid={}&product_id={}'
        
        self.get_category()
        
    
    def get_category(self):
        response = requests.get(self.api_categories_info, headers=self.headers)
        
        data = json.loads(response.text)['data']
        self.cat_info = []
        if response.status_code == 200:
            for item in data[1:]:
                arr = item['url'].split('/')
                cat_id = int(arr[-1].replace('c',''))
                self.cat_info.append([arr[-2],cat_id])
                
    def get_list_id_product(self,url):
        response = requests.get(url, headers=self.headers)
        list_ids = []
        if response.status_code == 200:
            data = json.loads(response.text)['data']
            for sample in data:
                list_ids.append([sample['id'],sample['seller_product_id']])

        return list_ids

    def get_information_product(self,url):
        response = requests.get(url, headers=self.headers)
        if response.status_code == 200:
            data = json.loads(response.text)
            return data
        return 0
        
    def get_max_paging_review(self,url):
        response = requests.get(url, headers=self.headers)
        if response.status_code == 200:
            data = json.loads(response.text)
            return data['paging']['last_page']
        return 0
    
    def get_review(self,url):
        response = requests.get(url, headers=self.headers)
        if response.status_code == 200:
            data = json.loads(response.text)
            return data['data']
        return 0
        
    
    