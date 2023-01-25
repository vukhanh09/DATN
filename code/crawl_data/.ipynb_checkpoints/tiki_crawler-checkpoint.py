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
            "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/108.0.0.0 Safari/537.36",
            "cookie":'_trackity=8696091e-f76b-b306-1e23-da0a85adf480; _gcl_au=1.1.1750823129.1667055175; _fbp=fb.1.1667055175488.1329747993; __RC=4; __R=1; _hjSessionUser_522327=eyJpZCI6IjdmNjc4YjY5LTk4Y2QtNTY2YS1hYWYwLWQzYjNmOGU1NjBmYyIsImNyZWF0ZWQiOjE2NjcwNTUxNzUzNTUsImV4aXN0aW5nIjp0cnVlfQ==; __UF=-1; __tb=0; _bs=21f6550d-137f-27bd-f56e-a1258a13ccda; __IP=1952460427; rl_page_init_referrer=StackityEncrypt:U2FsdGVkX1/Hr1air3EcLRKdkELmiRmERDN+G3YLehaOuc+t+skW5zIQTFGua1uw; rl_page_init_referring_domain=StackityEncrypt:U2FsdGVkX19wls0Ll3k8dFKk/AAaYXINvLaLrc2fBOpzFC3RGntfynjAT0IYg2M1; rl_group_id=StackityEncrypt:U2FsdGVkX19v42npyQrwBbWfBfth+3DEBHEdhAfwL7g=; rl_group_trait=StackityEncrypt:U2FsdGVkX1+WhJ0Dn4KHuAS9r24O3Yv1JwKC+VkcFZw=; rl_anonymous_id=StackityEncrypt:U2FsdGVkX1/M3rjRnNA5470lMMe4sxNkinSwg22y2nMPLLo7o1znsgD4OotsImITQMnPtPwPiGNm5Lx4zqe1kw==; rl_user_id=StackityEncrypt:U2FsdGVkX19vURNNX7Ulv8Q0M2BbbEvR6WYER4cIKi0=; rl_trait=StackityEncrypt:U2FsdGVkX19SrxirxGwbZUkFz7OSz+EeaDH0iBeTTTFHhCM7Z+zHgfExVzaOoe4C/aXjfHNW+wWac7VPMRyqhA==; __uidac=231be8277114f6a140fadb2be756fb2e; amp_99d374=EyOGNBnT7ei63RJgdsAbfL.MTY2OTA3Mw==..1giv2crm4.1giv2ctt3.g4.js.140; cto_bundle=1j0JNV9Wa05sdzVNZHpOVlJ1T1A2ZWhJOSUyQlBCUmFibER3OXFRaXVNbEtSTjdadmtOcUZZWXpZaHc2ck5Rc0k2a0Fia3F2NzhWaUZiVVl1UzNjWXVJSHA5UnNIQnozaE41JTJCc2FCQWw5N0t2elolMkZHZjEzN1VjZXNya3NtVXZSbm5HZmZiY0slMkJnNkxSSThPekFDY2g2eHptQ1RFZyUzRCUzRA; OTZ=6869312_28_28__28_; TOKENS={"access_token":"MmxEUvj2DePqa6kdJT5husHzR0N4K3BA"}; _gid=GA1.2.1158998007.1674464207; tiki_client_id=317787655.1667055171; TIKI_RECOMMENDATION=7ecacc695e97c436a4e65235cd7a7652; delivery_zone=Vk4wMzQwMjQwMTM=; TKSESSID=10f3a1bf4ea5403e60627397321caf20; _gat=1; _ga_GSD4ETCY1D=GS1.1.1674487200.34.1.1674488094.48.0.0; _ga=GA1.1.317787655.1667055171'
        }
        
        
        self.api_review = 'https://tiki.vn/api/v2/reviews?limit=20&include=comments,contribute_info,attribute_vote_summary&sort=score%7Cdesc,id%7Cdesc,stars%7Call&page={}&spid={}&product_id={}'
        
        
        self.shop_api = 'https://tiki.vn/api/shopping/v2/widgets/seller?seller_id={}'
        
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
        response = requests.get(url, headers=self.headers,timeout=10)
        if (response.status_code != 204 and
            response.headers["content-type"].strip().startswith("application/json")):
            try:
                return json.loads(response.text)
            except ValueError:
                return 0
#         if response.status_code == 200:
#             data = json.loads(response.text)
#             return data
#         return 0
        
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

    
    def get_shop_information(self,url):
        response = requests.get(url, headers=self.headers)
        if response.status_code == 200:
            data = json.loads(response.text)
            return data['data']['seller']
        return 0
        
    
    