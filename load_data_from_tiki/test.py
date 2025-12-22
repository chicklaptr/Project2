import pandas as pd
import requests 
import json
from bs4 import BeautifulSoup
import re

product=[]
def clean_description(html_text):
    if not html_text:
        return ""

    soup = BeautifulSoup(html_text, "html.parser")

    for li in soup.find_all("li"):
        li.insert_before("\n- ")
        li.unwrap()

    for tag in soup.find_all(["p","br","ul","ol"]):
        tag.insert_before("\n")
        tag.unwrap()

    text = soup.get_text()
    text = re.sub(r"\n\s*\n", "\n", text)
    text = text.strip()

    return text

idd=[9803081,7697454,7697437,7973548,7697522,7543425,7470553,9370172]
def  test(idd):
        headers = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 \
                   (KHTML, like Gecko) Chrome/142.0.0.0 Safari/537.36"
}       
        url = f"https://api.tiki.vn/product-detail/api/v1/products/{idd}"
        response = requests.get(url, headers=headers)
        print(response.status_code)
        if response.status_code==200 :
            data=response.json()
            print("bat dau")
            print(data.get("id"))
            print(data.get("name"))
            print(data.get("url_key"))
            print(data.get("price"))
            print(clean_description(data.get("description")))
            if(data.get("images")==None):
                print("Do phan data cua anh la null")
            else:
                for im in data.get("images"):
                    if(im.get("url")==None):
                        print("do url co gia tri null")
                    else:
                        print(im.get("url"))
for i in idd:
    test(i)

        
    
