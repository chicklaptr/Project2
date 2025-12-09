import pandas as pd
import requests
import json
from bs4 import BeautifulSoup
import re
import time
import random
from concurrent.futures import ThreadPoolExecutor
import time
#lay list  id
df=pd.read_csv("/home/tuananh/Project2/products-0-200000.csv")
idA=df["id"].tolist()
#kho tao 
product=[]
start=0
end=1000
MAX_THREADS=30
# ham chuan hoa description html -> text dep
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

headers = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 \
                   (KHTML, like Gecko) Chrome/142.0.0.0 Safari/537.36"
}
# dung session thay cho requests de ket noi luon
session=requests.Session()
session.headers.update(headers)

#ham lay tung sp theo id
def fetch_product(idp):
        url = f"https://api.tiki.vn/product-detail/api/v1/products/{idp}"
        try:
            response = session.get(url, timeout=10)
            #print(f"ID {idp} status {response.status_code}")  # log status
            time.sleep(random.uniform(0.05,0.1))
            if response.status_code == 200:
                data = response.json()
                return{
                    "id": data.get("id"),
                    "name": data.get("name"),
                    "url_key": data.get("url_key"),
                    "price": data.get("price"),
                    "description": clean_description(data.get("description")),
                    "images": [im.get("url") for im in data.get("images", [])]
                }
            else:
                print(f"loi do khac ma 200, {response.status_code}")
        except Exception as e:
            print(f"ID {idp} lỗi: {e}")
        return None
start_time=time.time()
for i in range(1,201):
    batch_start = time.time()
    batch_id=idA[start:end]
#co 20 luong cung chay ham mot cach song song tang toc do
    with ThreadPoolExecutor(max_workers=MAX_THREADS) as executor:
        results=list( executor.map(fetch_product,batch_id))
        product=[p for p in results if p]
    with open(f"/home/tuananh/test/product{i}.json", "w", encoding="utf-8") as f:
        json.dump(product,f,ensure_ascii=False,indent=2)

    batch_end = time.time()
    duration = batch_end - batch_start
    print(f"File json thứ {i} — Thời gian: {duration:.2f} giây")
    time.sleep(0.2)
    product=[]
    start=end
    end=end+1000

end_time=time.time()
total_time=end_time-start_time
print(total_time)

