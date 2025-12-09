import pandas as pd
import requests
import json
from bs4 import BeautifulSoup
import re
import time
import random
from concurrent.futures import ThreadPoolExecutor

# Load list ID
df = pd.read_csv("/home/tuananh/Project2/products-0-200000.csv")
idA = df["id"].tolist()

# Kho chứa sản phẩm
product = []
failed_ids = []  # danh sách lưu ID lỗi
start = 0
end = 1000
MAX_THREADS = 30

# Hàm chuẩn hóa description HTML -> text
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

# Header và session
headers = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 \
                   (KHTML, like Gecko) Chrome/142.0.0.0 Safari/537.36"
}
session = requests.Session()
session.headers.update(headers)

# Hàm lấy từng sản phẩm
def fetch_product(idp):
    url = f"https://api.tiki.vn/product-detail/api/v1/products/{idp}"
    try:
        response = session.get(url, timeout=10)
        time.sleep(random.uniform(0.05,0.1))
        if response.status_code == 200:
            data = response.json()
            return {
                "id": data.get("id"),
                "name": data.get("name"),
                "url_key": data.get("url_key"),
                "price": data.get("price"),
                "description": clean_description(data.get("description")),
                "images": [im.get("url") for im in data.get("images", []) 
                           if isinstance(im, dict) and im.get("url")]
            }
        else:
            print(f"ID {idp} lỗi status code: {response.status_code}")
            failed_ids.append(idp)
    except Exception as e:
        print(f"ID {idp} lỗi: {e}")
        failed_ids.append(idp)
    return None

# Chạy lấy dữ liệu theo batch
start_time = time.time()
for i in range(1, 201):
    batch_start = time.time()
    batch_id = idA[start:end]
    
    with ThreadPoolExecutor(max_workers=MAX_THREADS) as executor:
        results = list(executor.map(fetch_product, batch_id))
        product = [p for p in results if p]
    
    # Lưu file JSON
    with open(f"/home/tuananh/Project2/file_json/product{i}.json", "w", encoding="utf-8") as f:
        json.dump(product, f, ensure_ascii=False, indent=2)
    
    batch_end = time.time()
    print(f"File json thứ {i} — Thời gian: {batch_end - batch_start:.2f} giây")
    
    start = end
    end += 1000
    product = []

# Lưu danh sách ID lỗi
with open("/home/tuananh/Project2/file_json/failed_ids.json", "w", encoding="utf-8") as f:
    json.dump(failed_ids, f, ensure_ascii=False, indent=2)

end_time = time.time()
print(f"Tổng thời gian: {end_time - start_time:.2f} giây")

