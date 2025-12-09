import pandas as pd
import requests
import json
from bs4 import BeautifulSoup
import re
import time

# Load danh sách ID
df = pd.read_csv("/home/tuananh/Project2/products-0-200000.csv")
idA = df["id"].tolist()

# Hàm làm sạch mô tả
def clean_description(html_text):
    if not html_text:
        return ""

    soup = BeautifulSoup(html_text, "html.parser")

    for li in soup.find_all("li"):
        li.insert_before("\n- ")
        li.unwrap()

    for tag in soup.find_all(["p", "br", "ul", "ol"]):
        tag.insert_before("\n")
        tag.unwrap()

    text = soup.get_text()
    text = re.sub(r"\n\s*\n", "\n", text)
    return text.strip()

# Hàm fetch 1 sản phẩm (requests thuần)
def fetch_product(idp):
    url = f"https://api.tiki.vn/product-detail/api/v1/products/{idp}"
    try:
        response = requests.get(url, timeout=10)
        if response.status_code == 200:
            data = response.json()
            return {
                "id": data.get("id"),
                "name": data.get("name"),
                "url_key": data.get("url_key"),
                "price": data.get("price"),
                "description": clean_description(data.get("description")),
                "images": [im.get("url") for im in data.get("images", [])]
            }
    except Exception as e:
        print(f"ID {idp} lỗi: {e}")

    return None

# Chạy tuần tự – không thread, không sleep
start = 0
end = 1000

for i in range(1, 201):
    batch_id = idA[start:end]
    product = []

    t0 = time.time()

    for idp in batch_id:
        item = fetch_product(idp)
        if item:
            product.append(item)

    dt = time.time() - t0
    print(f"File {i} – lấy xong 1000 sản phẩm trong {dt:.2f} giây")

    with open(f"product_te{i}.json", "w", encoding="utf-8") as f:
        json.dump(product, f, ensure_ascii=False, indent=2)

    start = end
    end += 1000

