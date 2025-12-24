import pandas as pd
import requests
import json
from bs4 import BeautifulSoup
import re
import argparse
import time
import random
from concurrent.futures import ThreadPoolExecutor
import os   # thêm import để dùng checkpoint
from dotenv import load_dotenv

load_dotenv()

# ==== THÊM WEBHOOK DISCORD =====
WEBHOOK_URL =os.getenv("DISCORD_WEBHOOK_URL")
if not WEBHOOK_URL:
    raise ValueError("Missing DISCORD_WEBHOOK_URL in .env")

def notify(msg):
    try:
        requests.post(WEBHOOK_URL, json={"content": msg})
    except:
        pass
# =================================


# Load list ID
parser=argparse.ArgumentParser()
parser.add_argument(
        "--input",
        required=True,
        help="Path to ID CSV file"
)
args=parser.parse_args()
df = pd.read_csv(args.input)
idA = df["id"].tolist()
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
OUTPUT_DIR = os.path.join(BASE_DIR, "data", "processed")
os.makedirs(OUTPUT_DIR, exist_ok=True)
CHECKPOINT_DIR = os.path.join(BASE_DIR, "data", "checkpoints")
os.makedirs(CHECKPOINT_DIR, exist_ok=True)
CHECKPOINT_FILE = os.path.join(CHECKPOINT_DIR, "checkpoint.txt")
ID_ERROR_FILE=os.path.join(OUTPUT_DIR,"failed_ids.json")

# Kho chứa sản phẩm
product = []
failed_ids = []
start = 0
end = 1000
MAX_THREADS = 30

# ==== CHECKPOINT ====

if os.path.exists(CHECKPOINT_FILE):
    with open(CHECKPOINT_FILE) as f:
        last = int(f.read().strip())
else:
    last=1
BATCH_SIZE=1000
#Tính start – end dựa vào batch cũ
start = (last - 1) * BATCH_SIZE
end = start + 1000
begin_batch = last
# ==========================================


# Hàm chuẩn hóa description
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
# Hàm fetch sản phẩm
def fetch_product(idp):
    url = f"https://api.tiki.vn/product-detail/api/v1/products/{idp}"
    try:
        response = session.get(url, timeout=10)
        time.sleep(random.uniform(0.05,0.1))
        if response.status_code == 200:
            data = response.json()
            raw_images = data.get("images")

            if not isinstance(raw_images, list):
                raw_images = []

            images = [
                {
                    "base_url": im.get("base_url"),
                    "large_url": im.get("large_url"),
                    "medium_url": im.get("medium_url"),
                    "small_url": im.get("small_url"),
                    "thumbnail_url": im.get("thumbnail_url"),
                }
                for im in raw_images
                if isinstance(im, dict)
            ]

            return {
                "id": data.get("id"),
                "name": data.get("name"),
                "url_key": data.get("url_key"),
                "price": data.get("price"),
                "description": clean_description(data.get("description")),
                "images": images
            }
        else:
            print(f"ID {idp} lỗi status code: {response.status_code}")
            failed_ids.append(idp)

    except Exception as e:
        print(f"ID {idp} lỗi: {e}")
        failed_ids.append(idp)

    return None

start_time = time.time()

for i in range(begin_batch, 3):  
    batch_start = time.time()
    batch_id = idA[start:end]

    print(f"Bắt đầu batch {i}")
    notify(f"Bắt đầu batch {i}")   # thêm gửi Discord

    with ThreadPoolExecutor(max_workers=MAX_THREADS) as executor:
        results = list(executor.map(fetch_product, batch_id))
        product = [p for p in results if p]

    # Lưu file JSON
    file_path = os.path.join(OUTPUT_DIR, f"product{i}.json")
    with open(file_path, "w", encoding="utf-8") as f:
        json.dump(product, f, ensure_ascii=False, indent=2)

    # Ghi checkpoint 
    with open(CHECKPOINT_FILE, "w") as f:
        f.write(str(i + 1))

    batch_end = time.time()
    print(f"File json thứ {i} — Thời gian: {batch_end - batch_start:.2f} giây")

    notify(f"Xong batch {i} — {batch_end - batch_start:.2f} giây")

    start = end
    end += 1000
    product = []


# Lưu danh sách ID lỗi
with open(ID_ERROR_FILE, "w", encoding="utf-8") as f:
    json.dump(failed_ids, f, ensure_ascii=False, indent=2)

end_time = time.time()
notify(f" Hoàn thành 200 batch — Tổng {end_time - start_time:.2f} giây")
print(f"Tổng thời gian: {end_time - start_time:.2f} giây")

