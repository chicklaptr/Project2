import json
import os
import psycopg2
import sys
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from config.config  import load_config
from  dotenv import load_dotenv
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
JSON_DIR = os.path.join(BASE_DIR, "data","processed")

def insert_data(cur, tupl):
    sql = """
    INSERT INTO tiki(id,name,url_key,price,description,images)
    VALUES(%s,%s,%s,%s,%s,%s)
    ON CONFLICT (id) DO NOTHING;
    """
    cur.execute(sql, tupl)
load_dotenv()
password=os.getenv("DB_PASSWORD")
if not password:
    raise ValueError("Missing DB_PASSWORD in env")
if __name__ == '__main__':
    config = load_config()
    config["password"]=password
    with psycopg2.connect(**config) as conn:
        with conn.cursor() as cur:
            for i in range(1, 3):
                try:
                    FILE_PATH=os.path.join(JSON_DIR,f"product{i}.json")
                    print(FILE_PATH)
                    with open(FILE_PATH,"r",encoding="utf-8") as f:
                        data = json.load(f)
                    print("File:", i, "records:", len(data))
                    for dis in data:
                        tu = (
                                dis.get("id"),
                                dis.get("name"),
                                dis.get("url_key"),
                                dis.get("price"),
                                dis.get("description"),
                                json.dumps(dis.get("images"))
                        )
                        insert_data(cur, tu)
                    conn.commit()
                except FileNotFoundError:
                    print(f"File{i} khong ton tai")
                except PermissionError:
                    print(f"File{i} khon co quyen doc")
                except IsADirectoryError:
                    print(f"Directory {i}, khong phai file")
                except json.JSONDecodeError:
                    print(f"FIle{i} loi cu phap Json")
                except OSError as e:
                    print(f"File{i} loi",e)
                except Exception as e:
                    conn.rollback()
                    print(f"File{i} insert loi:",e)


