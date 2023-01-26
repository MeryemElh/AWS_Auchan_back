from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
import psycopg2
import uvicorn
from typing import List, Tuple


app = FastAPI()


pg_host  = "postgres.ckxrkh97gyop.eu-west-3.rds.amazonaws.com"
pg_port = 5432
pg_user = "postgres"
pg_pwd = "postgres"
pg_db = "postgres"

connection = psycopg2.connect(
    host=pg_host,
    database=pg_db,
    user=pg_user,
    password=pg_pwd,
    options='-c statement_timeout=15000'
)

origins = [
    "*"
]

app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"]
)


@app.get("/getChart1Data")
async def getChart1Data():
    cursor = connection.cursor()
    cursor.execute("""  SELECT brand, COUNT(*)
                        FROM (  SELECT DISTINCT url, brand
                                FROM product_data) as brand_products
                        GROUP BY brand
                        ORDER BY COUNT(*) DESC
                        LIMIT 7 ;""")
    elem_list = cursor.fetchall()
    cursor.close()

    return [{'brand': elem[0], 'count': elem[1]} for elem in elem_list]

def get_parent(id: str, liste : List[Tuple[str]]):
    if not liste[id]["parent_id"] or not liste[liste[id]["parent_id"]]["parent_id"]:
        return id
    
    return get_parent(liste[id]["parent_id"], liste)
    

@app.get("/getChart2Data")
async def getChart2Data():
    cursor = connection.cursor()

    cursor.execute("""  SELECT category_id, COUNT(*)
                        FROM (  SELECT DISTINCT url, category_id
                                FROM product_data) as brand_products
                        GROUP BY category_id
                        ;""")
    categories_count = cursor.fetchall()

    cursor.execute("""  SELECT id, parent_category_id, name
                        FROM category
                        ;""")
    categories = cursor.fetchall()
    categories_full = {}
    for cat in categories:
        categories_full[cat[0]] = {
            "parent_id": cat[1],
            "name": cat[2]
        }

    cursor.close()

    big_categories_count = {}
    for cat_id, count in categories_count:
        parent_id = get_parent(cat_id, categories_full)
        big_categories_count[parent_id] = big_categories_count.get(parent_id, 0) + count

    return [{"category": categories_full[id]["name"], "count": count} for id, count in big_categories_count.items()]


@app.get("/getChart3Data")
async def getChart3Data():

    cursor = connection.cursor()
    cursor.execute("""  SELECT brand, AVG(rating_value), AVG(price_unit), MAX(price_unit), MIN(price_unit), COUNT(*), COUNT(CASE WHEN availability THEN 1 END)
                        FROM (  SELECT DISTINCT ON (url, timestamp) url, timestamp, brand, rating_value, price_unit, availability
                                FROM product_data
                                ORDER BY url, timestamp DESC) AS data
                        GROUP BY brand
                        ORDER BY COUNT(*) DESC
                        LIMIT 2
                        ;""")
    elem_list = cursor.fetchall()
    cursor.close()

    return [{elem[0]: {
                'nbr of products': elem[5],
                'max price': elem[3],
                'min price': elem[4],
                'mean price': elem[2],
                'nbr of available products': elem[6],
                'rating': elem[1]
            }} for elem in elem_list]

@app.get("/getChart4Data")
async def getChart4Data():

    cursor = connection.cursor()
    cursor.execute("""  SELECT category_id, AVG(price_unit)
                        FROM (  SELECT DISTINCT ON (url) url, timestamp, category_id, price_unit::float
                                FROM product_data
                                WHERE price_unit is not null
                                ORDER BY url, timestamp DESC) AS price_products
                        GROUP BY category_id
                        ;""")
    categories_price_avg = cursor.fetchall()

    cursor.execute("""  SELECT id, parent_category_id, name
                        FROM category
                        ;""")
    categories = cursor.fetchall()
    categories_full = {}
    for cat in categories:
        categories_full[cat[0]] = {
            "parent_id": cat[1],
            "name": cat[2]
        }

    cursor.close()

    big_categories_avg = {}
    for cat_id, avg in categories_price_avg:
        parent_id = get_parent(cat_id, categories_full)
        big_categories_avg[parent_id] = {
            "sum": big_categories_avg.get(parent_id, {}).get("sum", 0) + avg,
            "count": big_categories_avg.get(parent_id, {}).get("count", 0) + 1
        }
    for key in big_categories_avg.keys():
        big_categories_avg[key] = big_categories_avg[key]["sum"]/big_categories_avg[key]["count"]

    return [{"category": categories_full[id]["name"], "price": avg} for id, avg in big_categories_avg.items()]

@app.get("/getNumber1")
async def getNumber1():
    cursor = connection.cursor()
    cursor.execute("""SELECT COUNT(*) FROM (SELECT DISTINCT availability, url FROM product_data) AS prods;""")
    res = cursor.fetchall()[0]
    cursor.close()
    return res[0] if res else -1

@app.get("/getNumber2")
async def getNumber2():
    cursor = connection.cursor()
    cursor.execute("""SELECT COUNT(*) FROM (SELECT DISTINCT availability, url FROM product_data) AS prods WHERE availability = true;""")
    res = cursor.fetchall()[0]
    cursor.close()
    return res[0] if res else -1

@app.get("/getNumber3")
async def getNumber3():
    cursor = connection.cursor()
    cursor.execute("""  SELECT AVG(price_unit)
                        FROM (  SELECT
                                DISTINCT ON (url) url,
                                timestamp,
                                price_unit
                            FROM
                                product_data
 
                               
                            ORDER BY
                                url,
                                timestamp DESC) AS prices;""")
    res = cursor.fetchall()[0]
    cursor.close()
    return res[0] if res else -1

uvicorn.run(app, host='0.0.0.0', port=80)
