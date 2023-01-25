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
    return [{"brand": "AUCHAN BIO", "count": 5}, {"brand": "USHUAIA", "count": 2}]
    cursor = connection.cursor()
    cursor.execute("""  SELECT brand, COUNT(*)
                        FROM (  SELECT DISTINCT url, brand
                                FROM product_data) as brand_products
                        GROUP BY brand
                        ;""")
    cursor.close()

    return [{'brand': elem[0], 'count': elem[1]} for elem in cursor.fetchall()]

def get_parent(id: str, liste : List[Tuple[str]]):
    if not liste[id]["parent_id"] or not liste[liste[id]["parent_id"]]["parent_id"]:
        return id
    
    return get_parent(liste[id]["parent_id"], liste)
    

@app.get("/getChart2Data")
async def getChart2Data():
    return [{"category": "Fruits et légumes frais", "count": 5}, {"category": "Hygiène, beauté, parapharmacie", "count": 2}]
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
        big_categories_count[parent_id] = big_categories_count.get(str(parent_id), 0) + count

    return [{"category": categories_full[id]["name"], "count": count} for id, count in big_categories_count.items()]


@app.get("/getChart3Data")
async def getChart3Data():
    return {
        "AUCHAN BIO": {
            "nbr_prod": "5", "max": "7.96", "min": "2.35", "mean": "5.14", "nbr_avail": "5", "mean_rating": "3.6"
        },
        "USHUAIA": {
            "nbr_prod": "2", "max": "10.99", "min": "10.99", "mean": "10.99", "nbr_avail": "2", "mean_rating": "4.7"
        }
    }
    cursor = connection.cursor()
    cursor.execute("""SELECT* FROM product_data;""")

@app.get("/getChart4Data")
async def getChart4Data():
    return [{"category": "Fruits et légumes frais", "price": 5.14}, {"category": "Hygiène, beauté, parapharmacie", "price": 10.99}]
    cursor = connection.cursor()
    cursor.execute("""SELECT* FROM product_data;""")

@app.get("/getNumber1")
async def getNumber1():
    return 7
    cursor = connection.cursor()
    cursor.execute("""SELECT* FROM product_data;""")

@app.get("/getNumber2")
async def getNumber2():
    return 7
    cursor = connection.cursor()
    cursor.execute("""SELECT* FROM product_data;""")

@app.get("/getNumber3")
async def getNumber3():
    return 6.81
    cursor = connection.cursor()
    cursor.execute("""SELECT* FROM product_data;""")

uvicorn.run(app, host='0.0.0.0', port=80)
