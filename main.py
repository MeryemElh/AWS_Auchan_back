from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
import psycopg2
import uvicorn
from typing import List, Tuple
import logging
logger = logging.getLogger()

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

def get_cursor():
    global connection
    
    try:
        cursor = connection.cursor()
    except psycopg2.InterfaceError:
        connection = psycopg2.connect(
            host=pg_host,
            database=pg_db,
            user=pg_user,
            password=pg_pwd,
            options='-c statement_timeout=15000'
        )
        cursor = connection.cursor()
    
    return cursor


@app.get("/getChart1Data")
async def getChart1Data():
    cursor = get_cursor()

    cursor.execute("""  SELECT brand, COUNT(*)
                        FROM (  SELECT DISTINCT url, brand
                                FROM product_data
                                WHERE brand is not null) as brand_products
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
    cursor = get_cursor()

    cursor.execute("""  SELECT parent_category.name, COUNT(*)
                        FROM (  SELECT DISTINCT url, category_id
                                FROM product_data) as brand_products, category AS child_category, category AS parent_category
                        WHERE child_category.id = category_id AND child_category.first_parent_category_id = parent_category.id
                        GROUP BY parent_category.name
                        ORDER BY COUNT(*) DESC
                        LIMIT 6
                        ;""")

    res = cursor.fetchall()
    
    return [{"category": elem[0], "count": elem[1]} for elem in res]


@app.get("/getChart3Data")
async def getChart3Data():
    cursor = get_cursor()

    cursor.execute("""  SELECT brand, AVG(rating_value), AVG(price_unit), MAX(price_unit), MIN(price_unit), COUNT(*), COUNT(CASE WHEN availability THEN 1 END)
                        FROM (  SELECT DISTINCT ON (url, timestamp) url, timestamp, brand, rating_value, price_unit, availability
                                FROM product_data
                                WHERE brand IS NOT null
                                ORDER BY url, timestamp DESC) AS data
                        GROUP BY brand
                        ORDER BY COUNT(*) DESC
                        LIMIT 2
                        ;""")
    elem_list = cursor.fetchall()
    cursor.close()

    elem_dict = {}
    for elem in elem_list:
        elem_dict[elem[0]] = {
            'name': elem[0],
            'nbr_prod': elem[5],
            'max': elem[3],
            'min': elem[4],
            'mean': elem[2],
            'nbr_avail': elem[6],
            'mean_rating': elem[1]
        }

    return elem_dict

@app.get("/getChart4Data")
async def getChart4Data():
    cursor = get_cursor()

    cursor.execute("""  SELECT parent_category.name, AVG(price_unit)
                        FROM (  SELECT DISTINCT ON (url) url, timestamp, category_id, price_unit::float
                                FROM product_data
                                WHERE price_unit is not null
                                ORDER BY url, timestamp DESC) AS price_products, category AS child_category, category AS parent_category
                        WHERE child_category.id = category_id AND child_category.first_parent_category_id = parent_category.id
                        GROUP BY parent_category.name
                        ORDER BY AVG(price_unit) DESC
                        LIMIT 6
                        ;""")

    res = cursor.fetchall()

    return [{"category": elem[0], "price": elem[1]} for elem in res]

@app.get("/getNumber1")
async def getNumber1():
    cursor = get_cursor()

    cursor.execute("""SELECT COUNT(*) FROM (SELECT DISTINCT availability, url FROM product_data) AS prods;""")
    res = cursor.fetchall()[0]
    cursor.close()
    return res[0] if res else -1

@app.get("/getNumber2")
async def getNumber2():
    cursor = get_cursor()

    cursor.execute("""SELECT COUNT(*) FROM (SELECT DISTINCT availability, url FROM product_data) AS prods WHERE availability = true;""")
    res = cursor.fetchall()[0]
    cursor.close()
    return res[0] if res else -1

@app.get("/coupe", tags=['test'])
async def coupe():
    connection.close()

@app.get("/getNumber3")
async def getNumber3():
    cursor = get_cursor()

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
