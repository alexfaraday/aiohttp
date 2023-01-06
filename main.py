from aiohttp import web
import sqlite3
from sqlite3 import connect
import pandas as pd
import json
from sqlalchemy import insert
import aiosqlite
import random
import requests
import datetime

from datetime import date
from dateutil.relativedelta import relativedelta
conn = sqlite3.connect('firstdz.db')
cur = conn.cursor()





cur.execute("""CREATE TABLE IF NOT EXISTS shops (
   shopid INTEGER PRIMARY KEY AUTOINCREMENT,
   shopname TEXT,
   shopadress TEXT);
""")

cur.execute("""CREATE TABLE IF NOT EXISTS products (
   productid INTEGER PRIMARY KEY AUTOINCREMENT,
   price INT,
   productname TEXT);
""")

cur.execute("""CREATE TABLE IF NOT EXISTS orders (
   orderid INTEGER PRIMARY KEY AUTOINCREMENT,
   storeid INT REFERENCES shops(shopid),
   totalsum INT,
   saleproductid INTEGER REFERENCES products(productid),
   date DATE);
""")

db = sqlite3.connect('firstdz.db')
cursor =db.execute("SELECT * FROM shops ")
rows = cursor.fetchall()

if rows:
    print('БД наполнена можно работать')
else:
    print('Необходимо наполнить БД, пожалуйста, подождите')
    SQLSHOPS="""INSERT INTO `shops` ( `shopname`,`shopadress`) VALUES
  ("Adrienne Hyde","441-1402 Nibh Ave"),
  ("Zachary Aguirre","Ap #670-8556 Vitae Rd."),
  ("Sydnee Cole","P.O. Box 694, 7288 Vestibulum Road"),
  ("Tashya Ross","Ap #580-9197 Nascetur St."),
  ("Pascale Sawyer","Ap #390-9303 Et, Street"),
  ("Rinah Cline","942-6367 Amet, Street"),
  ("Baker Wiggins","Ap #146-8463 Tempor Rd."),
  ("Zephania Burnett","Ap #777-8441 Cursus St."),
  ("Sharon Reid","P.O. Box 111, 8003 Eget, Rd."),
  ("Rebecca Warren","Ap #564-8454 Magna Ave"),
  ("Giacomo Shepard","Ap #372-9125 Quisque St."),
  ("Erica Hoover","538-7507 Duis Rd."),
  ("Kirby Dixon","P.O. Box 129, 2660 Dignissim. Avenue"),
  ("Jane Chen","P.O. Box 774, 5006 Ipsum. Av."),
  ("Judah Mendez","5120 Ac, Rd.");
"""
    SQLPRODUCTS="""INSERT INTO products (price,productname)
VALUES
  ("250","beef"),
  ("150","duck"),
  ("100","ham"),
  ("350","mutton"),
  ("400","turkey"),
   ("450","trout"),
  ("100","sole"),
  ("250","asparagus"),
  ("100","avocado "),
  ("150","pea"),
   ("100","potato"),
  ("250","almond"),
  ("100","barley"),
  ("250","butter"),
  ("100","cream"),
  ("100","chocolate"),
  ("250","marshmallow"),
  ("100","coffee ");"""

    cursor = db.execute(SQLSHOPS)
    cursor = db.execute(SQLPRODUCTS)
    db.commit()
    for i in range(1, 100):
        randshop = random.randint(1, 15)
        randproduct = random.randint(1, 18)
        randomdate=date.today()+ relativedelta(days=-(random.randint(1, 40)))
        SQL = "SELECT * FROM products WHERE productid=" + str(randproduct)


        cursor = db.execute(SQL)
        rows =cursor.fetchall()
        SQLORDER="INSERT INTO orders ('storeid','totalsum','saleproductid', 'date') VALUES(" + str(randshop) + "," + str(rows[0][1]) + ",'" + str(randproduct) + "','"+ str(randomdate)+"')"
        db.commit()
        cursor = db.execute(SQLORDER)




    cursor.close()
    db.close()
    print('БД наполнена можно работать')
    print('Получить все товарные позиции:http://localhost:8080/allproducts')
    print('Получить все магазины:http://localhost:8080/allshops')
    print('Получить топ 10 самых доходных магазинов за месяц:http://localhost:8080/bestshop')
    print('Получить топ 10  самых продаваемых товаров:http://localhost:8080/bestsales')
    print('Прочитать readme')










async def handle(request):
    name = request.match_info.get('name', "Anonymous")
    text = "Hello, " + name
    conn.close()
    return web.Response(text=text)


async def allshops(request):

    db = await aiosqlite.connect('firstdz.db')
    cursor = await db.execute("SELECT * FROM shops ")
    rows = await cursor.fetchall()
    print(rows)
    #jsonString = json.dumps(rows)
    #await db.commit()
    await cursor.close()
    await db.close()
    #return web.json_response({'ok': True})
    return web.json_response({'code': 1, 'data':rows})


async def allproducts(request):

    db = await aiosqlite.connect('firstdz.db')
    cursor = await db.execute("SELECT * FROM products ")
    rows = await cursor.fetchall()
    print(rows)
    #jsonString = json.dumps(rows)
    #await db.commit()
    await cursor.close()
    await db.close()
    #return web.json_response({'ok': True})
    return web.json_response({'code': 1, 'data':rows})


async def bestsales(request):

    db = await aiosqlite.connect('firstdz.db')
    cursor = await db.execute("select saleproductid,productname, count(saleproductid) from orders INNER JOIN products ON saleproductid=productid  group by saleproductid order by count(saleproductid) desc limit 10;")
    rows = await cursor.fetchall()
    print(rows)
    #jsonString = json.dumps(rows)
    #await db.commit()
    await cursor.close()
    await db.close()
    #return web.json_response({'ok': True})
    return web.json_response({'code': 1, 'data':rows})

async def bestshop(request):
    one_month = date.today() + relativedelta(months=-1)
    today = date.today()+ relativedelta(days=+1)
    #today=today.strftime("%Y-%m-%d")
    print(today)
    print(one_month)


    db = await aiosqlite.connect('firstdz.db')
    SQL="select saleproductid, sum(totalsum) AS totalsales, shopadress from orders INNER JOIN shops ON storeid=shopid  WHERE date >='"+str(one_month)+"' AND date <='"+str(today)+"'  group by saleproductid order by sum(totalsum) desc limit 10"
    print(SQL)
    cursor = await db.execute(SQL)
    rows = await cursor.fetchall()
    print(rows)
    await cursor.close()
    await db.close()
    return web.json_response({'code': 200, 'data':rows})



async def sendorder(request):
    if request.method == "POST":
        data = await request.json()
        db = await aiosqlite.connect('firstdz.db')
        cursor = await db.execute("INSERT INTO orders ('storeid','totalsum','saleproductid', 'date') VALUES("+str(data['storeid'])+","+str(data['totalsum'])+",'"+str(data['saleproductid'])+"', date('now'))")
        await db.commit()
        await cursor.close()
        await db.close()
        return web.json_response({'ok': True})

async def makeorders(request):
    randshop=random.randint (1,5)
    randproduct=random.randint (1,6)

    #url = "http://localhost:8080/sendorder"


    db = await aiosqlite.connect('firstdz.db')
    SQL="SELECT * FROM products WHERE productid="+str(randproduct)

    print(SQL)
    cursor = await db.execute(SQL)
    rows = await cursor.fetchall()
    cursor = await db.execute("INSERT INTO orders ('storeid','totalsum','saleproductid', 'date') VALUES(" + str(randshop) + "," + str(rows[0][1]) + ",'" + str(randproduct) + "', date('now'))")
    await db.commit()
    await cursor.close()
    await db.close()




    #msg = "Твой текст!"
    #data = {"shopid": randshop,"productid": randproduct,"sum": rows[0][1]}
    #print(data)
    #response = await requests.post(url, data={"shopid": randshop,"productid": randproduct,"sum": rows[0][1]})
    return web.json_response({'ok': True})

    #response = requests.post(url, data=json.dumps(data)).json()

    #answer = response.get("replies")
    #print(*answer)



app = web.Application()
app.add_routes([web.get('/', handle),
                web.get('/allshops', allshops),
                web.post('/sendorder', sendorder),
                web.get('/allproducts', allproducts),
                web.get('/makeorders', makeorders),
                web.get('/bestsales', bestsales),
                web.get('/bestshop', bestshop)])


if __name__ == '__main__':
    web.run_app(app)

