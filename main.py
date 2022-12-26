from aiohttp import web
import sqlite3
from sqlite3 import connect
import pandas as pd
import json
from sqlalchemy import insert
import aiosqlite
conn = sqlite3.connect('firstdz.db')
cur = conn.cursor()



cur.execute("""CREATE TABLE IF NOT EXISTS shops (
   shopid INTEGER PRIMARY KEY AUTOINCREMENT,
   shopname TEXT,
   shopadress TEXT);
""")

cur.execute("""CREATE TABLE IF NOT EXISTS products (
   priductid INTEGER PRIMARY KEY AUTOINCREMENT,
   price INT,
   productname TEXT);
""")

cur.execute("""CREATE TABLE IF NOT EXISTS orders (
   orderid INTEGER PRIMARY KEY AUTOINCREMENT,
   shopid INT,
   sum INT,
   productid INT,
   date DATETIME);
""")

print('http://localhost:8080/allshops')
conn.commit()


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
    cursor = await db.execute("select productname, sum(price) from products group by productname order by sum(price) desc limit 3;")
    rows = await cursor.fetchall()
    print(rows)
    #jsonString = json.dumps(rows)
    #await db.commit()
    await cursor.close()
    await db.close()
    #return web.json_response({'ok': True})
    return web.json_response({'code': 1, 'data':rows})

async def bestshop(request):

    db = await aiosqlite.connect('firstdz.db')
    cursor = await db.execute("select productname, sum(price) from products group by productname order by sum(price) desc limit 3;")
    rows = await cursor.fetchall()
    print(rows)
    #jsonString = json.dumps(rows)
    #await db.commit()
    await cursor.close()
    await db.close()
    #return web.json_response({'ok': True})
    return web.json_response({'code': 1, 'data':rows})



async def sendorder(request):
    if request.method == "POST":
        data = await request.json()
        db = await aiosqlite.connect('firstdz.db')
        cursor = await db.execute("INSERT INTO orders ('shopid','sum','productid', 'date') VALUES("+str(data['shopid'])+","+str(data['sum'])+",'"+str(data['sum'])+"', datetime('now'))")
        await db.commit()
        await cursor.close()
        await db.close()
        return web.json_response({'ok': True})




app = web.Application()
app.add_routes([web.get('/', handle),
                web.get('/allshops', allshops),
                web.post('/sendorder', sendorder),
                web.get('/allproducts', allproducts),
                web.get('/bestsales', bestsales),
                web.get('/bestshop', bestshop)])

if __name__ == '__main__':
    web.run_app(app)

