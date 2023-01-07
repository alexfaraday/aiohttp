from aiohttp import web
import sqlite3
import aiosqlite
import random
from datetime import date, datetime
from dateutil.relativedelta import relativedelta



#создаем и наполняем БД при первом запуске
conn = sqlite3.connect('firstdz.db')
cur = conn.cursor()

#создаем таблицу с магазинами
cur.execute("""CREATE TABLE IF NOT EXISTS shops (  
   shopid INTEGER PRIMARY KEY AUTOINCREMENT,
   shopname TEXT,
   shopadress TEXT);
""")
#создаем таблицу с продуктами
cur.execute("""CREATE TABLE IF NOT EXISTS products (
   productid INTEGER PRIMARY KEY AUTOINCREMENT,
   price INT,
   productname TEXT);
""")
#создаем таблицу с заказами
cur.execute("""CREATE TABLE IF NOT EXISTS orders (
   orderid INTEGER PRIMARY KEY AUTOINCREMENT,
   storeid INT REFERENCES shops(shopid),
   totalsum INT,
   saleproductid INTEGER REFERENCES products(productid),
   date DATE);
""")

#проверяем наполненность базы
db = sqlite3.connect('firstdz.db')
cursor =db.execute("SELECT * FROM shops ")
rows = cursor.fetchall()
#если база наполнена приступаем к работе
if rows:
    print('БД наполнена можно работать\n')
    print('Прочитать readme:http://localhost:8080/\n')
    print('Получить все товарные позиции:http://localhost:8080/allproducts')
    print('Получить все магазины:http://localhost:8080/allshops')
    print('Получить топ 10 самых доходных магазинов за месяц:http://localhost:8080/bestshop')
    print('Получить топ 10  самых продаваемых товаров:http://localhost:8080/bestsales\n\n')

#если база не наполнена, начинаем ее наполнять
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
    #наполняем БД случайными транзакциями
    for i in range(1, 500):
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
#завершили наполнение базы
    print('БД наполнена можно работать\n')
    print('Прочитать readme:http://localhost:8080/\n')
    print('Получить все товарные позиции:http://localhost:8080/allproducts')
    print('Получить все магазины:http://localhost:8080/allshops')
    print('Получить топ 10 самых доходных магазинов за месяц:http://localhost:8080/bestshop')
    print('Получить топ 10  самых продаваемых товаров:http://localhost:8080/bestsales')
    print('Прочитать readme:http://localhost:8080/')










async def readme(request):

    text = "1.Для того чтобы создать заказ необходимо отправить POST запрос на адрес: \n  http://localhost:8080/sendorder \n \n {'storeid': '5','saleproductid': '17','totalsum': '350'} \n\n в ответ вернется подтверждение записи \n \n{'code': 200,'message': 'success insert'}\n\n 2.Получить все товарные позиции:http://localhost:8080/allproducts \n\n 3.Получить все магазины:http://localhost:8080/allshops \n\n 4.Получить топ 10 самых доходных магазинов за месяц:http://localhost:8080/bestshop \n\n 5.Получить топ 10  самых продаваемых товаров:http://localhost:8080/bestsales \n\n"
    conn.close()
    return web.Response(text=text)

#функция получения всех магазинов
async def allshops(request):
    db = await aiosqlite.connect('firstdz.db')
    cursor = await db.execute("SELECT * FROM shops ")
    rows = await cursor.fetchall()

    #print (json.dumps(rows, default=json_serial))

    await cursor.close()
    await db.close()
    return web.json_response({'code': 200, 'shops':rows})

#функция получения всех продуктов
async def allproducts(request):
    db = await aiosqlite.connect('firstdz.db')
    cursor = await db.execute("SELECT * FROM products ")
    rows = await cursor.fetchall()
    await cursor.close()
    await db.close()
    return web.json_response({'code': 200, 'products':rows})

#функция получения 10 самых продаваемых товаров
async def bestsales(request):
    db = await aiosqlite.connect('firstdz.db')
    cursor = await db.execute("select saleproductid,productname, count(saleproductid) from orders INNER JOIN products ON saleproductid=productid  group by saleproductid order by count(saleproductid) desc limit 10;")
    rows = await cursor.fetchall()
    await cursor.close()
    await db.close()
    return web.json_response({'code': 200, 'data':rows})

#функция получения 10 самых доходных магазинов за 30 дней
async def bestshop(request):
    one_month = date.today() + relativedelta(months=-1)
    today = date.today()
    db = await aiosqlite.connect('firstdz.db')
    SQL="select saleproductid, sum(totalsum) AS totalsales, shopadress from orders INNER JOIN shops ON storeid=shopid  WHERE date >='"+str(one_month)+"' AND date <='"+str(today)+"'  group by saleproductid order by sum(totalsum) desc limit 10"
    cursor = await db.execute(SQL)
    rows = await cursor.fetchall()
    await cursor.close()
    await db.close()
    return web.json_response({'code': 200, 'data':rows})


#функция обработки продажи
async def sendorder(request):
    if request.method == "POST":
        data = await request.json()
        db = await aiosqlite.connect('firstdz.db')
        cursor = await db.execute("INSERT INTO orders ('storeid','totalsum','saleproductid', 'date') VALUES("+str(data['storeid'])+","+str(data['totalsum'])+",'"+str(data['saleproductid'])+"', date('now'))")
        await db.commit()
        await cursor.close()
        await db.close()
        return web.json_response({'code': 200, 'message':'success insert'})





app = web.Application()
app.add_routes([web.get('/', readme),
                web.get('/allshops', allshops),
                web.post('/sendorder', sendorder),
                web.get('/allproducts', allproducts),
                web.get('/bestsales', bestsales),
                web.get('/bestshop', bestshop)])


if __name__ == '__main__':
    web.run_app(app)

