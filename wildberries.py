import requests
import sqlite3
import json
import datetime
import time

# api documentation
# https://images.wbstatic.net/portal/education/Kak_rabotat'_s_servisom_statistiki.pdf?abc=1612952230000

WB_API1 = 'MjdiN2Q5YTEtYjk1Yy00YzZlLThmZWItNGQyNGE4Y2UxOTI2'
WB_API2 = '27b7d9a1-b95c-4c6e-8feb-4d24a8ce1926'

# DB_LOG = 'db_logs.db'
DB_SALES = 'main_database.db'
DATE = '2022-06-01'

def generate_date(start, deep):
    return (start - datetime.timedelta(days=deep)).strftime('%Y-%m-%d')


def get_sales(date, flag=1):  # db_log=False, db_sales=False,
    URL = "https://suppliers-stats.wildberries.ru/api/v1/supplier/sales"
    PARAMS = {'dateFrom': date, 'flag': flag, 'key': WB_API1}
    r = requests.get(url=URL, params=PARAMS)

    for i in range(15):
        if r.status_code == 429:
            sleep_time = i * 3
            print('Sleeping {}'.format(sleep_time))
            time.sleep(sleep_time)
        else:
            break
    if r.status_code == 429:
        return ValueError

    data = r.json()

    # if db_log:
    #     add_response_to_db(data=data, params=PARAMS)
    #
    # if db_sales:
    #     add_lines_to_database(lines=data)
    return data


# def add_response_to_db(data, params):
#
#     sql_ = '''
#         INSERT INTO
#             sales (created, request, response)
#             VALUES (:created, :request, :response)'''
#
#     par_ = {
#         'created': datetime.datetime.now().isoformat(),
#         'request': json.dumps(params, indent=2),
#         'response': json.dumps(data, indent=2)}
#
#     with sqlite3.connect(DB_LOG) as conn:
#         c = conn.cursor()
#         c.execute('''CREATE TABLE IF NOT EXISTS
#         sales (
#             created TEXT NOT NULL,
#             request TEXT NOT NULL,
#             response TEXT NOT NULL)''')
#         c.execute(sql_, par_)


def add_lines_to_database(lines):
    with sqlite3.connect(DB_SALES) as conn:
        c = conn.cursor()

        c.execute('''
        CREATE TABLE IF NOT EXISTS sales_wb (
            date                TEXT,
            lastChangeDate      TEXT,
            supplierArticle     TEXT,
            techSize            INTEGER,
            barcode             TEXT,
            totalPrice          REAL,
            discountPercent     REAL,
            isSupply            BOOLEAN,
            isRealization       BOOLEAN,
            promoCodeDiscount   REAL,
            warehouseName       TEXT,
            countryName         TEXT,
            oblastOkrugName     TEXT,
            regionName          TEXT,
            incomeID            INTEGER,
            saleID              TEXT,
            odid                INTEGER,
            spp                 INTEGER,
            forPay              REAL,
            finishedPrice       REAL,
            priceWithDisc       REAL,
            nmId                INTEGER,
            subject             TEXT,
            category            TEXT,
            brand               TEXT,
            IsStorno            INTEGER,
            gNumber             TEXT,
            sticker             TEXT
 )
        ''')

        sql_ = '''
        INSERT INTO sales_wb (
            date,           lastChangeDate,     supplierArticle,    techSize,
            barcode,        totalPrice,         discountPercent,    isSupply,
            isRealization,  promoCodeDiscount,  warehouseName,      countryName,
            oblastOkrugName,regionName,         incomeID,           saleID,
            odid,           spp,                forPay,             finishedPrice,
            priceWithDisc,  nmId,               subject,            category,
            brand,          IsStorno,           gNumber,            sticker
            )
        VALUES (
            :date,          :lastChangeDate,    :supplierArticle,   :techSize,
            :barcode,       :totalPrice,        :discountPercent,   :isSupply,
            :isRealization, :promoCodeDiscount, :warehouseName,     :countryName,
            :oblastOkrugName,:regionName,       :incomeID,          :saleID,
            :odid,          :spp,               :forPay,            :finishedPrice,
            :priceWithDisc, :nmId,              :subject,           :category,
            :brand,         :IsStorno,          :gNumber,           :sticker)
            '''

        c.executemany(sql_, lines)


def main():
    with sqlite3.connect(DB_SALES) as conn:
        c = conn.cursor()
        c.execute('DROP TABLE IF EXISTS sales_wb')

    # with sqlite3.connect(DB_LOG) as conn:
    #     c = conn.cursor()
    #     c.execute('DROP TABLE IF EXISTS sales')

    date_from = generate_date(datetime.datetime.now(), 90)
    data = get_sales(date_from, flag=0)
    print('Date from: {}. Num lines: {}'.format(date_from, len(data)))
    add_lines_to_database(data)




