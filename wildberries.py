import requests
import sqlite3
import json
import datetime
import time
import pandas as pd

# api documentation
# https://images.wbstatic.net/portal/education/Kak_rabotat'_s_servisom_statistiki.pdf?abc=1612952230000

API_KEY = 'MjdiN2Q5YTEtYjk1Yy00YzZlLThmZWItNGQyNGE4Y2UxOTI2'
# WB_API2 = '27b7d9a1-b95c-4c6e-8feb-4d24a8ce1926'

# DB_LOG = 'db_logs.db'
DB_SALES = 'main_database.db'
DATE = '2022-06-01'

def generate_date(start, deep):
    return (start - datetime.timedelta(days=deep)).strftime('%Y-%m-%d')

def api_sales_by_realization(date_from, date_to, limit=100000, rrdid=0):
    """
    Отчет о продажах по реализации

    :param dateFrom – начальная дата периода. (Например, если dateFrom = 2020-07-06)
    :param dateTo – конечная дата периода.
    :param limit – максимальное количество строк отчета получаемых в результате вызова API.
        Рекомендуем загружать отчет небольшими частями, например, по 100 000 строк на один вызов.
    :param rrdid – уникальный идентификатор строки отчета.
        Необходим для получения отчета частями.
        Загрузку отчета нужно начинать с rrdid = 0, и при последующих вызовах API передавать
        максимальное значение rrdid полученное в результате предыдущего вызова.
        Таким образом для загрузки одного отчета может понадобится вызывать API до тех пор,
        пока количество возвращаемых строк не станет равным нулю.
    """
    url_ = 'https://suppliers-stats.wildberries.ru/api/v1/supplier/reportDetailByPeriod'
    pars_ = {'dateFrom': date_from, 'dateTo': date_to, 'limit': limit,
              'rrdid': rrdid, 'key': API_KEY}
    r = requests.get(url=url_, params=pars_)
    if r.status_code != 429:
        data = r.json()
        return data

def api_sales_report(date_from, flag=0):
    """
    ОТЧЕТ ПРОДАЖИ

    :param date_from:
    :param flag:
        flag = 0 либо пропущено:
            Возвращает все значения, где lastChangeDate (дата время обновления информации в сервисе)
            больше параметра dateFrom.

        flag = 1,
            Возвращает все значения, где дата равна
            переданному параметру dateFrom (в данном случае время в дате значения не имеет).
            При этом количество возвращенных строк данных равно количеству всех заказов или продаж,
            сделанных в дате, переданной в параметре dateFrom.

    :return:
    """
    URL = "https://suppliers-stats.wildberries.ru/api/v1/supplier/sales"
    PARAMS = {'dateFrom': date_from, 'flag': flag, 'key': API_KEY}
    r = requests.get(url=URL, params=PARAMS)

    if r.status_code != 429:
        data = r.json()
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


# def add_lines_to_database(lines):
#     with sqlite3.connect(DB_SALES) as conn:
#         c = conn.cursor()
#
#         c.execute('''
#         CREATE TABLE IF NOT EXISTS sales_wb (
#             date                TEXT,
#             lastChangeDate      TEXT,
#             supplierArticle     TEXT,
#             techSize            INTEGER,
#             barcode             TEXT,
#             totalPrice          REAL,
#             discountPercent     REAL,
#             isSupply            BOOLEAN,
#             isRealization       BOOLEAN,
#             promoCodeDiscount   REAL,
#             warehouseName       TEXT,
#             countryName         TEXT,
#             oblastOkrugName     TEXT,
#             regionName          TEXT,
#             incomeID            INTEGER,
#             saleID              TEXT,
#             odid                INTEGER,
#             spp                 INTEGER,
#             forPay              REAL,
#             finishedPrice       REAL,
#             priceWithDisc       REAL,
#             nmId                INTEGER,
#             subject             TEXT,
#             category            TEXT,
#             brand               TEXT,
#             IsStorno            INTEGER,
#             gNumber             TEXT,
#             sticker             TEXT
#  )
#         ''')
#
#         sql_ = '''
#         INSERT INTO sales_wb (
#             date,           lastChangeDate,     supplierArticle,    techSize,
#             barcode,        totalPrice,         discountPercent,    isSupply,
#             isRealization,  promoCodeDiscount,  warehouseName,      countryName,
#             oblastOkrugName,regionName,         incomeID,           saleID,
#             odid,           spp,                forPay,             finishedPrice,
#             priceWithDisc,  nmId,               subject,            category,
#             brand,          IsStorno,           gNumber,            sticker
#             )
#         VALUES (
#             :date,          :lastChangeDate,    :supplierArticle,   :techSize,
#             :barcode,       :totalPrice,        :discountPercent,   :isSupply,
#             :isRealization, :promoCodeDiscount, :warehouseName,     :countryName,
#             :oblastOkrugName,:regionName,       :incomeID,          :saleID,
#             :odid,          :spp,               :forPay,            :finishedPrice,
#             :priceWithDisc, :nmId,              :subject,           :category,
#             :brand,         :IsStorno,          :gNumber,           :sticker)
#             '''
#
#         c.executemany(sql_, lines)
#

def main_sales(date_start, single_day=False):
    data = api_sales_report(date_start, flag=1 if single_day else 0)
    df = pd.DataFrame(data)
    print('Num lines: {}'.format(len(data)))
    with sqlite3.connect(DB_SALES) as conn:
        conn.execute('DROP TABLE IF EXISTS sales_main')
        df.to_sql('sales_main', conn, index=False)
        df.to_csv('sales_main.csv', index=False)

def main_realization_sales(date_from, date_to):
    data = api_sales_by_realization(date_from=date_from, date_to=date_to)
    df = pd.DataFrame(data)
    print('Num lines: {}'.format(len(data)))
    with sqlite3.connect(DB_SALES) as conn:
        conn.execute('DROP TABLE IF EXISTS sales_realization')
        df.to_sql('sales_realization', conn, index=False)
        df.to_csv('sales_realization.csv', index=False)


def demo():
    main_sales('2022-05-01')
    time.sleep(10)
    main_realization_sales('2022-05-01', '2022-06-30')





