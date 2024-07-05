import os
import requests
import pandas as pd
import zipfile
import io
import time
from pymongo import MongoClient
import base64
from concurrent.futures import ThreadPoolExecutor

# Настройка клиента MongoDB
client = MongoClient("mongodb://localhost:27017/")
db = client.gram_base
collection = db.fin_reports
processed_reports_collection = db.processed_reports

COOKIE = os.getenv('COOKIE')
COOKIE_BNS = os.getenv('COOKIE_BNS')

uniq = [
    'Кол-во доставок',
    'Цена розничная с учётом согласованной скидки',
    "Себестоимость сумма",
    '№',
    'СПП',
    'К перечислению за товар, НДС',
    'К перечислению за товар',
    'Сумма комиссии продаж',
    'Вознаграждение поставщику',
    'Вознаграждение покупателю',
    'Себестоимость Сумма',
    'Ставка НДС',
    'Организатор перевозки',
    'Возмещение издержек по перевозке',
    'Наименование банка-эквайера',
    'Возмещение издержек по эквайрингу',
    'Размер снижения кВВ из-за акции, %',
    'Размер снижения кВВ из-за рейтинга, %',
    'Код маркировки',
    'Виды логистики, штрафов и доплат',
    'Номер поставки',
    'Предмет',
    'Код номенклатуры',
    'Бренд',
    'Артикул поставщика',
    'Название',
    'Размер',
    'Баркод',
    'Тип документа',
    'Обоснование для оплаты',
    'Дата заказа покупателем',
    'Дата продажи',
    'Кол-во',
    'Цена розничная',
    'Вайлдберриз реализовал Товар (Пр)',
    'Согласованный продуктовый дисконт, %',
    'Промокод %',
    'Итоговая согласованная скидка',
    'Цена розничная с учетом согласованной скидки',
    'Скидка постоянного Покупателя (СПП)',
    'Размер кВВ, %',
    'Размер  кВВ без НДС, % Базовый',
    'Итоговый кВВ без НДС, %',
    'Вознаграждение с продаж до вычета услуг поверенного, без НДС',
    'Возмещение за выдачу и возврат товаров на ПВЗ',
    'Возмещение расходов по эквайрингу',
    'Вознаграждение Вайлдберриз (ВВ), без НДС',
    'НДС с Вознаграждения Вайлдберриз',
    'К перечислению Продавцу за реализованный Товар',
    'Количество доставок',
    'Количество возврата',
    'Услуги по доставке товара покупателю',
    'Общая сумма штрафов',
    'Доплаты',
    'Обоснование штрафов и доплат',
    'Стикер МП',
    'Наименование банка эквайринга',
    'Номер офиса',
    'Наименование офиса доставки',
    'ИНН партнера',
    'Партнер',
    'Склад',
    'Страна',
    'Тип коробов',
    'Номер таможенной декларации',
    'ШК',
    'Rid',
    'Srid',
    'rari'
]
fifa = [
    'dostavok quantity',
    'cena rosnichnaya',
    "sebestoimost",
    'no',
    'SPP',
    'to transfer for the product, NDS',
    'to transfer for the product',
    'commission amount',
    'seller reward',
    'buyer reward',
    'selfcost sum',
    'NDS bid',
    'transport organizer',
    'transport payment',
    'name bank equiring',
    'equiring payment',
    'kvv downgrade, because of sales',
    'kvv downgrade, because of rating',
    'markup code',
    'logistic_type',
    'delivery_number',
    'item',
    'nomenclature_code',
    'brand',
    'provider_article',
    'name',
    'size',
    'barcode',
    'type_document',
    'justification_for_payment',
    'date_buyer_order',
    'sales_date',
    'amount',
    'retail_price',
    'wb_sold_goods',
    'coordinated_discount',
    'promocode',
    'final_discount',
    'retail_price_with_discount',
    'buyer_discount',
    'kvv',
    'kvv_without_NDS',
    'final_kvv',
    'remuneration',
    'compensation',
    'acquiring',
    'remuneration_WB',
    'NDS_rewards_WB',
    'transfer_of_seller',
    'num_delivery',
    'num_return',
    'delivery_services',
    'total_amount_of_fines',
    'surcharges',
    'rationale',
    'sticker_mp',
    'name_bank',
    'office_number',
    'name_delivery_office',
    'INN_partner',
    'partner',
    'warehouse',
    'country',
    'type_of_boxes',
    'num_customs_declaration',
    'shk',
    'rid',
    'Srid',
    'bebrari'
]

uniq_and_fifa = dict(zip(uniq, fifa))


# Загрузка отчётов
def fetch_reports(url, cookie):
    headers = {'Cookie': cookie}
    response = requests.get(url, headers=headers)
    response.raise_for_status()
    reports = response.json()['data']['reports']
    return [report['id'] for report in reports]


def retry_request(url, headers, max_retries=5, delay=5):
    for attempt in range(max_retries):
        response = requests.get(url, headers=headers)
        if response.status_code == 200:
            return response
        elif response.status_code == 429:
            print(f"Rate limit exceeded. Retrying in {delay} seconds...")
            time.sleep(delay)
        else:
            response.raise_for_status()
    raise Exception(f"Failed to fetch {url} after {max_retries} attempts")


def download_report(report_id, report_type, cookie):
    if report_type == "new":
        if cookie == COOKIE_BNS:
            url = f"https://seller-weekly-report.wildberries.ru/ns/reports/seller-wb-balance/api/v1/reports-weekly/{report_id}/details/archived-excel"
        else:
            url = f"https://seller-services.wildberries.ru/ns/reports/sup-balance/api/v1/reports-weekly/{report_id}/details/archived-excel"
    else:
        url = f"https://seller-weekly-report.wildberries.ru/ns/realization-reports/suppliers-portal-analytics/api/v1/reports/{report_id}/details/archived-excel"

    headers = {'Cookie': cookie}
    response = retry_request(url, headers)

    file_content = response.json()['data']['file']
    if isinstance(file_content, str):
        file_content = base64.b64decode(file_content)

    zip_file = zipfile.ZipFile(io.BytesIO(file_content))
    report_path = f'reports/{report_id}'
    os.makedirs(report_path, exist_ok=True)
    zip_file.extractall(report_path)
    return report_path


def process_excel_file(file_path):
    df = pd.read_excel(file_path, sheet_name=0)
    df.rename(columns=uniq_and_fifa, inplace=True)
    df['company'] = 'default_company'
    records = df.to_dict('records')
    collection.insert_many(records)


def process_report(report_id, report_type, cookie):
    try:
        with client.start_session() as session:
            with session.start_transaction():
                if processed_reports_collection.find_one({"report_id": report_id}):
                    print(f"Report {report_id} already processed.")
                    return

                report_path = download_report(report_id, report_type, cookie)
                for filename in os.listdir(report_path):
                    file_path = os.path.join(report_path, filename)
                    process_excel_file(file_path)

                processed_reports_collection.insert_one({"report_id": report_id})
    except Exception as e:
        print(f"Failed to process report {report_id}. Error: {e}")


if __name__ == "__main__":
    cookies_urls = [
        (COOKIE, "https://seller-services.wildberries.ru/ns/reports/sup-balance/api/v1/reports-weekly?limit=200&searchBy=&skip=0&type=7"),
        (COOKIE_BNS, "https://seller-weekly-report.wildberries.ru/ns/reports/seller-wb-balance/api/v1/reports-weekly?limit=5&searchBy=&skip=0&type=6")
    ]
    old_reports_url = "https://seller-weekly-report.wildberries.ru/ns/realization-reports/suppliers-portal-analytics/api/v1/reports?limit=300&searchBy=&skip=0&type=2"

    report_ids_with_types = []

    for cookie, new_reports_url in cookies_urls:
        new_report_ids = fetch_reports(new_reports_url, cookie)
        old_report_ids = fetch_reports(old_reports_url, cookie)

        new_report_ids_with_type = [(report_id, "new", cookie) for report_id in new_report_ids]
        old_report_ids_with_type = [(report_id, "old", cookie) for report_id in old_report_ids]

        report_ids_with_types.extend(new_report_ids_with_type + old_report_ids_with_type)

    with ThreadPoolExecutor(max_workers=5) as executor:
        executor.map(lambda args: process_report(*args), report_ids_with_types)