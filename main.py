import asyncio
import csv

from requests_html import AsyncHTMLSession
from bs4 import BeautifulSoup

# словарь с результатами парсинга
result_dict = {}
# список ссылок на страницы товаров
page_urls = ['https://online.metro-cc.ru/category/ofis-obuchenie-hobbi/pismennye-prinadlezhnosti']

# генерируем недостоющие ссылка
for i in range(1, 5):
    page_urls.append(f'{page_urls[0]}?page={i}')


async def pars_data_brend(session, url, key_dict):
    """
    :param session: объект сессии
    :param url: ссылка на товар
    :param key_dict: id товара из result_dict
    :return:
    """
    r = await session.get(url)
    soup = BeautifulSoup(r.text, 'html.parser')
    product_atr = soup.find('div', {'class': 'product-page-content__labels-and-short-attrs'}).find_all('li', {
        'class': 'product-attributes__list-item'})

    for itm in product_atr:
        if 'Бренд' in itm.prettify():
            brend = itm.find('a').text.strip()
            result_dict[key_dict].append(brend)


async def pars_page_goods(session, url):
    """
    :param session: объект сессии
    :param url: ссылка на страницу товаров
    :return: при сборе 100 товаров
    """
    r = await session.get(url)

    soup = BeautifulSoup(r.text, 'html.parser')
    elements = soup.findAll('div', {
        'class': 'catalog-2-level-product-card product-card subcategory-or-type__products-item with-prices-drop'})

    for itm in elements:
        id = itm['id']
        price = itm.find('span', {'class': 'product-price__sum-rubles'}).text.replace(' ', '')
        name = itm.find('span', {'class': 'product-card-name__text'}).text.split(',')[0].strip()
        url_part = itm.find('a')['href']
        url = f'https://online.metro-cc.ru{url_part}'

        print(f'{id} | {name} | {url} | {price}')

        # количество товаров для персинга
        if len(result_dict) < 100:
            result_dict[id] = [name, url, price]
        else:
            return


async def get_brend(r_dict):
    """
    :param r_dict: словарь товаров
    :return: ждем завершения задачи
    """
    # создаем объект сессии
    session = AsyncHTMLSession()
    # ассинхронно распределяем задачи
    tasks = (pars_data_brend(session, value[1], key) for key, value in r_dict.items())
    return await asyncio.gather(*tasks)


async def get_goods():
    """
    :return: ждем завершения задачи
    """
    # создаем объект сессии
    session = AsyncHTMLSession()
    # ассинхронно распределяем задачи
    tasks = (pars_page_goods(session, url) for url in page_urls)
    return await asyncio.gather(*tasks)


if __name__ == "__main__":
    # парсим 100 товаров, заполняем словарь результатов
    tasks_1 = asyncio.run(get_goods())
    # проходимся по каждому товару и собираем бренд
    tasks_2 = asyncio.run(get_brend(result_dict))

    # записываем результаты в .csv файл
    with open("result.csv", mode="w", encoding='utf-8') as file:
        file_writer = csv.writer(file, delimiter=";", lineterminator="\r")
        file_writer.writerow(["ID", "Наименование", "Ссылка на товар", "Регулярная цена", "Бренд"])
        for key, value in result_dict.items():
            value.insert(0, key)
            file_writer.writerow(value)

    print('pars finished!')
