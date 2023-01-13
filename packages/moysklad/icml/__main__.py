import json
import xml.etree.ElementTree as ET
from pydantic import BaseModel
from datetime import datetime
import asyncio
import httpx
import math
import logging
import os

login = os.getenv('MS_LOGIN')
password = os.getenv('MS_PASSWORD')
headers = {'Content-Type': 'application/json'}
auth = (login, password)

moysklad_semaphore = asyncio.Semaphore(5)

FORMAT = '%(asctime)s | %(levelname)s | %(message)s'
logging.basicConfig(level='INFO', format=FORMAT, filename='logs.log')
logger = logging.getLogger()


class Category(BaseModel):
    category_id: str
    category_name: str
    parent_category: str | None = None
    meta: dict | None = None


class Offer(BaseModel):
    offer_id: str
    product_id: str
    url: str | None = None
    price: float
    category_id: str | None = None
    picture: str | None = None
    name: str
    xmlid: str | None = None
    article: str | None = None
    description: str | None = None
    vendor: str | None = None
    meta: dict | None = None


async def create_batch_in_ms(client: httpx.AsyncClient, url: str, offers: str, start: int, end: int):
    async with moysklad_semaphore:
        response = await client.post(url, headers=headers, auth=auth, data=offers, timeout=600)
        if response.status_code != 200:
            logger.error(response.text)


async def create_products_in_ms(offers: list):
    url = 'https://online.moysklad.ru/api/remap/1.2/entity/product'
    logger.info('Offer loading starting...')
    async with httpx.AsyncClient() as client:
        tasks = []
        start = 0
        end = 1000
        total_end = len(offers)
        number_of_tasks = math.ceil(total_end / 1000)
        for number in range(0, number_of_tasks):
            tasks.append(
                asyncio.ensure_future(create_batch_in_ms(client, url, json.dumps(offers[start:end]), start, end)))
            start += 1000
            end += 1000
            if end > total_end:
                end = total_end
        await asyncio.gather(*tasks)
    logger.info(f'Offers loaded: {total_end}')


async def create_lists_from_xml():
    category_dict = {}
    offer_dict = {}
    async with httpx.AsyncClient() as client:
        r = await client.get('https://mobistore.by/integration/icml/icml.xml')
        xml = r.text
    tree = ET.fromstring(xml)
    for child in tree.iter('category'):
        category_dict[child.attrib.get('id')] = Category(category_id=child.attrib.get('id'),
                                                         category_name=child.text,
                                                         # .encode("iso-8859-1").decode("utf-8"),
                                                         parent_category=child.attrib.get('parentId'))

    for child in tree.iter('offer'):
        if child.find('picture') is not None:
            picture = child.find('picture').text
        else:
            picture = None
        if child.find('xmlId') is not None:
            xmlid = child.find('xmlId').text
        else:
            xmlid = None
        if child.find('article') is not None:
            article = child.find('article').text
        else:
            article = None
        if child.find('description') is not None:
            description = child.find('description').text
        else:
            description = None

        if child.find('url') is not None:
            url = child.find('url').text
        else:
            url = None

        if child.find('categoryId') is not None:
            category_id = child.find('categoryId').text
        else:
            category_id = None

        if child.find('vendor') is not None:
            vendor = child.find('vendor').text
        else:
            vendor = None

        offer_dict[xmlid] = Offer(offer_id=child.attrib['id'],
                                  product_id=child.attrib['productId'],
                                  url=url,
                                  price=float(child.find('price').text),
                                  category_id=category_id,
                                  picture=picture,
                                  name=child.find('name').text,  # .encode("iso-8859-1").decode("utf-8"),
                                  xmlid=xmlid,
                                  article=article,
                                  description=description,
                                  vendor=vendor)
    return offer_dict


async def ms_async_get(client: httpx.AsyncClient, url: str, limit: int, offset: int):
    async with moysklad_semaphore:
        params = {'limit': limit,
                  'offset': offset}
        r = await client.get(url, headers=headers, params=params, auth=auth)
        return r.json()


async def get_all_meta_from_ms(url: str, cats: bool = None):
    all_meta = []
    async with httpx.AsyncClient() as client:
        first_response = await ms_async_get(client, url, limit=1, offset=0)
        number_of_tasks = math.ceil(first_response['meta']['size'] / 1000)
        tasks = []
        offset = 0
        for number in range(0, number_of_tasks):
            tasks.append(asyncio.ensure_future(ms_async_get(client, url, limit=1000, offset=offset)))
            offset += 1000
        all_responses = await asyncio.gather(*tasks)
        for response in all_responses:
            all_meta.extend(response['rows'])
        if cats:
            all_cats = {}
            for category in all_meta:
                all_cats[category['externalCode']] = category
            return all_cats
        else:
            return all_meta


async def create_category(category_id: str):
    url = f'https://online.moysklad.ru/api/remap/1.2/entity/productfolder'
    data = {'name': category_id, 'externalCode': category_id}
    async with httpx.AsyncClient as client:
        response = await client.post(url, headers=headers, auth=auth, data=json.dumps(data))
    return response.json()['meta']


async def combine_arrays():
    start_time = datetime.now()
    offers_from_site = await create_lists_from_xml()
    logger.info('Offers from site loaded')
    offers_from_ms = await get_all_meta_from_ms('https://online.moysklad.ru/api/remap/1.2/entity/product')
    logger.info('Offers from moysklad loaded')
    all_categories = await get_all_meta_from_ms('https://online.moysklad.ru/api/remap/1.2/entity/productfolder',
                                                cats=True)
    logger.info('Categories from moysklad loaded')
    offers_to_load = []
    for ms_offer in offers_from_ms:
        if offers_from_site.get(ms_offer['externalCode']):
            if ms_offer['salePrices'][0]['value'] != round(offers_from_site[ms_offer['externalCode']].price * 100):
                ms_offer['salePrices'][0]['value'] = round(offers_from_site[ms_offer['externalCode']].price * 100)
                offers_to_load.append(ms_offer)
                del offers_from_site[ms_offer['externalCode']]
            else:
                del offers_from_site[ms_offer['externalCode']]

    data = []
    for new_offer in offers_from_site.values():

        product = {'name': new_offer.name,
                   'code': new_offer.xmlid,
                   'externalCode': new_offer.xmlid,
                   "effectiveVat": 20,
                   "effectiveVatEnabled": True,
                   "vat": 20,
                   "vatEnabled": True,
                   "salePrices": [
                       {
                           "value": new_offer.price * 100,
                           "currency": {
                               "meta": {
                                   "href": "https://online.moysklad.ru/api/remap/1.2/entity/currency/c954c2a9-326f-11ec-0a80-07fe000fd4e3",
                                   "metadataHref": "https://online.moysklad.ru/api/remap/1.2/entity/currency/metadata",
                                   "type": "currency",
                                   "mediaType": "application/json",
                                   "uuidHref": "https://online.moysklad.ru/app/#currency/edit?id=c954c2a9-326f-11ec-0a80-07fe000fd4e3"
                               }
                           },
                           "priceType": {
                               "meta": {
                                   "href": "https://online.moysklad.ru/api/remap/1.2/context/companysettings/pricetype/c9561b60-326f-11ec-0a80-07fe000fd4e4",
                                   "type": "pricetype",
                                   "mediaType": "application/json"
                               },
                               "id": "c9561b60-326f-11ec-0a80-07fe000fd4e4",
                               "name": "Цена продажи",
                               "externalCode": "cbcf493b-55bc-11d9-848a-00112f43529a"
                           }
                       }
                   ],
                   "attributes": [
                       {
                           "meta": {
                               "href": "https://online.moysklad.ru/api/remap/1.2/entity/product/metadata/attributes/38d405ce-4bb7-11ed-0a80-04e0000e820c",
                               "type": "attributemetadata",
                               "mediaType": "application/json"
                           },
                           "id": "38d405ce-4bb7-11ed-0a80-04e0000e820c",
                           "name": "Ссылка на сайте",
                           "type": "string",
                           "value": new_offer.url
                       },
                       {
                           "meta": {
                               "href": "https://online.moysklad.ru/api/remap/1.2/entity/product/metadata/attributes/780f347c-4bb7-11ed-0a80-06b3000e0ae7",
                               "type": "attributemetadata",
                               "mediaType": "application/json"
                           },
                           "id": "780f347c-4bb7-11ed-0a80-06b3000e0ae7",
                           "name": "Производитель",
                           "type": "string",
                           "value": new_offer.vendor
                       }
                   ]
                   }
        if new_offer.category_id:
            meta = all_categories.get(new_offer.category_id)['meta']
            if meta:
                product['productFolder'] = {
                    "meta": meta
                }
            else:
                all_categories[new_offer.category_id] = await create_category(new_offer.category_id)
                product['productFolder'] = {
                    "meta": all_categories[new_offer.category_id]['meta']
                }

        data.append(product)
    logger.info('All products processed')
    logger.info(f'Offers to update: {len(offers_to_load)}')
    logger.info(f'Offers to create: {len(data)}')
    offers_to_load.extend(data)
    await create_products_in_ms(offers_to_load)
    total_time = datetime.now() - start_time
    logger.info(f'All script takes: {total_time}')
    return f'Offers proceed: {len(data)}'


def main():
    asyncio.run(combine_arrays())


if __name__ == '__main__':
    main()


# def main(args):
#     asyncio.run(combine_arrays())
#     name = args.get("name", "stranger")
#     greeting = "Hello " + name + "!"
#     print(greeting)
#     return {"body": greeting}
