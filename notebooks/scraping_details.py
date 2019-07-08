from functools import reduce
import logging
from multiprocessing import cpu_count
from multiprocessing import Pool

from bs4 import BeautifulSoup
import urllib
import pandas as pd
import numpy as np

NCORES = cpu_count()
logging.basicConfig(format='%(asctime)s - %(message)s', level=logging.DEBUG)


def property_details(url_page):
    logging.debug('%s', url_page)
    try:
        page = urllib.request.urlopen(url_page)
    except urllib.error.HTTPError as e:
        print('HTTPError: {}'.format(e.code))
        return pd.Series({'uri': url_page})
    except urllib.error.URLError as e:
        print('URLError: {}'.format(e.reason))
        return pd.Series({'uri': url_page})
    soup = BeautifulSoup(page, 'html.parser')
    ficha_tecnica = soup.find_all(class_='ficha-tecnica')
    amenities = soup.find_all(id='amenities')
    description = soup.find(id='descripcion')
    agency = soup.find('p', class_='titulo-inmobiliaria')
    price = soup.find('p', class_='precio-final')
    title = soup.find('h1', class_='likeh2 titulo one-line-txt')
    kind = soup.find('p', class_='venta')
    # visitation = soup.find_all(class_='allContentVisitation')

    details = {item.find('p').get_text()[:-1].replace(' ', '_'): item.find('div').get_text()
               for item in ficha_tecnica[0].find_all(class_='lista')} if ficha_tecnica else {}
    details['extra'] = ','.join(
        [key.find('p').get_text() for key in amenities[0].find_all(class_='lista active')]) if amenities else ''
    details['description'] = '. '.join([p.get_text() for p in description.find_all('p')]) if description else ''
    details['uri'] = url_page
    details['agency'] = agency.get_text() if agency else ''
    details['price'] = price.get_text() if price else ''
    details['title'] = title.get_text() if title else ''
    details['kind'] = kind.get_text() if kind else ''

    return pd.Series(details)


def get_with_pool(urls, outputfile):
    # result = []

    with Pool(NCORES) as p:
        result = [p.apply_async(property_details, (url, )) for url in urls]
        # result = p.map(property_details, urls)
        result = [k.get() for k in result]

    df_result = reduce(lambda x, y: pd.concat([x, y], axis=1, sort=True), result).transpose().reset_index(drop=True)
    df_result.to_csv(outputfile, index=False)


if __name__ == '__main__':

    csv_path = '/home/cesar/software/houses-project/data/dataset_houses_2019-07-04_14_33_34.018491.csv'
    output_file = '/home/cesar/software/houses-project/data/details_houses_2019-07-04_14:33:34.018491_{}.csv'
    df = pd.read_csv(csv_path)

    for idx, subset in enumerate(np.array_split(df.index.values, 1000)[:2]):
        get_with_pool(df.loc[subset, 'uris'].values, output_file.format(idx))
