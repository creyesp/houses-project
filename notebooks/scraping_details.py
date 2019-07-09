from functools import reduce
import logging
from multiprocessing import cpu_count
from multiprocessing import Pool
import os

from bs4 import BeautifulSoup
import urllib
import pandas as pd
import numpy as np

NCORES = cpu_count()
logging.basicConfig(format='%(asctime)s - %(message)s', level=logging.DEBUG)


def retrive_general_data(url_page):
    logging.debug('%s', url_page)

    try:
        page = urllib.request.urlopen(url_page)
    except urllib.error.HTTPError as e:
        print('HTTPError: {}'.format(e.code))
        return pd.DataFrame([])
    except urllib.error.URLError as e:
        print('URLError: {}'.format(e.reason))
        return pd.DataFrame([])

    soup = BeautifulSoup(page, 'html.parser')
    table = soup.find_all('div', attrs={'class': 'propiedades-slider'})
    neighborhood = [
        [k.text for k in p.find_all('p')] for t in table \
        for p in t.find_all('div') \
        if 'singleLineDots' in p['class']
    ]
    price = [p.text.split()[-1] for t in table \
             for p in t.find_all('div') if 'precio' in p['class']]
    desc = [[k.text for k in p.find_all('p')] for t in table \
            for p in t.find_all('div') if
            'inDescription' in p['class']]
    desc = [k[0] for k in desc]
    details = [[d.find_all('span')[0].text for d in p.find_all('div')] \
               for t in table for p in t.find_all('div') \
               if 'contentIcons' in p['class']]
    details = pd.DataFrame(details,
                           columns=['rooms', 'bathrooms', 'area_m2'])
    data_id = [k.get('data-id', '') for k in table]
    data_idproject = [k.get('data-idproyecto', '') for k in table]
    link = [url_base + k.find('a')['href'] for k in table]
    proyecto_label = [
        k.find(class_='proyectoLabel').get_text() if k.find(
            class_='proyectoLabel') else None for k in table]

    df = pd.DataFrame(neighborhood, columns=['neighborhood', 'type'])
    df['price'] = price
    df['desc'] = desc
    df['uris'] = link
    df['id'] = data_id
    df['idproject'] = data_idproject
    df['project_label'] = proyecto_label
    df['page'] = url_page
    df = pd.concat([details, df], axis=1)

    return df

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

    basepath = '/home/cesar/software/houses-project/data/'
    input_file = 'raw_rent_house_dataset_2019-07-09 11:24:55.766324.csv'
    output_file_base = 'details_rent_house_dataset_2019-07-09 11:24:55.766324'
    tmp_path = basepath + 'datails/'

    csv_path = basepath + input_file
    output_file_tmp = tmp_path + '{name}_{idx}.csv'

    if not os.path.exists(tmp_path):
        os.mkdir(tmp_path)

    df = pd.read_csv(csv_path)

    for idx, subset in enumerate(np.array_split(df.index.values, 1000)[:2]):
        output_file = output_file_tmp.format(name=output_file_base, idx=idx)
        get_with_pool(df.loc[subset, 'uris'].values, output_file)

    dfs = [pd.read_csv(tmp_path+key) for key in os.listdir(tmp_path) if output_file_base in key]
    output = reduce(lambda x, y: pd.concat([x, y], sort=True), dfs)
    output.to_csv(basepath+'test.csv', index=False)