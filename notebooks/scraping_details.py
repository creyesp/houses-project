from functools import reduce
from multiprocessing import cpu_count
from multiprocessing import Pool

from bs4 import BeautifulSoup
import urllib.request
import pandas as pd

NCORES = cpu_count()

def property_details(url_page):
    try:
        print(url_page)
        page = urllib.request.urlopen(url_page)
        soup = BeautifulSoup(page, 'html.parser')
        ficha_tecnica = soup.find_all(class_='ficha-tecnica')
        amenities = soup.find_all(id='amenities')
        description = soup.find_all(id='descripcion')
        agency = soup.find('p', class_='titulo-inmobiliaria')
        price = soup.find('p', class_='precio-final')
        title = soup.find('h1', class_='likeh2 titulo one-line-txt')
        kind = soup.find('p', class_='venta')
        # visitation = soup.find_all(class_='allContentVisitation')

        details = {item.find('p').get_text()[:-1].replace(' ', '_'): item.find('div').get_text()
                   for item in ficha_tecnica[0].find_all(class_='lista')} if ficha_tecnica else {}
        details['extra'] = ','.join(
            [key.find('p').get_text() for key in amenities[0].find_all(class_='lista active')]) if amenities else ''
        details['description'] = description[0].find('p').get_text() if description else ''
        details['uri'] = url_page
        details['agency'] = agency.get_text() if agency else ''
        details['price'] = price.get_text() if price else ''
        details['title'] = title.get_text() if title else ''
        details['kind'] = kind.get_text() if kind else ''
    except ValueError:
        details = []
        print('ERROR: {}'.format(url_page))

    return pd.Series(details)


if __name__ == '__main__':

    csv_path = '../data/dataset_houses_2019-07-04 14:33:34.018491.csv'
    output_file = '../data/details_houses_2019-07-04 14:33:34.018491.csv'
    df = pd.read_csv('../data/dataset_houses_2019-07-04 14:33:34.018491.csv')

    result = []

    with Pool(NCORES) as p:
        result = p.map(property_details, df.loc[:, 'uris'].values)

    df_result = reduce(lambda x, y: pd.concat([x, y], axis=1, sort=True), result).transpose().reset_index(drop=True)
    df_result.to_csv(output_file, index=False)
