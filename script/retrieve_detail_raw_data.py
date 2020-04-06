from functools import reduce
import os

import numpy as np
import pandas as pd

from scraping_details import pool_property_details

if __name__ == '__main__':

    basepath = '/home/cesar/software/houses-project/data/raw/'
    input_file = 'raw_home_for_sale_dataset_2019-09-20.csv'
    output_file_base = 'raw_details_home_for_sale_dataset_2019-09-20'
    tmp_path = basepath + 'details_raw/'

    csv_path = basepath + input_file
    output_file_tmp = tmp_path + '{name}_{idx}.csv'

    if not os.path.exists(tmp_path):
        os.mkdir(tmp_path)

    df = pd.read_csv(csv_path)

    for idx, subset in enumerate(np.array_split(df.index.values, 100)):
        output_file = output_file_tmp.format(name=output_file_base, idx=idx)
        pool_property_details(df.loc[subset, 'url'].values, output_file)

    dfs = [pd.read_csv(tmp_path+key) for key in os.listdir(tmp_path)]
    output = reduce(lambda x, y: pd.concat([x, y], sort=True), dfs)
    output.to_csv(basepath + output_file_base + '.csv', index=False)
