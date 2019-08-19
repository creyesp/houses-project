from unidecode import unidecode

import pandas as pd

YES_NO_MAP = {'si': 1, 'no': 0}


def clean_dataset(src_file, dst_file):
    df = pd.read_csv(src_file)
    numerical = df.select_dtypes(exclude='object')
    strings = (df.select_dtypes(include='object')
               .fillna('')
               .apply(lambda x: x.str.strip())
               )
    string_mask = strings.apply(lambda x: x.str.match('(^[Pregúntale!]+$)'))
    strings[string_mask] = ''

    df = pd.concat([numerical, strings], axis=1, sort=False)
    df = df.loc[:, df.any()]
    df.columns = [unidecode(k).lower().replace('.','') for k in df.columns]
    df_clean = df.copy()

    # Boolean features
    bool_features = ['acepta_permuta', 'financia', 'oficina', 'penthouse',
                     'vista_al_mar', 'vivienda_social', ]

    for key in bool_features:
        df_clean.loc[:, key] = df.loc[:, key].str.lower().map(YES_NO_MAP)

    # Numeric features
    df_clean.loc[:, 'ambientes'] = df.loc[:, 'ambientes'].str.extract('(\d)\.?')[0]
    df_clean.loc[:, 'ambientes_extra'] = df.loc[:, 'ambientes'].str.match('(\d\.?\+)')

    df_clean.loc[:, 'banos'] = df.loc[:, 'banos'].str.extract('(\d)\.?')[0]
    df_clean.loc[:, 'banos_extra'] = df.loc[:, 'banos'].str.match('(\d\.?\+)')

    df_clean.loc[:, 'garajes'] = df.loc[:, 'garajes'].str.extract('([0-9]+)')[0]
    df_clean.loc[:, 'garajes_extra'] = df.loc[:, 'garajes'].str.match('(\d\+)')

    df_clean.loc[:, 'ano_de_construccion'] = df.loc[:, 'ano_de_construccion'].str.extract('(\d{4})')[0]

    rooms = df.loc[:, 'dormitorios'].str.extract('(\d)')[0]
    rooms_mask = df.loc[:, 'dormitorios'].str.lower().str.match('monoambiente')
    rooms[rooms_mask] = '0'
    df_clean.loc[:, 'dormitorios'] = rooms
    df_clean.loc[:, 'dormitorios_extra'] = df.loc[:, 'dormitorios'].str.match('(\d\+)')

    floor = df.loc[:, 'piso'].str.extract(("[0-9]+)\.?"))[0]
    floor_mask = df.loc[:, 'piso'].str.lower().str.match('planta baja')
    floor[floor_mask] = '0'
    df_clean.loc[:, 'piso'] = floor

    df_clean.loc[:, 'plantas'] = df.loc[:, 'plantas'].str.extract('([0-9]+)\.?')[0]
    df_clean.loc[:, 'plantas_extra'] = df.loc[:, 'plantas'].str.lower().str.contains('más')

    df_clean.loc[:, 'precio'] = df.loc[:, 'precio'].str.extract('([0-9\.]+)')[0].str.replace('.','')
    df_clean.loc[:, 'precio_moneda'] = df.loc[:, 'precio'].str.extract('([\$|UI|U\$S]+)')

    df_clean.loc[:, 'gastos_comunes'] = df.loc[:, 'gastos_comunes'].str.extract('([0-9\.]+)')[0].str.replace('.','')
    df_clean.loc[:, 'gastos_comunes_moneda'] = df.loc[:, 'gastos_comunes'].str.extract('([\$|UI|U\$S]+)')

    # FIXME: regex make error when number is 100.0 only work with this format 1.000.000
    dist_mar = df.loc[:, 'distancia_al_mar'].str.extract('([0-9\.?]+)', ).apply(lambda x: x.str.replace('.', ''))
    dist_mar_mask = df.loc[:, 'distancia_al_mar'].str.lower().str.match('frente')
    dist_mar[dist_mar_mask] = '0'
    df_clean.loc[:, 'distancia_al_mar'] = dist_mar[0]

    # String number to float
    to_numeric = ['ambientes', 'banos', 'garajes', 'ano_de_construccion',
                  'distancia_al_mar', 'dormitorios', 'piso', 'plantas',
                  'precio', 'gastos_comunes', ]
    df_to_numeric = df_clean[to_numeric]
    df_clean.loc[:, to_numeric] = df_to_numeric.astype(float)

    sorted_col = df_clean.columns.sort_values()
    df_clean[sorted_col].to_csv(output_file, index=False)
    cat_trans = (df
                 .drop(columns=['url', 'referencia'])
                 .select_dtypes(include='O')
                 .fillna('')
                 .apply(lambda x: x.str.lower().apply(lambda y: unidecode(y)))
                 )
    cat_trans[cat_trans == ''] = None
    df_clean.loc[:, cat_trans.columns] = cat_trans

    return df_clean[sorted_col]


if __name__ == '__main__':
    scr_file = '../data/raw/raw_details_home_for_sale_dataset_2019-07-25.csv'
    dst_file = '../data/preprocessed/details_home_for_sale_dataset_2019-07-25.csv'

    clean_dataset(scr_file, dst_file)
