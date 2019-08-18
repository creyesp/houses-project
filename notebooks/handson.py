import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
from sklearn.metrics import mean_absolute_error
from sklearn.metrics import mean_squared_error
from sklearn.metrics import median_absolute_error
from sklearn.metrics import r2_score


def info(data):
    """Get info about dataset.

    Create a resume about missing values, uniques, and dtypes.

    Parameters
    ------
    data: pandas.DataFrame
        dataset to get info

    Returns
    -------
    info: pandas.DataFrame

    """
    desc = {
        'dtypes': data.dtypes,
        'isnull': data.isnull().any(),
        '%null': data.isnull().sum()/data.shape[0]*100,
        'nunique': data.nunique()
    }
    return pd.DataFrame(desc)


def pareto_rule(data, default='otros'):
    """Implementation of Pareto rule to categorical variable.

    "80% of outputs are produced by 20% of inputs"

    Parameters
    ---------
    data: pandas.Series
        categorical variable to map Pareto rule
    default: str
        value to group tail values of data.

    Returns
    ------
    mapped: pandas.Series

    """
    zn_dominante = data.value_counts().cumsum() / data.shape[0]
    zn_select = zn_dominante[zn_dominante < 0.8].index.tolist()
    map_barrio = dict(zip(zn_select, zn_select))
    mapped = data.map(lambda x: map_barrio.get(x, default))

    return mapped


def plot_predict_result(y_test, y_pred, **kwargs):
    if 'figsize' not in kwargs:
        kwargs['figsize'] = (8, 8)

    f, ax = plt.subplots(2, 2, **kwargs)
    ax = ax.flatten()
    ax[0].scatter(y_test, y_pred, alpha=0.4)
    ax[0].plot(ax[0].get_xbound(), ax[0].get_xbound(), '--k')
    ax[0].set(ylabel='Target predicted',
              xlabel='True Target',
              title='test v/s predict price')
    ax[1].scatter(y_test, y_test - y_pred, alpha=0.4)
    ax[1].set(ylabel='residual',
              xlabel='test price',
              title='Residual')
    ax[2].hist(y_test - y_pred, bins=100)
    ax[2].set(ylabel='frequency',
              xlabel='residual',
              title='Residual Distribution')

    return f, ax


def get_scores(test_train, test_predict):
    mse = mean_squared_error(test_train, test_predict)
    mea = mean_absolute_error(test_train, test_predict)
    meae = median_absolute_error(test_train, test_predict)
    r2 = r2_score(test_train, test_predict)

    return r'MSE={:.2f}, MAE={:.2f}, MEAE={:.2f}, $R^2$={:.2f}'.format(mse,
                                                               mea,
                                                               meae,
                                                               r2)