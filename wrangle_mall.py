import pandas as pd
import numpy as np
import env
import os
from sklearn.model_selection import train_test_split

def get_connection(db, user=env.user, host=env.host, password=env.password):
    '''
    get_connection will determine the database we are wanting to access, and load the database along with env stored values like username, password, and host
    to create the url needed for SQL to read the correct database.
    '''
    return f'mysql+pymysql://{user}:{password}@{host}/{db}'

def acquire_mall():
    file_name = 'mall.csv'
    if os.path.isfile(file_name):
        return pd.read_csv(file_name)
    else:
        query = 'SELECT * FROM customers'
        connection = get_connection('mall_customers')
        df = pd.read_sql(query, connection)
        df.to_csv('mall.csv', index=False)
        return df
    
    
def get_upper_outliers(series, k):
    '''
    Given a series and a cutoff value, k, returns the upper outliers for the
    series.

    The values returned will be either 0 (if the point is not an outlier), or a
    number that indicates how far away from the upper bound the observation is.
    '''
    q1, q3 = s.quantile([.25, .75])
    iqr = q3 - q1
    upper_bound = q3 + k * iqr
    return s.apply(lambda x: max([x - upper_bound, 0]))

def add_upper_outlier_columns(df, k):
    '''
    Add a column with the suffix _outliers for all the numeric columns
    in the given dataframe.
    '''
    for col in df.select_dtypes('number'):
        df[col + '_outliers'] = get_upper_outliers(df[col], k)

    return df