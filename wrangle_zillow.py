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

def acquire_zillow():
    file_name = 'zillow_cluster.csv'
    if os.path.isfile(file_name):
        return pd.read_csv(file_name)
    else:
        query = 'SELECT * FROM properties_2017 LEFT JOIN airconditioningtype USING (airconditioningtypeid) LEFT JOIN architecturalstyletype USING (architecturalstyletypeid) LEFT JOIN buildingclasstype USING (buildingclasstypeid) LEFT JOIN heatingorsystemtype USING (heatingorsystemtypeid) LEFT JOIN propertylandusetype USING (propertylandusetypeid) LEFT JOIN storytype USING (storytypeid) LEFT JOIN typeconstructiontype USING (typeconstructiontypeid) LEFT JOIN unique_properties USING (parcelid) LEFT JOIN predictions_2017 USING (parcelid) WHERE transactiondate BETWEEN "2017-01-01" AND "2017-12-31" AND latitude NOT LIKE "null" AND longitude NOT LIKE "null"'
        connection = get_connection('zillow')
        df = pd.read_sql(query, connection)
        df.to_csv('zillow_cluster.csv', index=False)
        return df