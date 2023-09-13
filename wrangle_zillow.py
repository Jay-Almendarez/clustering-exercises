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
        query = 'SELECT DISTINCT predictions_2017.id, typeconstructiontypeid, storytypeid, propertylandusetypeid, heatingorsystemtypeid, buildingclasstypeid,  architecturalstyletypeid, airconditioningtypeid, basementsqft,  bathroomcnt, bedroomcnt, buildingqualitytypeid, calculatedbathnbr,  decktypeid, finishedfloor1squarefeet, calculatedfinishedsquarefeet,  finishedsquarefeet12, finishedsquarefeet13, finishedsquarefeet15,  finishedsquarefeet50, finishedsquarefeet6, fips, fireplacecnt,  fullbathcnt, garagecarcnt, garagetotalsqft, hashottuborspa, latitude,  longitude, lotsizesquarefeet, poolcnt, poolsizesum, pooltypeid10,  pooltypeid2, pooltypeid7, propertycountylandusecode, propertyzoningdesc,  rawcensustractandblock, regionidcity, regionidcounty, regionidneighborhood,  regionidzip, roomcnt, threequarterbathnbr, unitcnt, yardbuildingsqft17,  yardbuildingsqft26, yearbuilt, numberofstories, fireplaceflag,  structuretaxvaluedollarcnt, taxvaluedollarcnt, assessmentyear, landtaxvaluedollarcnt,  taxamount, taxdelinquencyflag, taxdelinquencyyear, censustractandblock,  airconditioningdesc, architecturalstyledesc, buildingclassdesc,  heatingorsystemdesc, propertylandusedesc, storydesc, typeconstructiondesc,  logerror, parcelid FROM predictions_2017 LEFT JOIN properties_2017 USING (parcelid) LEFT JOIN airconditioningtype USING (airconditioningtypeid) LEFT JOIN architecturalstyletype USING (architecturalstyletypeid) LEFT JOIN buildingclasstype USING (buildingclasstypeid) LEFT JOIN heatingorsystemtype USING (heatingorsystemtypeid) LEFT JOIN propertylandusetype USING (propertylandusetypeid) LEFT JOIN storytype USING (storytypeid) LEFT JOIN typeconstructiontype USING (typeconstructiontypeid) LEFT JOIN unique_properties USING (parcelid) WHERE transactiondate BETWEEN "2017-01-01" AND "2017-12-31" AND latitude NOT LIKE "null" AND longitude NOT LIKE "null"'
        connection = get_connection('zillow')
        df = pd.read_sql(query, connection)
        df.to_csv('zillow_cluster.csv', index=False)
        return df
    
    
def null_table(df):
    '''
    null_table will take in a dataframe and create a new dataframe showing total null values for each column and the percentage of null values to the total column size.
    '''
    null_df = pd.DataFrame(data=[{'column_name':df.columns, 'num_rows_missing':df.isnull().sum(), 'pct_rows_missing':round(((df.isnull().sum())/df.shape[0])* 100,2)}])
    k = 0
    for col in df.columns:
        null_df.loc[k] = [df.columns[k], df.isnull().sum()[k], round(((df.isnull().sum()[k])/df.shape[0])* 100,2)]
        k += 1
    return null_df


def handle_missing_values(df, prop_required_column, prop_required_row):
    '''
    handle_missing_values will take the dataframe, the threshold for our null values in our columns in the form of a float between 0 and 1,
    and the threshold for our null values in our rows in the form of a float between 0 and 1
    return: cleaned dateframe with dropped null values above threshold desired.
    '''
    # iterate through every column name in df:
    for col in df.columns:
        # check the ratio of missing values:
        if df[col].isnull().sum()/df.shape[0] > prop_required_column:
            # drop this specific column if its not up to snuff
            df = df.drop(columns=col)
    # for every index and row in the dataframe:
    for i, row in df.iterrows():
        # if the row null cells ratio do not meet what you want:
        if (row.isnull().sum()/df.shape[1]) > prop_required_row:
            # use the index i to drop that specific row
            df = df.drop(index=i)
    return df