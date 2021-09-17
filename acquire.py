import numpy as np
import pandas as pd
import env

# get connection to server
def get_connection(db, user=env.user, host=env.host, password=env.password):
    return f'mysql+pymysql://{user}:{password}@{host}/{db}'
    
def get_zillow_data():
    df = pd.read_sql("""SELECT *
                        FROM properties_2017
                        LEFT JOIN airconditioningtype 
                        ON airconditioningtype.airconditioningtypeid = properties_2017.airconditioningtypeid
                        LEFT JOIN architecturalstyletype
                        ON architecturalstyletype.architecturalstyletypeid = properties_2017.architecturalstyletypeid
                        LEFT JOIN buildingclasstype 
                        ON buildingclasstype.buildingclasstypeid = properties_2017.buildingclasstypeid
                        LEFT JOIN heatingorsystemtype
                        ON heatingorsystemtype.heatingorsystemtypeid = properties_2017.heatingorsystemtypeid
                        LEFT JOIN predictions_2017
                        ON predictions_2017.id = properties_2017.id
                        INNER JOIN (SELECT id, MAX(transactiondate) as last_trans_date 
                                    FROM predictions_2017
                                    GROUP BY id) predictions 
                        ON predictions.id = properties_2017.id 
                        AND predictions_2017.transactiondate = predictions.last_trans_date
                        LEFT JOIN propertylandusetype
                        ON propertylandusetype.propertylandusetypeid = properties_2017.propertylandusetypeid
                        LEFT JOIN storytype
                        ON storytype.storytypeid = properties_2017.storytypeid
                        LEFT JOIN typeconstructiontype
                        ON typeconstructiontype.typeconstructiontypeid = properties_2017.typeconstructiontypeid
                        JOIN unique_properties
                        ON unique_properties.parcelid = properties_2017.parcelid
                        WHERE latitude IS NOT NULL and longitude IS NOT NULL;""", get_connection('zillow'))
    df = df.loc[:,~df.columns.duplicated()]
    df.drop(df.tail(1).index,inplace = True)
    return df