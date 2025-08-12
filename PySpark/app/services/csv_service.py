import os
import uuid
import pandas as pd
from ..spark_app import spark

def save_uploaded_file(file_storage, upload_folder, filename = None):
    if filename is None:
        filename = f'{uuid.uuid4().hex}.csv'
        
    dest_path = os.path.join(upload_folder, filename)
    file_storage.save(dest_path)
    
    return dest_path

def read_csv_to_spark_df(path, header = True, infer_schema = True):
    df = spark.read.csv(path, 
                        header = header, 
                        inferSchema = infer_schema)

    return df

def df_to_html(spark_df, max_rows = 1000):
    pd_df = spark_df.limit(max_rows).toPandas()
    
    return pd_df.to_html(index = False,
                        classes = ['table'],
                        border = 0)