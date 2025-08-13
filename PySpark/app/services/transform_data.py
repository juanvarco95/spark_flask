import os
from ..spark_app import spark
from pyspark.sql.functions import col
from csv_service import read_csv_to_spark_df

def add_column(column_name):
    path = os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))), 'data', 'raw', 'test_1.csv')
    print(f'Path: {path}')
    df = read_csv_to_spark_df(path)