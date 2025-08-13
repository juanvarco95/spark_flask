import os
import uuid
import pandas as pd
from ..spark_app import spark
from abc import ABC, abstractmethod


# def save_uploaded_file(file_storage, upload_folder, filename = None):
#     if filename is None:
#         filename = f'{uuid.uuid4().hex}.csv'
        
#     dest_path = os.path.join(upload_folder, filename)
#     file_storage.save(dest_path)
    
#     return dest_path

class UploadFile(ABC):
    def __init__(self, 
                file_storage, 
                upload_folder,
                filename = None):
        self.file_storage = file_storage
        self.upload_folder = upload_folder
        self.filename = filename
    
    @abstractmethod 
    def work():
        ...

class SaveUploadFile(UploadFile):
    def __init__(self,
                file_storage, 
                upload_folder,
                filename = None):
        
        super().__init__(file_storage, upload_folder, filename)
        
    def save_upload_file(self):
        if self.filename is None:
            self.filename = f'{uuid.uuid4().hex}.csv'
        
        dest_path = os.path.join(self.upload_folder, self.filename)
        self.file_storage.save(dest_path)
    
        return dest_path
    
    def work(self):
        dest_path = self.save_upload_file()
        
        return dest_path

class LoadFileSpark(ABC):
    def __init__(self, file_path):
        self.file_path = file_path
    
    @abstractmethod
    def work(self):
        ...
    
class LoadFileSparkCSV(LoadFileSpark):
    def __init__(self, file_path, header, inferschema, sep):
        super().__init__(file_path)
        self.header = header
        self.inferschema = inferschema
        self.sep = sep
        
    def load_file_csv(self):
        df_csv = spark.read.csv(path = self.file_path, 
                                header = self.header, 
                                inferSchema = self.inferschema,
                                sep = self.sep)

        return df_csv
        
    def work(self):
        df_csv = self.load_file_csv()
        
        return df_csv
    
    

# def read_csv_to_spark_df(path, header = True, infer_schema = True):
#     df = spark.read.csv(path, 
#                         header = header, 
#                         inferSchema = infer_schema)

#     return df


class DfTo(ABC):
    def __init__(self, spark_df, max_rows):
        self.spark_df = spark_df
        self.max_rows = max_rows
    
    @abstractmethod
    def work(self):
        ...
        
class DfToHTML(DfTo):
    def __init__(self, spark_df, max_rows):
        super().__init__(spark_df, max_rows = 1000)

    def df_to_html(self):
        pd_df = self.spark_df.limit(self.max_rows).toPandas()
        
        return pd_df.to_html(index = False,
                            classes = ['table'],
                            border = 0)
    
    def work(self):
        pd_df = self.df_to_html()
        
        return pd_df
    
# def df_to_html(spark_df, max_rows = 1000):
#     pd_df = spark_df.limit(max_rows).toPandas()
    
#     return pd_df.to_html(index = False,
#                         classes = ['table'],
#                         border = 0)