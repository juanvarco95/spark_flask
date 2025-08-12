import os 

class Settings:
    BASE_DIR = os.path.abspath(os.path.dirname(os.path.dirname(__file__)))
    UPLOAD_FOLDER = os.environ.get('UPLOAD_FOLDER', os.path.join(BASE_DIR, '..', 'data', 'raw'))
    
    SPARK_APP_NAME = os.environ.get('SPARK_APP_NAME', 'FlaskPySparkApp')
    SPARK_MASTER = os.environ.get('SPARK_MASTER', 'local[*]')
    SPARK_UI_ENABLED = os.environ.get('SPARK_UI_ENABLED', 'false')
    SPARK_DRIVER_MEMORY = os.environ.get('SPARK_DRIVER_MEMORY', '1g')
    SPARK_LOCAL_DIR = os.environ.get('SPARK_LOCAL_DIR', 'C:/tmp/spark')
    
    SECRET_KEY = os.environ.get('SECRET_KEY', 'dev-secret')
    
settings = Settings()