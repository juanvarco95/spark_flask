from pyspark.sql import SparkSession
from .config.settings import Settings

def _create_spark_session():
    builder = SparkSession.builder \
        .appName(Settings.SPARK_APP_NAME) \
        .master(Settings.SPARK_MASTER) \
        .config('spark.ui.enabled', Settings.SPARK_UI_ENABLED) \
        .config('spark.driver.memory', Settings.SPARK_DRIVER_MEMORY) \
        .config('spark.local.dir', Settings.SPARK_LOCAL_DIR)
    
    spark = builder.getOrCreate()
    return spark

spark = _create_spark_session()

def stop_spark():
    try:
        spark.stop()
    except Exception:
        ...