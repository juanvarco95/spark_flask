from flask import Flask, jsonify
from pyspark.sql import SparkSession
import atexit

app = Flask(__name__)

spark = SparkSession.builder \
    .appName('pyspark-flask-app') \
    .master('local[*]') \
    .config('spark.ui.enable', 'false') \
    .config('spark.driver.memory', '1g') \
    .getOrCreate()
    
@atexit.register
def shutdown_spark():
    try:
        spark.stop()
    except Exception:
        pass
    
@app.route('/')
def index():
    return 'Hello world in spark' 
    
@app.route('/api/df')
def df_endpint():
    data = [('Juan', 1), ('Miguel', 2)]
    df = spark.createDataFrame(data, ['name', 'id'])
    result = [row.asDict() for row in df.collect()]
    
    return jsonify(result)
    
    
@app.route('/api/wordcount')
def wordcount():
    ...
    
if __name__ == '__main__':
    app.run(debug = True,
            host = '0.0.0.0',
            port = 5000)