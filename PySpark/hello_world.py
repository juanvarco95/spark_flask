from pyspark.sql import SparkSession

def main():
    spark = SparkSession.builder \
        .appName('Hello-world') \
        .master('local[*]') \
        .config("spark.local.dir", "C:/tmp/spark/") \
        .getOrCreate()
        
    data = [('Juan', 1), ('Miguel', 2)]
    df = spark.createDataFrame(data, ['name', 'id'])
    df.show(vertical = True)
    
    spark.stop()

if __name__ == '__main__':
    main()