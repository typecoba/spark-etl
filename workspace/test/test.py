from pyspark.sql import SparkSession

if __name__=='__main__':
    # spark session
    spark = SparkSession.builder \
            .appName('test-session') \
            .master('local[*]') \
            .getOrCreate()
    
    configs = spark.sparkContext.getConf().getAll()
    for config in configs :
        print(config)