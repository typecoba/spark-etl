from pyspark.sql import SparkSession
from pyspark import SparkConf

class SessionHelper:
    def get_session():
        spark = SparkSession.builder \
                .appName('pyspark-application') \
                .master('local[*]') \
                .config('spark.eventLog.enabled','true')\
                .getOrCreate()
        spark.conf.set('mapreduce.fileoutputcommitter.marksuccessfuljobs', 'false') # 파일생성시 메타데이터 저장 여부
        spark.conf.set('parquet.enable.summary-metadata', 'false') # 파일생성시 메타데이터 저장 여부
        return spark
    
