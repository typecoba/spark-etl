from common.SessionHelper import SessionHelper
from common.Schema import Schema
import pyspark.sql.functions as F
import os, gc
from datetime import datetime, timedelta
import time

class RunPreprocess:
    warehouse_tier1_path = '/home/data/data-warehouse/tier1/event/'

    def __init__(self):
        pass
        

    def run(self):
        print('==== run')      
        
        # 날짜범위에서 없는 폴더 확인후 작동
        # os.isdir 이슈 
        # os.isdir 함수가 425개 이상 처리할경우 log4j 옵션과 충돌?
        # ... batch job 프로세스 db로 관리해야할듯...
        if len(os.listdir(self.warehouse_tier1_path)) > 5 :
            mtime = lambda f: os.stat(os.path.join(self.warehouse_tier1_path,f)).st_mtime
            startdate = list(sorted(os.listdir(self.warehouse_tier1_path), key=mtime, reverse=True))[3] # 마지막폴더 5번째전... 임시
            print(startdate)
        else:
            startdate = '2020-12-01'
        
        mindate = datetime.strptime(startdate, '%Y-%m-%d')
        maxdate = datetime.strptime('20220131', '%Y%m%d')
        daterange = [mindate + timedelta(hours=x) for x in range(0, (maxdate - mindate).days)]
        
        for date in daterange:
            path = self.warehouse_tier1_path + date.strftime('%Y-%m-%d/%H')                        
            if os.path.isdir(path) == False:
                print(date.strftime('%Y-%m-%d/%H'))
                os.makedirs(path)
                self.process(date)
                break             
        

    def process(self, datetime:datetime):
        print('==== process')
        # spark session 
        spark = SessionHelper.get_session()
        # print(spark)

        # rawdata path
        datepath = datetime.strftime('%Y-%m/%d/%H')
        adbrix_data_path = f'/home/data/dmc-integrated-analytics/rawdata/thirdparty-adbrix/{datepath}'
        branch_data_path = f'/home/data/dmc-integrated-analytics/rawdata/thirdparty-branch/{datepath}'
        singular_data_path = f'/home/data/dmc-integrated-analytics/rawdata/thirdparty-singular/{datepath}'

        # attribute

        

        # rawdata load & transform
        data_format = 'json'
        uniondf = spark.createDataFrame([], schema=Schema.union_schema)

        if os.path.isdir(adbrix_data_path) :
            adbrixdf = spark.read.format(data_format).option('header',True).load(f'{adbrix_data_path}/*').selectExpr(Schema.pre_adbrix_attr)
            uniondf = uniondf.union(adbrixdf)
        
        if os.path.isdir(branch_data_path) :
            branchdf = spark.read.format(data_format).option('header',True).load(f'{branch_data_path}/*').selectExpr(Schema.pre_branch_attr)
            uniondf = uniondf.union(branchdf)
        
        if os.path.isdir(singular_data_path) :
            singulardf = spark.read.format(data_format).option('header',True).load(f'{singular_data_path}/*').selectExpr(Schema.pre_singular_attr)
            uniondf = uniondf.union(singulardf)        
        
        uniondf = uniondf.selectExpr(Schema.pre_union_attr)
        
        # write
        # coalesce 파티션수 줄이기/늘이기(true 옵션) (repartition과는 다르게 셔플최소화작동->파일당사이즈 제각각)
        uniondf.where('trackingId is not NULL')\
            .sort('eventTimeStamp')\
            .coalesce(20)\
            .write\
            .option('compression','gzip')\
            .option('parquet.enable.dictionary', 'true')\
            .option('parquet.block.size', f'{32*1024*1024}')\
            .option('parquet.page.size', f'{2*1024*1024}')\
            .option('parquet.dictionary.page.size', f'{8*1024*1024}')\
            .mode('overwrite')\
            .parquet(f'{self.warehouse_tier1_path}{datetime.strftime("%Y-%m-%d/%H")}')
        
        # memory clean
        del[[uniondf]]
        gc.collect()

        spark.stop()