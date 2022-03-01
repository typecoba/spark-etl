from common.SessionHelper import SessionHelper
from common.Schema import Schema
import pyspark.sql.functions as F
import os, gc
from datetime import datetime, timedelta
import time

class RunProcessEventDaily:
    warehouse_tier1_path = '/home/data/data-warehouse/tier1/event/'    
    start = time.time()

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
            startdate = list(sorted(os.listdir(self.warehouse_tier1_path), key=mtime, reverse=True))[0] # 마지막폴더 3번째전... 임시            
        else:
            startdate = '2021-01-01'
        
        mindate = datetime.strptime(startdate, '%Y-%m-%d')
        maxdate = datetime.strptime('2021-12-31', '%Y-%m-%d')
        daterange = [mindate + timedelta(hours=x) for x in range(0, (maxdate - mindate).days)]
        
        for date in daterange:
            # path = self.warehouse_tier1_path + date.strftime('%Y-%m-%d/%H') # 폴더생성 path
            path = self.warehouse_tier1_path + f"{date.strftime('%Y-%m-%d')}" # 폴더확인
            print(f'==== {path}')
            if os.path.isdir(path) == False:
                os.makedirs(path)
                self.process(date)
                break             
        
        # test
        # self.process(datetime.strptime('2021-06-04 00', '%Y-%m-%d %H'))
        
    def process(self, datetime:datetime):
        print('==== process')
        # spark session 
        spark = SessionHelper.get_session()
        
        # rawdata path 시간별
        datepath = datetime.strftime('%Y-%m/%d') # raw데이터 폴더구조        
        adbrix_data_path = f'/home/data/dmc-integrated-analytics/rawdata/thirdparty-adbrix/{datepath}'
        branch_data_path = f'/home/data/dmc-integrated-analytics/rawdata/thirdparty-branch/{datepath}'
        singular_data_path = f'/home/data/dmc-integrated-analytics/rawdata/thirdparty-singular/{datepath}'

        # rawdata load & transform        
        uniondf = spark.createDataFrame([], schema=Schema.union_schema) # 스키마가 union대상과 일치해야 함
        if os.path.isdir(adbrix_data_path) :
            adbrixdf = spark.read.format('json').option('header',True).load(f'{adbrix_data_path}/*').selectExpr(Schema.pre_adbrix_attr)
            uniondf = uniondf.union(adbrixdf)
        if os.path.isdir(branch_data_path) :    
            branchdf = spark.read.format('json').option('header',True).load(f'{branch_data_path}/*').selectExpr(Schema.pre_branch_attr)        
            uniondf = uniondf.union(branchdf)
        if os.path.isdir(singular_data_path) :
            singulardf = spark.read.format('json').option('header',True).load(f'{singular_data_path}/*').selectExpr(Schema.pre_singular_attr)            
            uniondf = uniondf.union(singulardf)
        
                                    
        # write
        # coalesce(0) 파티션수 줄이기/늘이기 (repartition과는 다르게 셔플최소화작동->파일당사이즈 제각각)
        # repartition(0,'') 파티션 균일분할, 
        # partitionBy('') 컬럼기준 파티션 분할
        # 파티션은 core 사이즈랑 맞춰서 -> 테스트
        # .sort('eventTimeStamp')\ -> 정렬필요있나?
        # partitionBy시 컬럼유지위해 withColumn으로 복제컬럼 추가
        uniondf\
            .withColumn('date', uniondf.eventDate)\
            .withColumn('hour', uniondf.eventHour)\
            .where('trackingId is not NULL')\
            .coalesce(2)\
            .write\
            .partitionBy('date')\
            .option('compression','gzip')\
            .mode('overwrite')\
            .parquet(f'{self.warehouse_tier1_path}{datetime.strftime("%Y-%m-%d")}')
        # .parquet(f'{self.warehouse_tier1_path}')

        print(f'==== time: {int(time.time() - self.start)}s')
        
        # memory clean
        del[[uniondf]]
        gc.collect()

        spark.stop()