from common.SessionHelper import SessionHelper
from common.Schema import Schema
import pyspark.sql.functions as F
import os, gc
from datetime import datetime, timedelta
import time
'''
'''
class PurchaseByUser:
    warehouse_tier1_path = '/home/data/data-warehouse/tier1/event/'
    warehouse_tier2_path = '/home/data/data-warehouse/tier2/purchaseByUser/'
    start = time.time()

    def run(self):        
        mindate = datetime.strptime('2020-12-31', '%Y-%m-%d')
        maxdate = datetime.strptime('2022-01-01', '%Y-%m-%d')
        daterange = [mindate + timedelta(days=x) for x in range(0, (maxdate - mindate).days)]
        # print(len(daterange))

        # num = 0
        for date in daterange:
            path = f'{self.warehouse_tier2_path}date={date.strftime("%Y-%m-%d")}' # 폴더확인용    
            if os.path.isdir(path) == False:
                # num = num+1
                # if num > 8 : break
                # print(f'==== {path}')
                # os.makedirs(path)
                self.process(date)

        # self.process()

    def process(self, datetime:datetime=None):
        # session
        spark = SessionHelper.get_session()

        # dataload
        datepath = datetime.strftime('%Y-%m-%d')
        event_data_path = f'{self.warehouse_tier1_path}/date={datepath}/*'
        # event_data_path = f'{self.warehouse_tier1_path}/*'
        spark\
            .read\
            .format('parquet')\
            .option('header',True)\
            .load(f'{event_data_path}')\
            .createOrReplaceTempView('t_master')

        # transform
        sql='''
            SELECT 
                trackingid          as app_name,
                osTypeCode          as os_typecode,
                cid                 as c_id,
                contentId           as content_id,
                SUM(quantity)       as quantity_total,
                SUM(amount)         as amount_total,
                eventDate           as event_date,
                year(eventDate)     as event_year,
                month(eventDate)    as event_month,
                day(eventDate)      as event_day
            FROM t_master
            WHERE 1=1
                AND amount > 0
                AND trackingEventCode = 'TPD004'
            GROUP BY trackingid, osTypeCode, eventDate, cid, contentId
            ORDER BY trackingid, eventDate
        '''

        df = spark.sql(sql)
        
        # write
        df\
            .coalesce(1)\
            .write\
            .option('compression','gzip')\
            .mode('overwrite')\
            .parquet(f'{self.warehouse_tier2_path}date={datepath}')        
        # .partitionBy('date')\
        # .withColumn('date', df.eventDate)\
        # .withColumn('year', F.year(df.eventDate))\
        # .withColumn('month', F.month(df.eventDate))\
        # .withColumn('day', F.dayofmonth(df.eventDate))\
        # elastic search



        print(f'==== time: {int(time.time() - self.start)}s')

        # del[[df]]
        # gc.collect()

        # spark.stop()