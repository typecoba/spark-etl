from common.SessionHelper import SessionHelper
from common.Schema import Schema
import pyspark.sql.functions as F

class RunPreprocess:
    def run():
        print('run preprocess')

        # spark session 
        spark = SessionHelper.get_session()
        # print(spark)


        # rawdata path
        adbrix_data_path = '/home/data/dmc-integrated-analytics/rawdata/thirdparty-adbrix/2021-12/01/15/*'
        branch_data_path = '/home/data/dmc-integrated-analytics/rawdata/thirdparty-branch/2021-12/01/15/*'
        singular_data_path = '/home/data/dmc-integrated-analytics/rawdata/thirdparty-singular/2021-12/01/15/*'

        # attribute


        # rawdata load & transform
        data_format = 'json'
        adbrixdf = spark.read.format(data_format).option('header',True).load(adbrix_data_path).selectExpr(Schema.pre_adbrix_attr)
        branchdf = spark.read.format(data_format).option('header',True).load(branch_data_path).selectExpr(Schema.pre_branch_attr)
        # singulardf = spark.read.format(data_format).option('header',True).load(singular_data_path).selectExpr(Schema.pre_singular_attr)

        uniondf = adbrixdf.union(branchdf).selectExpr(Schema.pre_union_attr)
        
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
            .parquet(f'/home/data/temp')
        
        spark.stop()
