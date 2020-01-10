# -*- coding: utf-8 -*-
"""
Created on Fri Dec 27 13:46:53 2019

@author: Artur.Dossatayev
"""




from pyspark import SparkContext, SQLContext
from pyspark.sql import Window
import pyspark.sql.functions as func
import time
sc = SparkContext()
sqlctx = SQLContext(sc)


start_time = time.time()
filenames = 'file:///G:/jsons/*.json.gz'
df = sqlctx.read.json(filenames)

#df.count()  #По сути лишняя операция

df_window = Window.orderBy('log_timestamp').partitionBy('id')

excl_col = 'some_colimn_here'
clean_df = (df.select('*',
                      (func.row_number().over(df_window)).alias('rnk'))
            .where(func.col('rnk')==1)
            .select([cols for cols in df.columns if cols not in excl_col])
)

#clean_df.printSchema()  #По сути лишняя операция
#clean_df.count()  #По сути лишняя операция


clean_df.write.option("header", "true").csv("file:///C:/Python/Spark/out")
print("--- %s seconds ---" % (time.time() - start_time))