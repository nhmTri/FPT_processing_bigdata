
# Import module
import findspark
findspark.init()
from pyspark.context import SparkContext
from pyspark.sql.session import SparkSession
from pyspark.sql.functions import *
import pandas as pd
import os
from pyspark.sql.window import Window

# Build SparkSession
spark = SparkSession.builder.config("spark.driver.memory", "8g").config("spark.executor.cores",8).getOrCreate()

# Covert_data_for 30_day
def ETL_1_day (path):
    list_path=os.listdir(path)
    df=spark.read.json(path+list_path[0])
    df=df.withColumn("Path",
              lit("{}".format(list_path[0])))
    df=df.withColumn("Date", split("Path", ".json").getItem(0))
    for i in list_path[1:]:
        save_path=i
        read_path=spark.read.json(path+save_path)
        read_path=read_path.withColumn("Path", 
                                   lit("{}".format([save_path])))
        read_path=read_path.withColumn("Date", split('Path', ".json").getItem(0))
        df=df.union(read_path)
        print('Successfully df {}'.format(i))
    df=df.drop("Path") 
    print('-----------------Done--------------')
    return df

# Prepare Activeness
def Prepare_Activeness_Job(df):
    activeness=df
    return activeness

# Tranform_basic_data
def transfrom_data (df):
    df=df.select("_source.*")
    df=df.withColumn('Type',
        when ((col('AppName')=='CHANNEL') | (col('AppName')=='KPLUS')| (col('AppName')=='KPlus'),"Truyen Hinh")
        .when((col("AppName") == 'VOD') | (col("AppName") =='FIMS_RES')| (col("AppName") =='BHD_RES')| 
              (col("AppName") =='VOD_RES')| (col("AppName") =='FIMS')| (col("AppName") =='BHD')| (col("AppName") =='DANET'), "Phim Truyen")
        .when((col("AppName") == 'RELAX'), "Giai Tri")
        .when((col("AppName") == 'CHILD'), "Thieu Nhi")
        .when((col("AppName") == 'SPORT'), "The Thao")
        .otherwise("Error")
        )
    
    df=df.select('Contract','Type','TotalDuration')
    df=df.filter(df.Type != 'Error')
    df=df.groupBy('Contract','Type').sum('TotalDuration')
    df=df.withColumnRenamed('sum(TotalDuration)','TotalDuration')
    df=df.groupBy('Contract').pivot('Type').sum('TotalDuration')
    df=df.na.fill(0)
    return df

# ETL col Most_watch
def Most_watch(df):
    df=df.withColumn("MostWatch",greatest(col("Giai Tri"),col("Phim Truyen"),col("The Thao"),col("Thieu Nhi"),col("Truyen Hinh"),col("Giai Tri")))
    df=df.withColumn('MostWatch',
        when(col('MostWatch')==col('Truyen Hinh'),'Truyen Hinh')
        .when(col('MostWatch')==col('Thieu Nhi'),'Thieu Nhi')
        .when(col('MostWatch')==col('The Thao'),'The Thao')
        .when(col('MostWatch')==col('Phim Truyen'),'Phim Truyen')
        .when(col('MostWatch')==col('Giai Tri'),'Giai Tri')
        )
    return df

# ETL_col Customer_taste
def Customer_Taste(df):
    df=df.withColumn("Taste",concat_ws("-",
                    when(col("Giai Tri")>0,lit("Giai Tri"))
                    ,when(col("Phim Truyen")>0,lit("Phim Truyen"))
                    ,when(col("The Thao")>0,lit("The Thao"))
                    ,when(col("Thieu Nhi")>0,lit("Thieu Nhi"))
                    ,when(col("Truyen Hinh")>0,lit("Truyen Hinh"))))
    return df

# ETL col Activeness
def Activeness(activeness):
    activeness=activeness.select('_source.*','Date')
    window=Window.partitionBy("Contract")
    activeness=activeness.withColumn("Active",size(collect_set("Date").over(window)))
    activeness=activeness.withColumn("Activeness",
                      when(col('Active')>=15,"High Active")
                      .otherwise("Low Active")
    )
    distinct_activeness=activeness.select(['Contract',"Activeness"]).distinct()
    return distinct_activeness 

def OLAP_OUTPUT (distinct_activeness,df):
    complete_task=distinct_activeness.join(df,['Contract'],how='inner')
    return complete_task


def main_path(path):
    df=ETL_1_day(path)
    activeness=Prepare_Activeness_Job(df)
    print ('------------Start Tranform-----------')
    df=transfrom_data(df)
    print ('------------Mostwatch----------------')
    df=Most_watch(df)
    print ('------------Customer_taste-----------')
    df=Customer_Taste(df)
    print ('--------------Acitveness-------------')
    distinct_activeness=Activeness(activeness)
    print ('----------Procesing Merge------------')
    complete_task=OLAP_OUTPUT(distinct_activeness,df)
    return complete_task.show()


path='D:\\Project_Python_(DA-DE)\\DE_Project\\BigData\\Class_5\\OLAP_OUTPUT\\Data\\'
main_path(path)

print('-----------------------------------------------All Done---------------------------------------------------------------')
