
from pyspark.sql import SparkSession
import csv
import pandas as pd

spark = SparkSession.builder \
.master("local") \
.appName("Exercise3")\
.getOrCreate()
df= spark.read.csv('data_2022.csv', header = True)

l = []
 

l.append(pd.read_csv("data_2019.csv"))
l.append(pd.read_csv("data_2020.csv"))
l.append(pd.read_csv("data_2021.csv"))
     
df_res = pd.concat(l, ignore_index=True)

df_res.to_csv("merged19_20_21.csv")