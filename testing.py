#import pandas
#import numpy as np

#df = pandas.read_csv("merged19_20_21.csv")
#print(df.head())
#print(df.describe())
#print(df.groupby("etuus").count().show())

from pyspark.sql import SparkSession
import csv
import pandas as pd

spark = SparkSession.builder.master("local").appName("lopputyo").getOrCreate()

df = spark.read.csv("merged19_20_21.csv", header=True, inferSchema=True)
df.show(vertical=True)
df.printSchema()
df.registerTempTable("example")
df.groupBy('etuus').count().orderBy('count',ascending=False).show()