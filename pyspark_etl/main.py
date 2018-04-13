from pyspark import SparkContext
from pyspark.sql import SparkSession

sc = SparkContext.getOrCreate()
ss = SparkSession.Builder().appName('test').getOrCreate()
print('IT WORKS')
