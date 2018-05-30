from time import time

from pyspark import SparkContext
from pyspark.sql import SparkSession

spark_context = SparkContext.getOrCreate()
spark_session = SparkSession.Builder().appName('sparklanes_%d' % int(time())).getOrCreate()  # TODO: proper name

