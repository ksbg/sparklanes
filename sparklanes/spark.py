from time import time

from pyspark import SparkContext
from pyspark.sql import SparkSession

spark_context = SparkContext.getOrCreate()
spark_session = SparkSession.Builder().appName('sparklanes_%d' % int(time())).getOrCreate()  # TODO: proper name


def set_spark_context(new_sc):
    """
    Reassigns the default, globally accessible SparkContext

    Parameters
    ----------
    new_sc (pyspark.SparkContext)
    """
    if not isinstance(new_sc, SparkContext):
        raise TypeError('`new_sc` must be an instance of `pyspark.SparkContext`.')

    global spark_context
    spark_context = new_sc


def set_spark_session(new_spark):
    """
    Reassigns the default, globally accessible SparkSession

    Parameters
    ----------
    new_spark (pyspark.sql.SparkSession)
    """
    if not isinstance(new_spark, SparkSession):
        raise TypeError('`new_spark` must be an instance of `pyspark.sql.SparkSession`.')

    global spark_session
    spark_session = new_spark
