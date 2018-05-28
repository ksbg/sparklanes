import logging
import os
import sys
from time import time

from pyspark import SparkContext
from pyspark.sql import SparkSession

spark_context = SparkContext.getOrCreate()
spark_session = SparkSession.Builder().appName('sparklanes_%d' % int(time())).getOrCreate()  # TODO: proper name


def get_logger(name, level=logging.INFO, fmt='%(asctime)s - %(name)s - %(levelname)s - %(message)s'):
    # TODO: better logging
    logger = logging.getLogger(name)
    logger.setLevel(level)
    if logger.handlers:
        logger.warning('Logger `%s` already exists!' % name)
    else:
        ch = logging.StreamHandler(sys.stderr)
        ch.setLevel(level)
        formatter = logging.Formatter(fmt)
        ch.setFormatter(formatter)
        logger.addHandler(ch)

    return logger


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
