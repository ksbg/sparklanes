from time import time
import logging
import sys
from pyspark import SparkContext
from pyspark.sql import SparkSession

spark_context = SparkContext.getOrCreate()
spark_session = SparkSession.Builder().appName('sparklanes_%d' % int(time())).getOrCreate()  # TODO: proper name


def get_logger(name, level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'):
    logger = logging.getLogger(name)
    logger.setLevel(level)
    if logger.handlers:
        logger.warning('Logger `%s` already exists!' % name)
    else:
        ch = logging.StreamHandler(sys.stderr)
        ch.setLevel(level)
        formatter = logging.Formatter(format)
        ch.setFormatter(formatter)
        logger.addHandler(ch)

    return logger


def set_spark_context(new_sc):
    global spark_context
    spark_context = new_sc

    return spark_context


def set_spark_session(new_spark):
    global spark_session
    spark_session = new_spark

    return spark_session
