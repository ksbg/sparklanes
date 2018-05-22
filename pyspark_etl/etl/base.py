from abc import ABCMeta, abstractmethod

from pyspark import SparkContext
from pyspark.sql import SparkSession


class PipelineProcessorBase(object):
    """Base class from which all pipeline processors must inherit"""
    __metaclass__ = ABCMeta

    def __init__(self, spark_app_name='pyspark-etl'):
        """
        :param pyspark-etl: (str) Name of the spark app (used when creating a SparkSession)
        """
        self.sc = SparkContext.getOrCreate()
        self.logger = self.sc._jvm.org.apache.log4j.LogManager.getLogger(self.__class__.__name__)
        self.spark = SparkSession.Builder().appName(spark_app_name).getOrCreate()

    @abstractmethod
    def run(self):
        pass
