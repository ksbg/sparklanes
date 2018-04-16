from abc import ABCMeta, abstractmethod

from pyspark import SparkContext
from pyspark.sql import SparkSession


class PipelineProcessBase(object):
    __metaclass__ = ABCMeta

    def __init__(self, spark_app_name='pyspark-etl'):
        self.sc = SparkContext.getOrCreate()
        self.logger = self.sc._jvm.org.apache.log4j.LogManager.getLogger(self.__class__.__name__)
        self.spark = SparkSession.Builder().appName(spark_app_name).getOrCreate()

    @abstractmethod
    def run(self):
        pass


class SharedBase(object):
    @classmethod
    def __add_to_self(cls, name, res):
        if not isinstance(name, str):
            raise TypeError('`name` must be of type `string`')
        setattr(cls, name, res)
