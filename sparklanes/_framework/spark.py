"""Used to allow sharing of SparkContext and SparkSession, to avoid having to "getOrCreate" them
again and again for each task. This way, they can just be imported and used right away."""
# pylint: disable=invalid-name,too-many-arguments
from pyspark import SparkContext, PickleSerializer, BasicProfiler
from pyspark.sql import SparkSession
from six import PY2

from sparklanes._framework.env import INIT_SPARK_ON_IMPORT, SPARK_APP_NAME


class SparkContextAndSessionContainer(object):
    """Container class holding SparkContext and SparkSession instances, so that any changes
    will be propagated across the application"""
    sc = None
    spark = None

    def __new__(cls, *args, **kwargs):
        if cls is SparkContextAndSessionContainer:
            raise TypeError('SparkSession & SparkContext container class may not be instantiated.')

        return object.__new__(cls, *args, **kwargs) if PY2 else object.__new__(cls)

    @classmethod
    def set_sc(cls, master=None, appName=None, sparkHome=None, pyFiles=None, environment=None,
               batchSize=0, serializer=PickleSerializer(), conf=None, gateway=None, jsc=None,
               profiler_cls=BasicProfiler):
        """Creates and initializes a new `SparkContext` (the old one will be stopped).
        Argument signature is copied from `pyspark.SparkContext
        <https://spark.apache.org/docs/latest/api/python/pyspark.html#pyspark.SparkContext>`_.
        """
        if cls.sc is not None:
            cls.sc.stop()
        cls.sc = SparkContext(master, appName, sparkHome, pyFiles, environment, batchSize,
                              serializer,
                              conf, gateway, jsc, profiler_cls)
        cls.__init_spark()

    @classmethod
    def set_spark(cls, master=None, appName=None, conf=None, hive_support=False):
        """Creates and initializes a new `SparkSession`. Argument signature is copied from
        `pyspark.sql.SparkSession
        <https://spark.apache.org/docs/latest/api/python/pyspark.sql.html#pyspark.sql.SparkSession>`_.
        """
        sess = SparkSession.builder
        if master:
            sess.master(master)
        if appName:
            sess.appName(appName)
        if conf:
            sess.config(conf=conf)
        if hive_support:
            sess.enableHiveSupport()

        cls.spark = sess.getOrCreate()

    @classmethod
    def init_default(cls):
        """Create and initialize a default SparkContext and SparkSession"""
        cls.__init_sc()
        cls.__init_spark()

    @classmethod
    def __init_sc(cls):
        cls.sc = SparkContext(appName=SPARK_APP_NAME).getOrCreate()

    @classmethod
    def __init_spark(cls):
        cls.spark = SparkSession.builder.appName(SPARK_APP_NAME).getOrCreate()


if INIT_SPARK_ON_IMPORT:
    SparkContextAndSessionContainer.init_default()
