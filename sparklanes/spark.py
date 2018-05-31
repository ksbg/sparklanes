"""Used to allow sharing of SparkContext and SparkSession, to avoid having to instantiate them again and again for
each task."""
from pyspark import SparkContext, PickleSerializer, BasicProfiler
from pyspark.sql import SparkSession

from ._framework.env import INIT_SPARK_ON_START

context = None
session = None


def set_context(master=None, appName=None, sparkHome=None, pyFiles=None, environment=None, batchSize=0,
                serializer=PickleSerializer(), conf=None, gateway=None, jsc=None, profiler_cls=BasicProfiler):
    global context
    context.stop()
    context = SparkContext(master, appName, sparkHome, pyFiles, environment, batchSize, serializer, conf, gateway, jsc,
                           profiler_cls)
    __init_session()


def set_session(master=None, appName=None, conf=None, hive_support=False):
    global session

    sess_tmp = SparkSession.builder
    if master:
        sess_tmp.master(master)
    if appName:
        sess_tmp.appName(appName)
    if conf:
        sess_tmp.config(conf=conf)
    if hive_support:
        sess_tmp.enableHiveSupport()

    session = sess_tmp.getOrCreate()


def __init_context():
    global context
    context = SparkContext(appName='sparklanes').getOrCreate()


def __init_session():
    global session
    session = SparkSession.builder.appName('sparklanes').getOrCreate()


if INIT_SPARK_ON_START:
    __init_context()
    __init_session()
