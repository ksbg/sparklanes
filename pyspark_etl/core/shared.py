from collections import OrderedDict

from pyspark import RDD
from pyspark.sql import DataFrame

from core import errors


class Shared(object):
    __data_frames = OrderedDict()
    __rdds = OrderedDict()
    __resources = OrderedDict()

    @staticmethod
    def add_resource(name, res):
        if name in Shared.__resources.keys():
            raise errors.PipelineSharedResourceError('A shared resource named `%s` already exists' % name)
        Shared.__resources[name] = res

    @staticmethod
    def add_data_frame(name, df):
        if name in Shared.__data_frames.keys():
            raise errors.PipelineSharedResourceError('A DataFrame named `%s` already exists' % name)
        if not isinstance(df, DataFrame):
            raise errors.PipelineSharedResourceError('Supplied resource is not an instance of `pyspark.sql.DataFrame`')
        Shared.__data_frames[name] = df

    @staticmethod
    def add_rdd(name, rdd):
        if name in Shared.__rdds.keys():
            raise errors.PipelineSharedResourceError('A RDD named `%s` already exists' % name)
        if not isinstance(rdd, RDD):
            raise errors.PipelineSharedResourceError('Supplied resource is not an instance of `pyspark.RDD`')
        Shared.__rdds[name] = rdd

    @staticmethod
    def update_resource(name, res):
        if name not in Shared.__resources.keys():
            raise errors.PipelineSharedResourceError('A shared resource named `%s` doesn\'t exist' % name)
        Shared.__resources[name] = res

    @staticmethod
    def update_data_frame(name, df):
        if name not in Shared.__data_frames.keys():
            raise errors.PipelineSharedResourceError('A data frame resource named `%s` doesn\'t exist' % name)
        if not isinstance(df, DataFrame):
            raise errors.PipelineSharedResourceError('Supplied resource is not an instance of `pyspark.sql.DataFrame`')
        Shared.__data_frames[name] = df

    @staticmethod
    def update_rdd(name, rdd):
        if name not in Shared.__rdds.keys():
            raise errors.PipelineSharedResourceError('A RDD named `%s` already exists' % name)
        if not isinstance(rdd, RDD):
            raise errors.PipelineSharedResourceError('Supplied resource is not an instance of `pyspark.RDD`')
        Shared.__rdds[name] = rdd

    @staticmethod
    def get_resource(name):
        if name not in Shared.__resources.keys():
            raise errors.PipelineSharedResourceError('A shared resource named `%s` already exists' % name)
        return Shared.__resources[name]

    @staticmethod
    def get_data_frame(name):
        if name not in Shared.__data_frames.keys():
            raise errors.PipelineSharedResourceError('A data frame named `%s` already exists' % name)
        return Shared.__data_frames[name]

    @staticmethod
    def get_rdd(name):
        if name not in Shared.__rdds.keys():
            raise errors.PipelineSharedResourceError('A RDD named `%s` already exists' % name)
        return Shared.__rdds[name]

    @staticmethod
    def get_all_resources():
        return Shared.__resources

    @staticmethod
    def get_all_data_frames():
        return Shared.__data_frames

    @staticmethod
    def get_all_rdds():
        return Shared.__rdds

    @staticmethod
    def delete_resource(name):
        Shared.__delete(name, 'resource')

    @staticmethod
    def delete_data_frame(name):
        Shared.__delete(name, 'data_frame')

    @staticmethod
    def delete_rdd(name):
        Shared.__delete(name, 'rdd')

    @staticmethod
    def delete_all():
        Shared.__data_frames = OrderedDict()
        Shared.__rdds = OrderedDict()
        Shared.__resources = OrderedDict()

    @staticmethod
    def __delete(name, res_type):
        try:
            if res_type == 'resource':
                Shared.__resources.pop(name)
            elif res_type == 'data_frame':
                Shared.__data_frames.pop(name)
            elif res_type == 'rdd':
                Shared.__rdds.pop(name)
            else:
                raise KeyError
        except KeyError:
            raise errors.PipelineSharedResourceError('Resource/DataFrame/RDD `%s` with name `%s` not found' %
                                                     (res_type, name))
