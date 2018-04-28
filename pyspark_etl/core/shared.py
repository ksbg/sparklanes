from collections import OrderedDict

from pyspark import RDD
from pyspark.sql import DataFrame
from six import string_types

from core import errors


class Shared(object):
    __data_frames = OrderedDict()
    __rdds = OrderedDict()
    __resources = OrderedDict()

    @staticmethod
    def __validate_name_type(name):
        if not isinstance(name, string_types):
            raise errors.PipelineInvalidResourceNameError('`name` must be a string')

    @staticmethod
    def add_resource(name, res):
        Shared.__validate_name_type(name)
        if name in Shared.__resources.keys():
            raise errors.PipelineSharedResourceError('A shared resource named `%s` already exists' % name)
        Shared.__resources[name] = res

    @staticmethod
    def add_data_frame(name, df):
        Shared.__validate_name_type(name)
        if name in Shared.__data_frames.keys():
            raise errors.PipelineSharedResourceError('A DataFrame named `%s` already exists' % name)
        if not isinstance(df, DataFrame):
            raise errors.PipelineSharedResourceTypeInvalid(
                'Supplied resource is not an instance of `pyspark.sql.DataFrame`')
        Shared.__data_frames[name] = df

    @staticmethod
    def add_rdd(name, rdd):
        Shared.__validate_name_type(name)
        if name in Shared.__rdds.keys():
            raise errors.PipelineSharedResourceError('A RDD named `%s` already exists' % name)
        if not isinstance(rdd, RDD):
            raise errors.PipelineSharedResourceTypeInvalid('Supplied resource is not an instance of `pyspark.RDD`')
        Shared.__rdds[name] = rdd

    @staticmethod
    def update_resource(name, res):
        Shared.__validate_name_type(name)
        if not isinstance(name, string_types):
            raise errors.PipelineSharedResourceError('`name` must be a string')
        if name not in Shared.__resources.keys():
            raise errors.PipelineSharedResourceNotFound('A shared resource named `%s` doesn\'t exist' % name)
        Shared.__resources[name] = res

    @staticmethod
    def update_data_frame(name, df):
        Shared.__validate_name_type(name)
        if name not in Shared.__data_frames.keys():
            raise errors.PipelineSharedResourceNotFound('A data frame resource named `%s` doesn\'t exist' % name)
        if not isinstance(df, DataFrame):
            raise errors.PipelineSharedResourceTypeInvalid(
                'Supplied resource is not an instance of `pyspark.sql.DataFrame`')
        Shared.__data_frames[name] = df

    @staticmethod
    def update_rdd(name, rdd):
        Shared.__validate_name_type(name)
        if name not in Shared.__rdds.keys():
            raise errors.PipelineSharedResourceNotFound('A RDD named `%s` doesn\'t exist' % name)
        if not isinstance(rdd, RDD):
            raise errors.PipelineSharedResourceTypeInvalid('Supplied resource is not an instance of `pyspark.RDD`')
        Shared.__rdds[name] = rdd

    @staticmethod
    def get_resource(name):
        Shared.__validate_name_type(name)
        if name not in Shared.__resources.keys():
            raise errors.PipelineSharedResourceNotFound('A shared resource named `%s` can\'t be found' % name)
        return Shared.__resources[name]

    @staticmethod
    def get_data_frame(name):
        Shared.__validate_name_type(name)
        if name not in Shared.__data_frames.keys():
            raise errors.PipelineSharedResourceNotFound('A data frame named `%s` can\'t be found' % name)
        return Shared.__data_frames[name]

    @staticmethod
    def get_rdd(name):
        Shared.__validate_name_type(name)
        if name not in Shared.__rdds.keys():
            raise errors.PipelineSharedResourceNotFound('A RDD named `%s` can\'t be found' % name)
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
        Shared.__validate_name_type(name)
        Shared.__delete(name, 'resource')

    @staticmethod
    def delete_data_frame(name):
        Shared.__validate_name_type(name)
        Shared.__delete(name, 'data_frame')

    @staticmethod
    def delete_rdd(name):
        Shared.__validate_name_type(name)
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
            raise errors.PipelineSharedResourceNotFound('Resource/DataFrame/RDD `%s` with name `%s` not found' %
                                                        (res_type, name))
