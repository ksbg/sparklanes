from collections import OrderedDict

from core import errors


class Shared(object):
    data_frames = OrderedDict()
    resources = OrderedDict()

    @staticmethod
    def add_resource(name, res):
        Shared.resources[name] = res

    @staticmethod
    def add_data_frame(name, df):
        Shared.data_frames[name] = df

    @staticmethod
    def delete_resource(name):
        Shared.__delete(name, 'resource')

    @staticmethod
    def delete_data_frame(name):
        Shared.__delete(name, 'data_frame')

    @staticmethod
    def __delete(name, type):
        try:
            if type == 'resource':
                Shared.resources.pop(name)
            elif type == 'data_frame':
                Shared.data_frames.pop(name)
            else:
                raise KeyError
        except KeyError:
            raise errors.PipelineSharedResourceError('Resource `%s` with name `%s` not found' % (type, name))