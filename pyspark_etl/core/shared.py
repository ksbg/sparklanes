from core.base import SharedBase


class Shared(SharedBase):
    @staticmethod
    def add_resource(name, res):
        getattr(Shared, '_%s__add_to_self' % SharedBase.__name__)(name, res)


class DataFrames(SharedBase):
    @staticmethod
    def add_data_frame(name, df):
        getattr(DataFrames, '_%s__add_to_self' % SharedBase.__name__)(name, df)