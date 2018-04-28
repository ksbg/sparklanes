import os

from core.base import PipelineProcessBase
from core.shared import Shared

import csv


class ProcessNotInherited(object):
    pass


class ProcessWithoutArgs(PipelineProcessBase):
    def __init__(self, **kwargs):
        super(ProcessWithoutArgs, self).__init__(**kwargs)

    def run(self):
        return self.__class__.__name__


class ProcessWithOnePositionalArg(PipelineProcessBase):
    def __init__(self, a, **kwargs):
        super(ProcessWithOnePositionalArg, self).__init__(**kwargs)

    def run(self):
        return self.__class__.__name__


class ProcessWithOneOptionalArg(PipelineProcessBase):
    def __init__(self, a=None, **kwargs):
        super(ProcessWithOneOptionalArg, self).__init__(**kwargs)

    def run(self):
        return self.__class__.__name__


class ProcessWithOnePositionalAndTwoOptionalArgs(PipelineProcessBase):
    def __init__(self, a, b=None, c=None, **kwargs):
        super(ProcessWithOnePositionalAndTwoOptionalArgs, self).__init__(**kwargs)

    def run(self):
        return self.__class__.__name__


class ProcessWithThreePositionalArgs(PipelineProcessBase):
    def __init__(self, a, b, c, **kwargs):
        super(ProcessWithThreePositionalArgs, self).__init__(**kwargs)

    def run(self):
        return self.__class__.__name__


class ProcessWithThreeOptionalArgs(PipelineProcessBase):
    def __init__(self, a=None, b=None, c=None, **kwargs):
        super(ProcessWithThreeOptionalArgs, self).__init__(**kwargs)

    def run(self):
        return self.__class__.__name__


class ProcessAddSharedObject(PipelineProcessBase):
    def __init__(self, name, res_type, o, **kwargs):
        self.name = name
        self.res_type = res_type
        self.o = o
        super(ProcessAddSharedObject, self).__init__(**kwargs)

    def run(self):
        if self.res_type == 'shared':
            Shared.add_resource(self.name, self.o)
        elif self.res_type == 'data_frame':
            Shared.add_data_frame(self.name, self.o)
        elif self.res_type == 'rdd':
            Shared.add_rdd(self.name, self.o)
        else:
            raise ValueError


class ProcessDeleteSharedObject(PipelineProcessBase):
    def __init__(self, name, res_type, **kwargs):
        self.name = name
        self.res_type = res_type
        super(ProcessDeleteSharedObject, self).__init__(**kwargs)

    def run(self):
        if self.res_type == 'shared':
            Shared.delete_resource(self.name)
        elif self.res_type == 'data_frame':
            Shared.delete_data_frame(self.name)
        elif self.res_type == 'rdd':
            Shared.delete_rdd(self.name)
        else:
            raise ValueError


class ProcessCheckIfSharedObjectExists(PipelineProcessBase):
    def __init__(self, name, res_type, **kwargs):
        self.name = name
        self.res_type = res_type
        super(ProcessCheckIfSharedObjectExists, self).__init__(**kwargs)

    def run(self):
        if self.res_type == 'shared':
            Shared.get_resource(self.name)
        elif self.res_type == 'data_frame':
            Shared.get_data_frame(self.name)
        elif self.res_type == 'rdd':
            Shared.get_rdd(self.name)
        else:
            raise ValueError


class ProcessMultiplyIntsInSharedListByTwo(PipelineProcessBase):
    def __init__(self, resource_name, **kwargs):
        self.resource_name = resource_name
        super(ProcessMultiplyIntsInSharedListByTwo, self).__init__(**kwargs)

    def run(self):
        l = Shared.get_resource(self.resource_name)
        Shared.update_resource(self.resource_name, [i * 2 for i in l])


class ProcessAddColumnToDataFrameFromRandomColumn(PipelineProcessBase):
    def __init__(self, df_name, multiply_by, col_name, **kwargs):
        self.df_name = df_name
        self.multiply_by = multiply_by
        self.col_name = col_name
        super(ProcessAddColumnToDataFrameFromRandomColumn, self).__init__(**kwargs)

    def run(self):
        df = Shared.get_data_frame(self.df_name)
        df = df.withColumn(self.col_name, df.random * self.multiply_by)
        Shared.update_data_frame(self.df_name, df)


class ProcessExtractIntsFromCSV(PipelineProcessBase):
    def __init__(self, csv_path, **kwargs):
        self.csv_path = csv_path
        super(ProcessExtractIntsFromCSV, self).__init__(**kwargs)

    def run(self):
        df = self.spark.read.csv(path=self.csv_path,
                                 header=True)
        Shared.add_data_frame(self.data_frame_name, df)


class ProcessTransformConvertDataFrameToList(PipelineProcessBase):
    def __init__(self, output_shared_list_name, **kwargs):
        self.output_shared_list_name = output_shared_list_name
        super(ProcessTransformConvertDataFrameToList, self).__init__(**kwargs)

    def run(self):
        df = Shared.get_data_frame('ints_df')
        Shared.add_resource(self.output_shared_list_name, [i.number for i in df.collect()])


class ProcessTransformMultiplyIntsInSharedListByTwo(PipelineProcessBase):
    def __init__(self, list_name, **kwargs):
        self.list_name = list_name
        super(ProcessTransformMultiplyIntsInSharedListByTwo, self).__init__(**kwargs)

    def run(self):
        l = Shared.get_resource(self.list_name)
        Shared.update_resource(self.list_name, [int(i) * 2 for i in l])


class ProcessLoadDumpResultListToCSV(PipelineProcessBase):
    def __init__(self, list_name, output_file_name, **kwargs):
        self.list_name = list_name
        self.output_file_name = output_file_name
        super(ProcessLoadDumpResultListToCSV, self).__init__(**kwargs)

    def run(self):
        l = Shared.get_resource(self.list_name)
        with open(os.path.join(os.path.dirname(os.path.realpath(__file__)), self.output_file_name), 'wb') as out_file:
            writer = csv.writer(out_file)
            writer.writerow(['id', 'number'])
            for id, i in zip(range(10), l):
                writer.writerow([id, i])
