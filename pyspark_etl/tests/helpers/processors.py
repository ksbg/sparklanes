import os

from pyspark_etl.etl.base import PipelineProcessorBase
from pyspark_etl.etl.shared import Shared

import csv


class ProcessNotInherited(object):
    pass


class ProcessorWithoutArgs(PipelineProcessorBase):
    def __init__(self, **kwargs):
        super(ProcessorWithoutArgs, self).__init__(**kwargs)

    def run(self):
        return self.__class__.__name__


class ProcessorWithOnePositionalArg(PipelineProcessorBase):
    def __init__(self, a, **kwargs):
        super(ProcessorWithOnePositionalArg, self).__init__(**kwargs)

    def run(self):
        return self.__class__.__name__


class ProcessorWithOneOptionalArg(PipelineProcessorBase):
    def __init__(self, a=None, **kwargs):
        super(ProcessorWithOneOptionalArg, self).__init__(**kwargs)

    def run(self):
        return self.__class__.__name__


class ProcessorWithOnePositionalAndTwoOptionalArgs(PipelineProcessorBase):
    def __init__(self, a, b=None, c=None, **kwargs):
        super(ProcessorWithOnePositionalAndTwoOptionalArgs, self).__init__(**kwargs)

    def run(self):
        return self.__class__.__name__


class ProcessorWithThreePositionalArgs(PipelineProcessorBase):
    def __init__(self, a, b, c, **kwargs):
        super(ProcessorWithThreePositionalArgs, self).__init__(**kwargs)

    def run(self):
        return self.__class__.__name__


class ProcessorWithThreeOptionalArgs(PipelineProcessorBase):
    def __init__(self, a=None, b=None, c=None, **kwargs):
        super(ProcessorWithThreeOptionalArgs, self).__init__(**kwargs)

    def run(self):
        return self.__class__.__name__


class ProcessorAddSharedObject(PipelineProcessorBase):
    def __init__(self, name, res_type, o, **kwargs):
        self.name = name
        self.res_type = res_type
        self.o = o
        super(ProcessorAddSharedObject, self).__init__(**kwargs)

    def run(self):
        if self.res_type == 'shared':
            Shared.add_resource(self.name, self.o)
        elif self.res_type == 'data_frame':
            Shared.add_data_frame(self.name, self.o)
        elif self.res_type == 'rdd':
            Shared.add_rdd(self.name, self.o)
        else:
            raise ValueError


class ProcessorDeleteSharedObject(PipelineProcessorBase):
    def __init__(self, name, res_type, **kwargs):
        self.name = name
        self.res_type = res_type
        super(ProcessorDeleteSharedObject, self).__init__(**kwargs)

    def run(self):
        if self.res_type == 'shared':
            Shared.delete_resource(self.name)
        elif self.res_type == 'data_frame':
            Shared.delete_data_frame(self.name)
        elif self.res_type == 'rdd':
            Shared.delete_rdd(self.name)
        else:
            raise ValueError


class ProcessorCheckIfSharedObjectExists(PipelineProcessorBase):
    def __init__(self, name, res_type, **kwargs):
        self.name = name
        self.res_type = res_type
        super(ProcessorCheckIfSharedObjectExists, self).__init__(**kwargs)

    def run(self):
        if self.res_type == 'shared':
            Shared.get_resource(self.name)
        elif self.res_type == 'data_frame':
            Shared.get_data_frame(self.name)
        elif self.res_type == 'rdd':
            Shared.get_rdd(self.name)
        else:
            raise ValueError


class ProcessorMultiplyIntsInSharedListByTwo(PipelineProcessorBase):
    def __init__(self, resource_name, **kwargs):
        self.resource_name = resource_name
        super(ProcessorMultiplyIntsInSharedListByTwo, self).__init__(**kwargs)

    def run(self):
        l = Shared.get_resource(self.resource_name)
        Shared.update_resource(self.resource_name, [i * 2 for i in l])


class ProcessorAddColumnToDataFrameFromRandomColumn(PipelineProcessorBase):
    def __init__(self, df_name, multiply_by, col_name, **kwargs):
        self.df_name = df_name
        self.multiply_by = multiply_by
        self.col_name = col_name
        super(ProcessorAddColumnToDataFrameFromRandomColumn, self).__init__(**kwargs)

    def run(self):
        df = Shared.get_data_frame(self.df_name)
        df = df.withColumn(self.col_name, df.random * self.multiply_by)
        Shared.update_data_frame(self.df_name, df)


class ProcessorExtractIntsFromCSV(PipelineProcessorBase):
    def __init__(self, csv_path, **kwargs):
        self.csv_path = csv_path
        super(ProcessorExtractIntsFromCSV, self).__init__(**kwargs)

    def run(self):
        df = self.spark.read.csv(path=self.csv_path,
                                 header=True)
        Shared.add_data_frame('ints_df', df)


class ProcessorTransformConvertDataFrameToList(PipelineProcessorBase):
    def __init__(self, output_shared_list_name, **kwargs):
        self.output_shared_list_name = output_shared_list_name
        super(ProcessorTransformConvertDataFrameToList, self).__init__(**kwargs)

    def run(self):
        df = Shared.get_data_frame('ints_df')
        Shared.add_resource(self.output_shared_list_name, [i.number for i in df.collect()])


class ProcessorTransformMultiplyIntsInSharedListByTwo(PipelineProcessorBase):
    def __init__(self, list_name, **kwargs):
        self.list_name = list_name
        super(ProcessorTransformMultiplyIntsInSharedListByTwo, self).__init__(**kwargs)

    def run(self):
        l = Shared.get_resource(self.list_name)
        Shared.update_resource(self.list_name, [int(i) * 2 for i in l])


class ProcessorLoadDumpResultListToCSV(PipelineProcessorBase):
    def __init__(self, list_name, output_file_name, **kwargs):
        self.list_name = list_name
        self.output_file_name = output_file_name
        super(ProcessorLoadDumpResultListToCSV, self).__init__(**kwargs)

    def run(self):
        l = Shared.get_resource(self.list_name)
        with open(os.path.join(os.path.dirname(os.path.realpath(__file__)), self.output_file_name), 'w') as out_file:
            writer = csv.writer(out_file)
            writer.writerow(['id', 'number'])
            for id, i in zip(range(10), l):
                writer.writerow([id, i])
