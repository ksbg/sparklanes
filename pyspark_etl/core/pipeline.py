from collections import OrderedDict

import yaml
from pyspark import SparkContext
from tabulate import tabulate

from core import validation, errors
from core.shared import Shared, DataFrames


class PipelineDefinition(object):
    def __init__(self):
        self.processes = OrderedDict([('extract', []), ('transform', []), ('load', [])])
        self.shared = OrderedDict()
        self.data_frames = OrderedDict()

    def __str__(self):
        table = list()

        table.append(['Pipeline consists of the following tasks (to be performed in descending order):'])
        proc_table = [[step, proc['class'], proc['kwargs']] for step, procs in self.processes.items()
                      for proc in procs]
        table.append([tabulate(proc_table, headers=['Step', 'Class', 'Args'], tablefmt="rst")])

        if self.shared:
            table.append(['|'])
            table.append(['Shared resources consist of:'])
            shared_res_table = [[name, s['class'], s['kwargs']] for name, s in self.shared.items()]
            table.append([tabulate(shared_res_table, headers=['Name', 'Class', 'Args'], tablefmt="rst")])

        return tabulate(table)

    def add_extractor(self, cls, kwargs):
        self.__add_process(cls, kwargs, 'extract')

    def add_transformer(self, cls, kwargs):
        self.__add_process(cls, kwargs, 'transform')

    def add_loader(self, cls, kwargs):
        self.__add_process(cls, kwargs, 'load')

    def add_shared_resource(self, cls, kwargs, resource_name):
        self.shared[resource_name] = ({'class': cls,
                                       'kwargs': validation.validate_class_args(cls=cls, passed_args=kwargs)})

    def build_from_dict(self, pipeline_dict):
        pipeline_dict = validation.validate_pipeline_dict(pipeline_dict)

        for step, unique_kwarg in zip(['extract', 'transform', 'load', 'shared'],
                                      ['data_frame_name', None, None, 'shared_resource_name']):
            if step in pipeline_dict['processes'].keys() or step in pipeline_dict.keys():
                prs = pipeline_dict['processes'][step] if step != 'shared' else pipeline_dict[step]
                prs = [prs] if isinstance(prs, dict) else prs
                for p in prs:
                    print(p['class'])
                    kwargs = {'cls': validation.validate_and_get_class(p['class'],
                                                                       shared=(True if step == 'shared' else False)),
                              'res_type': step}
                    if 'kwargs' in p.keys():
                        kwargs['kwargs'] = p['kwargs']
                    if unique_kwarg:
                        kwargs[unique_kwarg] = p[unique_kwarg]
                    self.__add_resource(**kwargs)

    def build_from_yaml(self, yaml_file_stream):
        pipeline_dict = yaml.load(yaml_file_stream)
        self.build_from_dict(pipeline_dict)

    def __add_resource(self, cls, res_type, kwargs=None, shared_resource_name=None, data_frame_name=None):
        res = {'class': cls, 'kwargs': validation.validate_class_args(cls=cls, passed_args=kwargs)}
        if res_type == 'shared':
            Shared.add_resource(name=shared_resource_name, res=res)
            return
        elif res_type == 'extract':
            DataFrames.add_data_frame(name=data_frame_name, df=None)  # Init empty field for data frame

        self.processes[res_type].append(res)

    def __add_process(self, cls, kwargs, type):
        self.processes[type].append({'class': cls,
                                     'kwargs': validation.validate_class_args(cls, kwargs)})


class Pipeline(object):
    def __init__(self, definition):
        if not isinstance(definition, PipelineDefinition):
            raise TypeError('Supplied argument is not of type `PipelineDefinition`')

        self.shared = definition.shared
        self.data_frames = definition.data_frames
        self.processes = definition.processes

        self.logger = SparkContext.getOrCreate()._jvm.org.apache.log4j.LogManager.getLogger(self.__class__.__name__)

    def run(self):
        for step in ['extract', 'transform', 'load']:
            if step in self.processes.keys():
                for proc in self.processes[step]:
                    self.logger.info('Running `%s` process: `%s`'
                                     % (step, proc['class'].__module__ + '.' + proc['class'].__name__))
                    try:
                        proc = proc['class'](**proc['kwargs'])
                    except Exception as e:
                        raise errors.PipelineError('Exception `%s` occurred when instantiating class `%s`: \"%s\"'
                                                   % (e.__class__.__name__,
                                                     proc['class'].__module__ + '.' + proc['class'].__name__,
                                                     str(e)))

                    self.data_frames = proc.run()
