from collections import OrderedDict

import yaml
from pyspark import SparkContext
from tabulate import tabulate

from core import validation, errors
from core.shared import Shared


class PipelineDefinition(object):
    def __init__(self):
        self.processes = OrderedDict([('extract', []), ('transform', []), ('load', [])])
        self.shared = []

    def __str__(self):
        table = list()
        table.append(['Pipeline consists of the following tasks (to be performed in descending order):'])
        proc_table = [[step, proc['class'], proc['kwargs'] if 'kwargs' in proc.keys() else None]
                      for step, procs in self.processes.items() for proc in procs]
        table.append([tabulate(proc_table, headers=['Step', 'Class', 'Args'], tablefmt="rst")])

        if self.shared:
            table.append(['|'])
            table.append(['Shared resources consist of:'])
            shared_res_table = [[s['resource_name'], s['class'], s['kwargs'] if 'kwargs' in s.keys() else None]
                                for s in self.shared]
            table.append([tabulate(shared_res_table, headers=['Name', 'Class', 'Args'], tablefmt="rst")])

        return tabulate(table)

    def add_extractor(self, cls, kwargs=None):
        self.__add_resource(definition={'class': cls, 'kwargs': kwargs}, def_type='extract')

    def add_transformer(self, cls, kwargs=None):
        self.__add_resource(definition={'class': cls, 'kwargs': kwargs}, def_type='extract')

    def add_loader(self, cls, kwargs=None):
        self.__add_resource(definition={'class': cls, 'kwargs': kwargs}, def_type='extract')

    def build_from_dict(self, pipeline_dict):
        pipeline_dict = validation.validate_pipeline_dict_schema(pipeline_dict)

        for step, unique_kwarg in zip(['extract', 'transform', 'load', 'shared'],
                                      ['data_frame_name', None, None, 'resource_name']):
            if step in pipeline_dict['processes'].keys() or step in pipeline_dict.keys():
                prs = pipeline_dict['processes'][step] if step != 'shared' else pipeline_dict[step]
                prs = [prs] if isinstance(prs, dict) else prs
                for p in prs:
                    print(p['class'])
                    cls = validation.validate_and_get_class(p['class'], shared=(True if step == 'shared' else False))
                    definition = {'class': cls}
                    if 'kwargs' in p.keys() and p['kwargs']:
                        definition['kwargs'] = validation.validate_class_args(cls=cls, passed_args=p['kwargs'])
                    if unique_kwarg:
                        definition[unique_kwarg] = p[unique_kwarg]
                    self.__add_resource(definition=definition, def_type=step)

    def build_from_yaml(self, yaml_file_stream):
        pipeline_dict = yaml.load(yaml_file_stream)
        self.build_from_dict(pipeline_dict)

    def __add_resource(self, definition, def_type):
        if def_type == 'shared':
            self.shared.append(definition)
        else:
            self.processes[def_type].append(definition)


class Pipeline(object):
    def __init__(self, definition):
        if not isinstance(definition, PipelineDefinition):
            raise TypeError('Supplied argument is not of type `PipelineDefinition`')

        self.processes = definition.processes
        self.logger = SparkContext.getOrCreate()._jvm.org.apache.log4j.LogManager.getLogger(self.__class__.__name__)
        self.__init_resources(shared=definition.shared)
        self.__str__ = definition.__str__

    def __init_resources(self, shared):
        for s in shared:
            shared_obj = s['class'](**s['kwargs']) if 'kwargs' in s.keys() else s['class']()
            Shared.add_resource(s['resource_name'], shared_obj)

        # Init empty fields for data frames
        for ep in self.processes['extract']:
            Shared.add_data_frame(ep['data_frame_name'], None)

    def run(self):
        for step in ['extract', 'transform', 'load']:
            if step in self.processes.keys():
                for proc in self.processes[step]:
                    self.logger.info('Running `%s` process: `%s`'
                                     % (step, proc['class'].__module__ + '.' + proc['class'].__name__))
                    try:
                        proc = proc['class'](**proc['kwargs']) if 'kwargs' in proc.keys() else proc['class']()
                    except Exception as e:
                        raise errors.PipelineError('Exception `%s` occurred when instantiating class `%s`: \"%s\"'
                                                   % (e.__class__.__name__,
                                                      proc['class'].__module__ + '.' + proc['class'].__name__,
                                                      str(e)))

                    proc.run()
