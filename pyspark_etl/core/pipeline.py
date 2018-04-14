from collections import OrderedDict

import yaml
from tabulate import tabulate

from core import validation


class PipelineDefinition:
    def __init__(self):
        self.processes = OrderedDict([('extract', []), ('transform', []), ('load', [])])
        self.shared = OrderedDict()

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
                                       'kwargs': validation.validate_class_args(cls=cls, cls_args=kwargs)})

    def build_from_dict(self, pipeline_dict):
        pipeline_dict = validation.validate_pipeline_dict(pipeline_dict)
        procs = OrderedDict()
        for etl_step in ['extract', 'transform', 'load']:
            procs[etl_step] = []
            steps = pipeline_dict['processes'][etl_step]
            steps = [steps] if isinstance(steps, dict) else steps
            for step in steps:
                cls = validation.validate_and_get_class(step['class'])
                self.__add_process(cls=cls, kwargs=step['kwargs'], type=etl_step)

        if 'shared' in pipeline_dict.keys():
            shared_resources = pipeline_dict['shared']
            shared_resources = [shared_resources] if isinstance(shared_resources, dict) else shared_resources
            for res in shared_resources:
                cls = validation.validate_and_get_class(res['class'], shared=True)
                self.add_shared_resource(cls=cls, kwargs=res['kwargs'], resource_name=res['shared_resource_name'])

    def build_from_yaml(self, yaml_file_stream):
        pipeline_dict = yaml.load(yaml_file_stream)
        self.build_from_dict(pipeline_dict)

    def __add_process(self, cls, kwargs, type):
        self.processes[type].append({'class': cls,
                                     'kwargs': validation.validate_class_args(cls, kwargs)})


class Pipeline:
    def __init__(self, definition):
        if not isinstance(definition, PipelineDefinition):
            raise TypeError('Supplied argument is not of type `PipelineDefinition`')

        self.definition = definition

    def run(self):
        pass
