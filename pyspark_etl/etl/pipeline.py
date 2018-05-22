from collections import OrderedDict
from io import IOBase

import yaml
from pyspark import SparkContext
from six import class_types
from tabulate import tabulate

from pyspark_etl.etl import validation, errors
from pyspark_etl.etl.shared import Shared


class PipelineDefinition(object):
    """Pipeline definition class, from which a new pipeline can be created"""

    def __init__(self):
        self.processes = OrderedDict([('extract', []), ('transform', []), ('load', [])])
        self.shared = []

    def __str__(self):
        """Creates a printable overview of the pipeline's processors"""
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

        return '\n%s\n' % tabulate(table)

    def add_extractor(self, cls, kwargs=None):
        """
        Used to add an extractor class to the pipeline definition
        :param cls: (str) The extractor class
        :param kwargs: (dict) Dictionary of keyword arguments to be used when instantiating the class
        """
        self.__add_resource(definition=self.__build_resource_definition_dict(cls, kwargs),
                            def_type='extract')

    def add_transformer(self, cls, kwargs=None):
        """
        Used to add a transformer class to the pipeline definition
        :param cls: (str) The extractor class
        :param kwargs: (dict) Dictionary of keyword arguments to be used when instantiating the class
        """
        self.__add_resource(definition=self.__build_resource_definition_dict(cls, kwargs),
                            def_type='transform')

    def add_loader(self, cls, kwargs=None):
        """
        Used to add a loader class to the pipeline definition
        :param cls: (str) The extractor class
        :param kwargs: (dict) Dictionary of keyword arguments to be used when instantiating the class
        """
        self.__add_resource(definition=self.__build_resource_definition_dict(cls, kwargs),
                            def_type='load')

    def add_shared(self, cls, resource_name, kwargs=None):
        """
        Used to add a shared class to the pipeline definition
        :param cls: (str) The extractor class
        :param resource_name: (str) The name of the shared resource name, with which other processors can access it
        :param kwargs: (dict) Dictionary of keyword arguments to be used when instantiating the class
        """
        self.__add_resource(definition=self.__build_resource_definition_dict(cls, kwargs),
                            def_type='shared',
                            unique_kwarg=resource_name)

    def build_from_dict(self, pipeline_dict):
        """
        Builds a pipeline definition from a schematized dict (see the yaml schema in README.md for the schema)
        :param pipeline_dict: (dict) The pipeline definition dict
        """
        pipeline_dict = validation.validate_pipeline_dict_schema(pipeline_dict)

        for step, unique_kwarg in zip(['extract', 'transform', 'load', 'shared'],
                                      [None, None, None, 'resource_name']):  # TODO
            if step in pipeline_dict['processors'].keys() or step in pipeline_dict.keys():
                prs = pipeline_dict['processors'][step] if step != 'shared' else pipeline_dict[step]
                prs = [prs] if isinstance(prs, dict) else prs
                for p in prs:
                    if 'kwargs' not in p.keys():
                        p['kwargs'] = None
                    cls = validation.validate_and_get_class(p['class'], shared=(True if step == 'shared' else False))
                    definition = {'class': cls,
                                  'kwargs': validation.validate_class_args(cls=cls, passed_args=p['kwargs'])}
                    if unique_kwarg:
                        self.__add_resource(definition=definition, def_type=step, unique_kwarg=p[unique_kwarg])
                    else:
                        self.__add_resource(definition=definition, def_type=step)

    def build_from_yaml(self, yaml_file_stream):
        """
        Builds a pipeline from a schematized YAML file
        :param yaml_file_stream: A readable file stream to the YAML file
        """
        if not isinstance(yaml_file_stream, IOBase):
            raise TypeError('Argument `yaml_file_stream` must be a readable file stream')
        pipeline_dict = yaml.load(yaml_file_stream)
        self.build_from_dict(pipeline_dict)

    def __build_resource_definition_dict(self, cls, kwargs=None):
        return {'class': validation.validate_processor_parent(cls),
                'kwargs': validation.validate_class_args(cls, kwargs)}

    def __add_resource(self, definition, def_type, unique_kwarg=None):
        if not isinstance(definition['class'], class_types):
            raise TypeError('Supplied argument `cls` is not a class.')
        if def_type in ['extract', 'transform', 'load']:
            self.processes[def_type].append(definition)
        elif def_type == 'shared':
            if unique_kwarg:
                definition['resource_name'] = unique_kwarg
            self.shared.append(definition)
        else:
            raise ValueError('Argument `def_type` must be one of: `extract`, `transform` or `load`')


class Pipeline(object):
    def __init__(self, definition, sc=None):
        if not isinstance(definition, PipelineDefinition):
            raise TypeError('Supplied argument is not of type `PipelineDefinition`')
        elif len(definition.processes['extract']) == 0:
            raise errors.PipelineError('There must be at least one extract process in the pipeline definition')
        elif len(definition.processes['load']) == 0:
            raise errors.PipelineError('There must be at least one load process in the pipeline definition')

        self.processes = definition.processes
        self.sc = SparkContext.getOrCreate() if sc is None else sc
        self.logger = self.sc._jvm.org.apache.log4j.LogManager.getLogger(self.__class__.__name__)
        self.__init_resources(shared=definition.shared)

    def __init_resources(self, shared):
        for s in shared:
            shared_obj = s['class'](**s['kwargs']) if 'kwargs' in s.keys() else s['class']()
            Shared.add_resource(s['resource_name'], shared_obj)

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
