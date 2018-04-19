import yaml
from collections import OrderedDict
import itertools
import os
import tempfile
from random import choice
from uuid import uuid4


class ValidPipelineYAMLDefinitions(object):
    """
    Helper object that acts as an iterator to generate all possible valid pipeline definition schemas. All combinations
    of valid configuration options are checked:
        - Each step (extract, transform, load) and each shared resource containing a single process are generated with
          kwargs defined as either:
            - No kwargs key present
            - Kwargs key present, but no kwargs specified
            - One kwarg present
            - Three kwargs present
            --> 4 possible single-process steps
        - Each step and shared resource can either have a single process as a dict, or multiple processes in a list.
          Steps with two and three processes are also checked, while all multi-process steps together cover all possible
          kwargs definitions that are possible
          --> 6 possible two-process steps, 4 possible three-process steps
        --> This holds true for processes.extract, processes.transform, processes.load and shared. A combination of all
            is generated, which results in (4+6+4)^4 = 38416 possible combinations that can be tested
    This class implements an iterator, which, in each iteration, writes the pipeline definition as YAML to a file in the
    OS' temp dir and returns a stream to that file, so it can simply be used as in:

    for yaml_file_stream in ValidPipelineYAMLDefinitions():
        # Do stuff

    38416 files will be generated over all iterations (and of course also removed) and their file streams returned
    """

    def __init__(self):
        self.__kwarg_defs = {'with_one_kwarg': {'class': 'cls', 'kwargs': {'a': 'b'}},
                             'without_kwargs': {'class': 'cls'},
                             'with_empty_kwargs': {'class': 'cls', 'kwargs': None},
                             'with_three_kwargs': {'class': 'cls', 'kwargs': {'a': 'a', 'b': 'b', 'c': 'c'}}}
        test_cls_mdl = 'tests.helpers.processes.'
        self.__classes = {'with_one_kwarg': [test_cls_mdl + 'ProcessWithOnePositionalArg',
                                             test_cls_mdl + 'ProcessWithOneOptionalArg',
                                             test_cls_mdl + 'ProcessWithOnePositionalAndTwoOptionalArgs'],
                          'without_kwargs': [test_cls_mdl + 'ProcessWithoutArgs',
                                             test_cls_mdl + 'ProcessWithOneOptionalArg'],
                          'with_empty_kwargs': [test_cls_mdl + 'ProcessWithoutArgs',
                                                test_cls_mdl + 'ProcessWithOneOptionalArg'],
                          'with_three_kwargs': [test_cls_mdl + 'ProcessWithThreePositionalArgs',
                                                test_cls_mdl + 'ProcessWithThreeOptionalArgs',
                                                test_cls_mdl + 'ProcessWithOnePositionalAndTwoOptionalArgs']}
        self.combinations = self.generate_combinations()
        self.__iter_index, self.iter_len = 0, len(self.combinations)
        self.__old_file_path = ''

    def __iter__(self):
        return self

    def next(self):
        if self.__old_file_path:
            try:
                os.remove(self.__old_file_path)
            except OSError as e:
                raise e

        if not self.__iter_index < self.iter_len:
            self.__iter_index = 0
            raise StopIteration

        tmp_file, file_path = tempfile.mkstemp()

        with os.fdopen(tmp_file, 'wb') as tmp_yaml_file:
            yaml.dump(self.combinations[self.__iter_index], tmp_yaml_file)

        self.__iter_index += 1

        return open(file_path, 'rb')

    def generate_combinations(self):
        process_combs = list(itertools.product(*[self.__generate_combinations_per_process('extract', 'data_frame_name'),
                                                 self.__generate_combinations_per_process('transform'),
                                                 self.__generate_combinations_per_process('load')]))
        process_combs = [OrderedDict([(k, v) for i in c for k, v in i.items()]) for c in process_combs]
        shared = self.__generate_combinations_per_process('shared', 'resource_name')
        combs_with_shared = list(itertools.product(*[process_combs, shared]))

        return [OrderedDict([('processes', c[0]), ('shared', c[1]['shared'])]) for c in combs_with_shared]

    def __generate_combinations_per_process(self, proc, extra_field=None):
        possible_defs = []
        for key, deff in self.__kwarg_defs.items():
            deff_cp = deff.copy()
            deff_cp['class'] = choice(self.__classes[key])  # Randomly select one of the valid classes
            if extra_field:
                deff_cp[extra_field] = str(uuid4())
            possible_defs.append(deff_cp)

        # Add multi-class definitions (all combinations of possible defs)
        multi_two = [{proc: list(c)} for c in itertools.combinations(possible_defs, 2)]
        multi_three = [{proc: list(c)} for c in itertools.combinations(possible_defs, 3)]

        return [{proc: c} for c in possible_defs] + multi_two + multi_three
