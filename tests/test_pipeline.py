import warnings
from unittest import TestCase

from six import PY3

from sparklanes.framework import errors
from sparklanes.framework.pipeline import PipelineDefinition
from .helpers import processors


class TestPipelineDefinition(TestCase):
    def setUp(self):
        if PY3:
            warnings.simplefilter('ignore', ResourceWarning)
        self.example_definition = PipelineDefinition()

    def test_build_from_yaml(self):
        # Test argument type (all valid types already checked during integration tests)
        invalid_arg_types = [1, '1', [1], {'1': 1}, TestCase, self]
        for arg in invalid_arg_types:
            self.assertRaises(TypeError, PipelineDefinition().build_from_yaml, arg)

    def test_build_from_dict(self):
        # Invalid argument types (all valid types already checked during integration tests)
        args = ([1, 2, 3],
                '1',
                True,
                self,
                {},
                None)
        for arg in args:
            self.assertRaises(errors.PipelineSchemaError, self.example_definition.build_from_dict, arg)

    def test_add_extractor(self):
        self.__test_extractor_transformer_loader(self.example_definition.add_extractor)

    def test_add_transformer(self):
        self.__test_extractor_transformer_loader(self.example_definition.add_transformer)

    def test_add_loader(self):
        self.__test_extractor_transformer_loader(self.example_definition.add_loader)

    def test_shared(self):
        self.__test_extractor_transformer_loader(self.example_definition.add_shared, True)

    def __test_extractor_transformer_loader(self, mtd, unique_kwarg=False):
        """All three methods (add_transfomer, add_extractor, add_loader) are almost the same, so their tests can be
        wrapped."""
        # Invalid class (not inherited from base processor class)
        args = ((TestCase, None),
                (processors.ProcessNotInherited, None))
        for packed_args in args:
            packed_args = packed_args if not unique_kwarg else (packed_args[0], 'a_name', packed_args[1])
            self.assertRaises(errors.PipelineInvalidClassError, mtd, *packed_args)

        # Invalid argument types
        args = ((1, None),
                ('2', None),
                ([1, 2, 3], None),
                (None, None),
                (processors.ProcessorWithOnePositionalArg, [1, 2, 3]),
                (processors.ProcessorWithOnePositionalArg, 1),
                (processors.ProcessorWithOnePositionalArg, '1'))
        for packed_args in args:
            packed_args = packed_args if not unique_kwarg else (packed_args[0], 'a_name', packed_args[1])
            self.assertRaises((AttributeError, TypeError), mtd, *packed_args)

        # Invalid kwargs
        args = ((processors.ProcessorWithOnePositionalArg, {'a': 100, 'b': 200}),  # Too many kwargs
                (processors.ProcessorWithoutArgs, {'a': 100}),  # Too many kwargs
                (processors.ProcessorWithThreeOptionalArgs, {'a': 100, 'b': 200, 'c': 300, 'd': 400}),  # Too many kwargs
                (processors.ProcessorWithOnePositionalArg, None),  # Too few kwargs
                (processors.ProcessorWithOnePositionalAndTwoOptionalArgs, {}),  # Too few kwargs
                (processors.ProcessorWithThreePositionalArgs, {'a': 100, 'b': 200}),  # Too few kwargs
                (processors.ProcessorWithThreeOptionalArgs, {'d': 400, 'e': 500, 'f': 600}),  # Invalid kwarg names
                (processors.ProcessorWithOnePositionalAndTwoOptionalArgs, {'d': 400, 'e': 500, 'f': 600}),  # ""
                (processors.ProcessorWithOnePositionalArg, {'d': 400, 'e': 500, 'f': 600}))  # Invalid kwarg names
        for packed_args in args:
            packed_args = packed_args if not unique_kwarg else (packed_args[0], 'a_name', packed_args[1])
            self.assertRaises(errors.PipelineInvalidClassArgumentsError, mtd, *packed_args)
