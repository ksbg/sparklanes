from unittest import TestCase

from core import errors
from core.pipeline import PipelineDefinition
from tests.helpers import processes


class TestPipelineDefinition(TestCase):
    def setUp(self):
        self.example_definition = PipelineDefinition()

    def test_build_from_yaml(self):
        # Test argument type
        invalid_arg_types = [1, '1', [1], {'1': 1}, TestCase, self]
        for arg in invalid_arg_types:
            self.assertRaises(TypeError, PipelineDefinition().build_from_yaml, arg)

    def test_build_from_dict(self):
        # Invalid argument types
        args = ([1, 2, 3],
                '1',
                True,
                self,
                {},
                None)
        for arg in args:
            self.assertRaises(errors.PipelineSchemaError, self.example_definition.build_from_dict, arg)

    def test_add_extractor(self):
        self.__test_extractor_transformer_loader(self.example_definition.add_extractor, 'extract')

    def test_add_transformer(self):
        self.__test_extractor_transformer_loader(self.example_definition.add_transformer, 'transform')

    def test_add_loader(self):
        self.__test_extractor_transformer_loader(self.example_definition.add_loader, 'load')

    def __test_extractor_transformer_loader(self, mtd, def_type):
        """All three methods (add_transfomer, add_extractor, add_loader) are basically the same, so their tests can be
        wrapped."""
        # Invalid class (not inherited from base processor class)
        args = ((TestCase, None),
                (processes.ProcessNotInherited, None))
        for arg1, arg2 in args:
            self.assertRaises(errors.PipelineInvalidClassError, mtd, arg1, arg2)

        # Invalid argument types
        args = ((1, None),
                ('2', None),
                ([1, 2, 3], None),
                (None, None),
                (processes.ProcessWithOnePositionalArg, [1, 2, 3]),
                (processes.ProcessWithOnePositionalArg, 1),
                (processes.ProcessWithOnePositionalArg, '1'))
        for arg1, arg2 in args:
            self.assertRaises((AttributeError, TypeError), mtd, arg1, arg2)

        # Invalid kwargs
        args = ((processes.ProcessWithOnePositionalArg, {'a': 100, 'b': 200}),  # Too many kwargs
                (processes.ProcessWithoutArgs, {'a': 100}),  # Too many kwargs
                (processes.ProcessWithThreeOptionalArgs, {'a': 100, 'b': 200, 'c': 300, 'd': 400}),  # Too many kwargs
                (processes.ProcessWithOnePositionalArg, None),  # Too few kwargs
                (processes.ProcessWithOnePositionalAndTwoOptionalArgs, {}),  # Too few kwargs
                (processes.ProcessWithThreePositionalArgs, {'a': 100, 'b': 200}),  # Too few kwargs
                (processes.ProcessWithThreeOptionalArgs, {'d': 400, 'e': 500, 'f': 600}),  # Invalid kwarg names
                (processes.ProcessWithOnePositionalAndTwoOptionalArgs, {'d': 400, 'e': 500, 'f': 600}),  # ""
                (processes.ProcessWithOnePositionalArg, {'d': 400, 'e': 500, 'f': 600}))  # Invalid kwarg names
        for arg1, arg2 in args:
            self.assertRaises(errors.PipelineInvalidClassArgumentsError, mtd, arg1, arg2)


class TestPipeline(TestCase):
    def setUp(self):
        # Init a few pipeline definitions
        pass
