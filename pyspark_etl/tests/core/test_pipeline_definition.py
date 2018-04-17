from unittest import TestCase

from core import errors
from core.pipeline import PipelineDefinition
from tests.helpers.yaml_generator import ValidPipelineYAMLDefinitions


class TestPipelineDefinition(TestCase):
    def test_pipeline_definition_valid_build_from_yaml(self):
        """Also calls and tests all validation utils in core.validation"""
        # Test if all valid definitions are recognized as such
        counter = 0
        for yaml_file_stream in ValidPipelineYAMLDefinitions():
            print(counter, yaml_file_stream)
            counter += 1
            try:
                PipelineDefinition().build_from_yaml(yaml_file_stream=yaml_file_stream)
            except (errors.PipelineSchemaError, errors.PipelineInvalidClassError, errors.PipelineModuleNotFoundError,
                    errors.PipelineClassNotFoundError, errors.PipelineInvalidClassArgumentsError) as e:
                self.fail('\nException raised: %s\nMessage: %s\n\n'
                          'Contents of the YAML file which made the tests fail:\n\n%s'
                          % (e.__class__.__module__ + '.' + e.__class__.__name__, str(e), yaml_file_stream.read()))

        # Test if all invalid definitions are recognized as such

    def test_pipeline_definition_invalid_arguments_build_from_yaml(self):
        for yaml_file_stream in PipelineDefinitionFileGenerator(valid_classes=False):
            self.assertRaises(errors.PipelineInvalidClassArgumentsError)