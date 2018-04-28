import os
import subprocess
import tempfile
from filecmp import cmp
from unittest import TestCase

from core.pipeline import PipelineDefinition, Pipeline
from tests.helpers import processes


class TestFunctionalExtractTransformLoadCSV(TestCase):
    def test_csv_example_pipeline_defined_manually(self):
        """
        Checks the behavior of the entire pipeline. The following pipeline as defined in helpers/func_pl.yaml will be used:
        Extract: Load a csv file from disk as a spark dataframe
        Transform: Convert the data frame to a list
        Transform: Multiply all numbers in the list by two
        Load: Dump the results to a new csv file
        """
        cur_dir = os.path.dirname(os.path.realpath(__file__))
        input_csv = os.path.join(cur_dir, 'helpers/res/func_pl_data.csv')
        _, output_csv = tempfile.mkstemp()
        output_csv = os.path.join(cur_dir, 'helpers/res/func_pl_data_out.csv')
        expected_csv = os.path.join(cur_dir, 'helpers/res/func_pl_data_expected.csv')

        pd = PipelineDefinition()
        pd.add_extractor(cls=processes.ProcessExtractIntsFromCSV,
                         data_frame_name='ints_df',
                         kwargs={'csv_path': input_csv})
        pd.add_transformer(processes.ProcessTransformConvertDataFrameToList,
                           kwargs={'output_shared_list_name': 'ints_df_as_list'})
        pd.add_transformer(processes.ProcessTransformMultiplyIntsInSharedListByTwo,
                           kwargs={'list_name': 'ints_df_as_list'})
        pd.add_loader(processes.ProcessLoadDumpResultListToCSV,
                      kwargs={'list_name': 'ints_df_as_list', 'output_file_name': 'res/func_pl_data_out.csv'})
        pipeline = Pipeline(definition=pd)
        pipeline.run()

        # Test
        self.assertEqual(cmp(output_csv, expected_csv), True)

        # Clean up
        try:
            os.remove(output_csv)
        except OSError:
            pass

    def test_iris_example_pipeline_defined_from_yaml(self):
        """
        Runs the iris example pipeline and checks if the output is as expected
        """
        yaml_file = 'examples/iris.yaml'  # Relative from project root
        out_path = os.path.join(os.path.dirname(os.path.realpath(__file__)), '../../dist/out/')
        expected_out_file = os.path.join(os.path.dirname(os.path.realpath(__file__)), 'helpers/res/iris_expected.json')

        with open(os.devnull, 'wb') as devnull:
            subprocess.check_call(['make', 'build', 'submit', yaml_file],
                                  stdout=devnull,
                                  stderr=subprocess.STDOUT,
                                  cwd=os.path.abspath('..'))

        out_files = os.listdir(out_path)
        out_file = None
        for o_f in out_files:
            if o_f[-4:] == 'json':
                out_file = o_f
                break

        # Check if file has been found
        if out_file is None:
            self.fail('Iris output JSON file was not found.')

        # Check if file looks as expected
        self.assertEqual(cmp(os.path.join(out_path, out_file), expected_out_file), True)
