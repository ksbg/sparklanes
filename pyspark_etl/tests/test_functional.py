import os
from unittest import TestCase
from core.pipeline import PipelineDefinition, Pipeline
import csv


class TestFunctionalExtractTransformLoadCSV(TestCase):
    """
    Checks the behavior of the entire pipeline. The following pipeline as defined in helpers/func_pl.yaml will be used:
    Extract: Load a csv file from disk as a spark dataframe
    Transform: Convert the data frame to a list
    Transform: Multiply all numbers in the list by two
    Load: Dump the results to a new csv file
    """
    def test_pipeline(self):
        yaml_file = os.path.join(os.path.dirname(os.path.realpath(__file__)), 'helpers/func_pl.yaml')
        input_csv = os.path.join(os.path.dirname(os.path.realpath(__file__)), 'helpers/func_pl_data.csv')
        output_csv = os.path.join(os.path.dirname(os.path.realpath(__file__)), 'helpers/func_pl_data_out.csv')

        # Read input CSV
        with open(input_csv) as input_file:
            reader = csv.DictReader(input_file)
            input_data = []
            for row in reader:
                input_data.append(int(row['number']))
        expected_result = [i * 2 for i in input_data]

        pd = PipelineDefinition()
        with open(yaml_file) as pl_file:
            pd.build_from_yaml(pl_file)

        pipeline = Pipeline(definition=pd)

        pipeline.run()

        # Read output csv
        with open(output_csv) as output_file:
            reader = csv.DictReader(output_file)
            output_data = []
            for row in reader:
                output_data.append(int(row['number']))

        # Test
        self.assertEqual(output_data, expected_result)

        # Clean up
        try:
            os.remove(output_csv)
        except OSError:
            pass
