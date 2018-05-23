import warnings
from unittest import TestCase

import yaml
from pyspark import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import rand
from six import PY3

from pysparketl.etl import errors
from pysparketl.etl.pipeline import PipelineDefinition, Pipeline
from pysparketl.etl.shared import Shared
from pysparketl.tests.helpers import processors
from pysparketl.tests.helpers.yaml_generator import ValidPipelineYAMLDefinitions


class TestFromYAMLToPipeline(TestCase):
    def setUp(self):
        if PY3:
            warnings.simplefilter('ignore', ResourceWarning)
        self.counter = 0
        self.sc = SparkContext.getOrCreate()
        self.sc.setLogLevel('ERROR')

    def test_integration_from_yaml_to_pipeline(self):
        """
        Tests an (almost) exhaustive list of possible YAML pipeline definitions. Uses the ValidPipelineYAMLDefinitions
        helper, which generates combinations of possible definition styles and provides an iterator to create the
        YAML files and passes the open file stream for further processing. Steps:
        1. Read YAML from file into dictionary
        2. Build the pipeline definition from dictionary
        3. Build the pipeline from the pipeline definition
        4. Check if the pipeline definition contains the same data as defined in the YAML file, and if the pipeline
           contains the same data defined in the pipeline definition.
        5. Run the pipeline
        """
        valid_definitions = ValidPipelineYAMLDefinitions()
        for yaml_file_stream in valid_definitions:
            Shared.delete_all()
            self.counter += 1
            try:
                # Build definition
                pd_dict = yaml.load(yaml_file_stream)
                yaml_file_stream.close()
                pd = PipelineDefinition()
                pd.build_from_dict(pd_dict)

                # Build pipeline from definition
                pipeline = Pipeline(definition=pd, sc=self.sc)

                # Check if built correctly
                self.__check_definition_and_pipeline(pd_dict=pd_dict, pd=pd, pipeline=pipeline)

                # Run the pipeline
                pipeline.run()
            except Exception as e:
                self.fail('\nException raised: %s\nMessage: %s\n\nTested YAML file: `%s`' %
                          (e.__class__.__module__ + '.' + e.__class__.__name__, str(e), yaml_file_stream.name))
            if self.counter % 1000 == 0:
                print('Checked %s/%s definitions for integration test' % (self.counter, valid_definitions.iter_len))

    def __check_definition_and_pipeline(self, pd_dict, pd, pipeline):
        """Check if the pipeline definition contains the same data as defined in the YAML file, and if the pipeline
           contains the same data defined in the pipeline definition. """
        for def_type, p_processes in pipeline.processes.items():
            for i in range(len(p_processes)):
                # Turn single non-list processors into a list (to access it more easily)
                if isinstance(pd_dict['processors'][def_type], dict):
                    dict_processes = [pd_dict['processors'][def_type]]
                else:
                    dict_processes = pd_dict['processors'][def_type]

                # Check if grabbed class and kwargs in definition are the same is in YAML
                self.assertEqual('%s.%s' % (pd.processes[def_type][i]['class'].__module__,
                                            pd.processes[def_type][i]['class'].__name__),
                                 'pysparketl.' + dict_processes[i]['class'])
                if 'kwargs' in dict_processes[i].keys() and dict_processes[i]['kwargs']:
                    self.assertEqual(pd.processes[def_type][i]['kwargs'], dict_processes[i]['kwargs'])

                # Check if built pipeline has the same class and kwargs as definition
                self.assertEqual(p_processes[i]['class'], pd.processes[def_type][i]['class'])
                self.assertEqual(p_processes[i]['kwargs'], pd.processes[def_type][i]['kwargs'])

    def tearDown(self):
        self.sc.stop()


class TestSharedObjectPassingBetweenProcesses(TestCase):
    """Checks if shared resources, data frames and RDDs are shared correctly between processors"""

    def setUp(self):
        # Init spark
        self.sc = SparkContext.getOrCreate()
        self.sc.setLogLevel('ERROR')
        self.ss = SparkSession.Builder().appName('pyspark-etl').getOrCreate()

    def test_integration_passing_of_shared_resources(self):
        res_name = 'int_list'

        # Run process that checks for non-existing resource
        proc = processors.ProcessorCheckIfSharedObjectExists(name=res_name, res_type='shared')
        self.assertRaises(errors.PipelineSharedResourceNotFound, proc.run)

        # Add a list of integers to shared resources
        res = [100, 200, 300, 400, 500]
        proc = processors.ProcessorAddSharedObject(name=res_name, res_type='shared', o=res)
        proc.run()

        # Run process that checks for non-existing resource again (should not throw exception now)
        proc = processors.ProcessorCheckIfSharedObjectExists(name=res_name, res_type='shared')
        try:
            proc.run()
        except Exception as e:
            self.fail('\nException raised: %s\nMessage: %s' % (e.__class__.__module__ + '.' + e.__class__.__name__,
                                                               str(e)))

        # Run process which multiplies all numbers in the list by 2 (run two times)
        proc = processors.ProcessorMultiplyIntsInSharedListByTwo(resource_name=res_name)
        proc.run()
        proc.run()

        # Check if shared resource has been updated correctly
        self.assertEqual(Shared.get_resource(res_name), [400, 800, 1200, 1600, 2000])

        # Delete the shared resource
        proc = processors.ProcessorDeleteSharedObject(name=res_name, res_type='shared')
        proc.run()

        # Run process that checks for non-existing resource (should throw exception again)
        proc = processors.ProcessorCheckIfSharedObjectExists(name=res_name, res_type='shared')
        self.assertRaises(errors.PipelineSharedResourceNotFound, proc.run)

    def test_integration_passing_of_shared_data_frames(self):
        df_name = 'abc'

        # Run process that checks for non-existing resource
        proc = processors.ProcessorCheckIfSharedObjectExists(name=df_name, res_type='data_frame')
        self.assertRaises(errors.PipelineSharedResourceNotFound, proc.run)

        # Generate data frame with 10 rows (an index and a column with random numbers, rand_a and rand_b)
        df = self.ss.range(10)
        df = df.select("id", rand(seed=10).alias('random'))

        # Store state of data frame as python list
        rand_as_list = [i.random for i in df.collect()]

        # Add to Shared
        proc = processors.ProcessorAddSharedObject(name=df_name, res_type='data_frame', o=df)
        proc.run()

        # Run process that checks for non-existing resource again (should not throw exception now)
        proc = processors.ProcessorCheckIfSharedObjectExists(name=df_name, res_type='data_frame')
        try:
            proc.run()
        except Exception as e:
            self.fail('\nException raised: %s\nMessage: %s' % (e.__class__.__module__ + '.' + e.__class__.__name__,
                                                               str(e)))

        # Run process which adds a column (which is the 'random' column multiplied by 10)
        proc = processors.ProcessorAddColumnToDataFrameFromRandomColumn(df_name=df_name, multiply_by=10,
                                                                        col_name='random10')
        proc.run()
        # And another one (random * 100)
        proc = processors.ProcessorAddColumnToDataFrameFromRandomColumn(df_name=df_name, multiply_by=100,
                                                                        col_name='random100')
        proc.run()

        # Get updated data frame
        df = Shared.get_data_frame(df_name)

        # Check if results are as expected
        for col_name, expected_vals in zip(('random', 'random10', 'random100'),
                                           [rand_as_list, [i * 10 for i in rand_as_list],
                                            [i * 100 for i in rand_as_list]]):
            self.assertEqual([i[col_name] for i in df.collect()], expected_vals)

        # Delete data frame
        proc = processors.ProcessorDeleteSharedObject(name=df_name, res_type='data_frame')
        proc.run()

        # Run process that checks for non-existing resource (should throw exception again)
        proc = processors.ProcessorCheckIfSharedObjectExists(name=df_name, res_type='data_frame')
        self.assertRaises(errors.PipelineSharedResourceNotFound, proc.run)
