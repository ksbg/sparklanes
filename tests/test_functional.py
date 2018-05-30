"""Executes the 'iris' example lane, and checks if the resulting files match what's expected"""
import logging
import os
import subprocess
import sys
import warnings
from filecmp import cmp
from shutil import rmtree
from tempfile import mkdtemp
from unittest import TestCase

from sparklanes import Lane
from sparklanes._framework.config import VERBOSE_TESTING
from sparklanes._framework.log import make_default_logger
from .helpers.tasks import iris_tasks


class TestIrisLane(TestCase):
    @classmethod
    def setUpClass(cls):
        super(TestIrisLane, cls).setUpClass()
        warnings.simplefilter("ignore")

        cls.tmp_dir1 = mkdtemp()  # Used for test_from_code
        cls.tmp_dir2 = mkdtemp()  # Used for test_from_command_line
        cls.tmp_dir3 = mkdtemp()  # Used for test_from_command_line_with_custom_main

        cls.mdl_dir = os.path.dirname(os.path.abspath(__file__))
        cls.iris_input = os.path.join(cls.mdl_dir, 'helpers', 'data', 'iris_input.csv')
        cls.expected_output = os.path.join(cls.mdl_dir, 'helpers', 'data', 'iris_expected_output.json')

        # Add tasks to path
        sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), 'helpers', 'tasks'))

        # Verbosity
        if not VERBOSE_TESTING:
            logger = make_default_logger('sparklanes')
            logger.handlers[0].setLevel(logging.CRITICAL)
            cls.subprocess_out = {'stderr': subprocess.STDOUT, 'stdout': open(os.devnull, 'wb')}
        else:
            cls.subprocess_out = {}

        # Install sparklanes package to make sure the command line script is available
        subprocess.call([sys.executable, "-m", "pip", "install", os.path.join(cls.mdl_dir, '..')],
                        **cls.subprocess_out)

    def __find_iris_output_json(self, out_dir):
        out_file = None
        for f in os.listdir(out_dir):
            if f[-5:] == '.json':
                out_file = os.path.join(out_dir, f)
                break
        if not out_file:
            print(out_dir)
            self.fail('Could not find the iris lane\'s output file')

        return out_file

    def __prepare_iris_tmp_dir(self, tmp_dir):
        # Insert the location of the temporary folder into the YAML file
        new_yml_path = os.path.join(tmp_dir, 'iris.yml')
        out_dir = os.path.join(tmp_dir, 'out')
        package_dir = os.path.join(self.mdl_dir, 'helpers', 'tasks')
        data_dir = os.path.join(self.mdl_dir, 'helpers', 'data')

        with open(os.path.join(self.mdl_dir, 'helpers', 'yml', 'iris.yml'), 'r') as iris_yml_stream:
            with open(new_yml_path, 'w') as new_yml_stream:
                new_yml_stream.write(iris_yml_stream.read() % (self.iris_input, out_dir))

        return new_yml_path, package_dir, data_dir, out_dir

    def test_from_code(self):
        out_dir = os.path.join(self.tmp_dir1, 'out')

        lane = (Lane(name='IrisExamplePane')
                .add(iris_tasks.ExtractIrisCSVData, iris_csv_path=self.iris_input)
                .add(iris_tasks.AddRowIndex)
                .add(iris_tasks.NormalizeColumns)
                .add(iris_tasks.SaveAsJSON, out_dir))
        lane.run()

        out_file = self.__find_iris_output_json(out_dir)

        self.assertEqual(cmp(self.expected_output, out_file), True)

    def test_from_command_line(self):
        new_yml_path, package_dir, data_dir, out_dir = self.__prepare_iris_tmp_dir(self.tmp_dir2)

        # Execute command

        subprocess.check_call(['lane-submit',
                               '--yaml', new_yml_path,
                               '--package', package_dir,
                               '--extra-data', data_dir],
                              **self.subprocess_out)

        # Find output file
        out_file = self.__find_iris_output_json(out_dir)

        self.assertEqual(cmp(self.expected_output, out_file), True)

    def test_from_command_line_with_custom_main(self):
        custom_main = ["from argparse import ArgumentParser",
                       "from sparklanes import build_lane_from_yaml",
                       "",
                       "parser = ArgumentParser()",
                       "parser.add_argument('-l', '--lane', required=True)",
                       "lane = parser.parse_args().__dict__['lane']",
                       "build_lane_from_yaml(lane).run()"]

        main_path = os.path.join(self.tmp_dir3, 'custom_main.py')
        new_yml_path, package_dir, data_dir, out_dir = self.__prepare_iris_tmp_dir(self.tmp_dir3)

        with open(main_path, 'w') as main_file_stream:
            main_file_stream.write('\n'.join(custom_main))

        with open(os.path.join(self.mdl_dir, 'helpers', 'yml', 'iris.yml'), 'r') as iris_yml_stream:
            with open(new_yml_path, 'w') as new_yml_stream:
                new_yml_stream.write(iris_yml_stream.read() % (self.iris_input, out_dir))

        # Execute command
        with open(os.devnull, 'wb') as devnull:
            subprocess.check_call(['lane-submit',
                                   '-y', new_yml_path,
                                   '-p', package_dir,
                                   '-e', data_dir,
                                   '-m', main_path],
                                  **self.subprocess_out)

        out_file = self.__find_iris_output_json(out_dir)

        self.assertEqual(cmp(self.expected_output, out_file), True)

    @classmethod
    def tearDownClass(cls):
        rmtree(cls.tmp_dir1)
        rmtree(cls.tmp_dir2)
        rmtree(cls.tmp_dir3)

        # Set logging verbosity back
        if not VERBOSE_TESTING:
            logger = make_default_logger('sparklanes')
            logger.handlers[0].setLevel(logging.INFO)
            cls.subprocess_out['stdout'].close()
