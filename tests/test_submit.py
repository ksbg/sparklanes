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

import sparklanes
from sparklanes import Lane
from sparklanes._framework.env import VERBOSE_TESTING, INTERNAL_LOGGER_NAME
from sparklanes._framework.log import make_default_logger
from sparklanes._submit.submit import _package_and_submit, _parse_and_validate_args
from .helpers.tasks import iris_tasks


class TestSparkSubmit(TestCase):
    @classmethod
    def setUpClass(cls):
        super(TestSparkSubmit, cls).setUpClass()
        warnings.simplefilter("ignore")

        cls.tmp_dirs = []
        cls.mdl_dir = os.path.dirname(os.path.abspath(__file__))
        cls.iris_input = os.path.join(cls.mdl_dir, 'helpers', 'data', 'iris_input.csv')
        cls.expected_output = os.path.join(cls.mdl_dir, 'helpers', 'data',
                                           'iris_expected_output.json')

        # Custom main.py
        cls.custom_main = ["from argparse import ArgumentParser",
                           "from sparklanes import build_lane_from_yaml",
                           "",
                           "parser = ArgumentParser()",
                           "parser.add_argument('-l', '--lane', required=True)",
                           "lane = parser.parse_args().__dict__['lane']",
                           "build_lane_from_yaml(lane).run()"]

        # Add tasks to path
        sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)),
                                        'helpers', 'tasks'))

        # Verbosity
        if not VERBOSE_TESTING:
            logger = make_default_logger(INTERNAL_LOGGER_NAME)
            logger.handlers[0].setLevel(logging.CRITICAL)
            cls.devnull = open(os.devnull, 'w')
            sys.stdout = cls.devnull
            sys.stderr = cls.devnull
            cls.subprocess_out = {'stderr': subprocess.STDOUT, 'stdout': cls.devnull}
        else:
            cls.subprocess_out = {}

        # Install sparklanes package to make sure the command line script is available
        subprocess.call([sys.executable, "-m", "pip", "install", '--upgrade', '--force-reinstall',
                         os.path.join(cls.mdl_dir, '..')], **cls.subprocess_out)

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
        tmp_dir = mkdtemp()
        self.tmp_dirs.append(tmp_dir)
        out_dir = os.path.join(tmp_dir, 'out')

        lane = (Lane(name='IrisExamplePane')
                .add(iris_tasks.ExtractIrisCSVData, iris_csv_path=self.iris_input)
                .add(iris_tasks.AddRowIndex)
                .add(iris_tasks.NormalizeColumns)
                .add(iris_tasks.SaveAsJSON, out_dir))
        lane.run()

        out_file = self.__find_iris_output_json(out_dir)

        self.assertEqual(cmp(self.expected_output, out_file), True)

    def test_submit(self):
        tmp_dir = mkdtemp()
        self.tmp_dirs.append(tmp_dir)
        new_yml_path, package_dir, data_dir, out_dir = self.__prepare_iris_tmp_dir(tmp_dir)

        # Call submit
        args = ['--yaml', new_yml_path,
                '--package', package_dir,
                '--extra-data', data_dir,
                '--spark-args', 'deploy-mode=client']
        if not VERBOSE_TESTING:
            args += ['--silent']
        _package_and_submit(args)

        # Find output file
        out_file = self.__find_iris_output_json(out_dir)
        self.assertEqual(cmp(self.expected_output, out_file), True)

    def test_submit_with_custom_main(self):
        tmp_dir = mkdtemp()
        self.tmp_dirs.append(tmp_dir)
        main_path = os.path.join(tmp_dir, 'custom_main.py')
        new_yml_path, package_dir, data_dir, out_dir = self.__prepare_iris_tmp_dir(tmp_dir)

        with open(main_path, 'w') as main_file_stream:
            main_file_stream.write('\n'.join(self.custom_main))

        with open(os.path.join(self.mdl_dir, 'helpers', 'yml', 'iris.yml'), 'r') as iris_yml_stream:
            with open(new_yml_path, 'w') as new_yml_stream:
                new_yml_stream.write(iris_yml_stream.read() % (self.iris_input, out_dir))

        args = ['-y', new_yml_path,
                '-p', package_dir,
                '-e', data_dir,
                '-m', main_path,
                '-s', 'deploy-mode=client']
        if not VERBOSE_TESTING:
            args += ['--silent']
        _package_and_submit(args)

        # Compare output
        out_file = self.__find_iris_output_json(out_dir)
        self.assertEqual(cmp(self.expected_output, out_file), True)

    def test_command_line_args(self):
        # Valid args
        cur_dir = os.path.dirname(os.path.realpath(__file__))
        iris_yml = os.path.join(cur_dir, 'helpers', 'yml', 'iris.yml')
        data_dir = os.path.join(cur_dir, 'helpers')  # Just a random dir
        pkg_dir = os.path.join(cur_dir, 'helpers', 'tasks')
        custom_main = os.path.join(cur_dir, 'test_lane.py')  # Just a random python file
        requirements_txt = os.path.join(os.path.dirname(sparklanes.__file__),
                                        '_submit', 'requirements-submit.txt')
        spark_args = ['master=spark://127.0.0.1:7077', 'executor-memory=20G', 'deploy-mode=client',
                      'verbose', 'supervised']
        valid_args = [
            ['-y', iris_yml, '-p', pkg_dir],
            ['--yaml', iris_yml, '--package', pkg_dir],
            ['-y', iris_yml, '--package', pkg_dir, '--requirements', requirements_txt],
            ['--yaml', iris_yml, '-p', pkg_dir, '-r', requirements_txt, '-m', custom_main],
            ['-y', iris_yml, '-p', pkg_dir, '-r', requirements_txt, '-m', custom_main,
             '--extra-data', data_dir],
            ['--yaml', iris_yml, '-p', pkg_dir, '-e', data_dir, '-s'] + spark_args,
            ['-y', iris_yml, '--package', pkg_dir, '--requirements', requirements_txt,
             '--spark-args'] + spark_args
        ]

        # Invalid args
        invalid_args = [
            [],  # Empty
            ['-y', iris_yml],  # Missing package
            ['-p', pkg_dir],  # Missing YAML
            ['-y', iris_yml, '-p', pkg_dir, '-r'],  # Missing value after argument name
            ['-y', 'A', '-p', pkg_dir],  # Non-existent YAML
            ['-y', iris_yml, '--package', 'A'],  # Non-existent package
            ['-y', iris_yml, '-p', pkg_dir, '-r', 'A'],  # Non-existant requirements.txt
            ['-y', iris_yml, '-p', pkg_dir, '-r', requirements_txt, '-m', 'A'],  # n.e. custom main
            ['-y', iris_yml, '-p', pkg_dir, '-e', 'A'],  # Non-existent extra data
            ['-y', iris_yml, '-p', pkg_dir, '-s', 'A B C'],  # Invalid spark-args format
        ]

        for args in valid_args:
            try:
                _parse_and_validate_args(args)
            except SystemExit:
                self.fail('Command line parsing should not have failed for the following args: '
                          '`%s`' % str(args))

        for args in invalid_args:
            self.assertRaises(SystemExit, _parse_and_validate_args, args)

        sys.stderr = sys.__stderr__

    def test_command_line_script(self):
        tmp_dir = mkdtemp()
        self.tmp_dirs.append(tmp_dir)
        new_yml_path, package_dir, data_dir, out_dir = self.__prepare_iris_tmp_dir(tmp_dir)

        # Execute command
        subprocess.check_call(['lane-submit',
                               '--yaml', new_yml_path,
                               '--package', package_dir,
                               '--extra-data', data_dir,
                               '--spark-args', 'deploy-mode=client'],
                              **self.subprocess_out)

        # Compare output
        out_file = self.__find_iris_output_json(out_dir)
        self.assertEqual(cmp(self.expected_output, out_file), True)

    def test_command_line_script_with_custom_main(self):
        tmp_dir = mkdtemp()
        self.tmp_dirs.append(tmp_dir)
        main_path = os.path.join(tmp_dir, 'custom_main.py')
        new_yml_path, package_dir, data_dir, out_dir = self.__prepare_iris_tmp_dir(tmp_dir)

        with open(main_path, 'w') as main_file_stream:
            main_file_stream.write('\n'.join(self.custom_main))

        with open(os.path.join(self.mdl_dir, 'helpers', 'yml', 'iris.yml'), 'r') as iris_yml_stream:
            with open(new_yml_path, 'w') as new_yml_stream:
                new_yml_stream.write(iris_yml_stream.read() % (self.iris_input, out_dir))

        subprocess.check_call(['lane-submit',
                               '-y', new_yml_path,
                               '-p', package_dir,
                               '-e', data_dir,
                               '-m', main_path,
                               '-s', 'deploy-mode=client'],
                              **self.subprocess_out)

        # Compare output
        out_file = self.__find_iris_output_json(out_dir)
        self.assertEqual(cmp(self.expected_output, out_file), True)

    @classmethod
    def tearDownClass(cls):
        for tmp_dir in cls.tmp_dirs:
            rmtree(tmp_dir)

        # Set logging verbosity back
        if not VERBOSE_TESTING:
            logger = make_default_logger(INTERNAL_LOGGER_NAME)
            logger.handlers[0].setLevel(logging.INFO)
            sys.stdout = sys.__stdout__
            sys.stderr = sys.__stderr__
            cls.devnull.close()
