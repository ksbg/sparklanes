import argparse
import logging
import os
import re
import shutil
import tempfile
from subprocess import call

REQUIREMENTS = ['PyYAML==3.12', 'schema==0.6.7', 'tabulate==0.8.2', 'six==1.11.0']
# TODO: make cwd curdir


def submit_to_spark(pipeline, package, extra_data=None, spark_args=None):  # TODO needs more work (>lane)
    if extra_data and not isinstance(extra_data, list):
        raise ValueError('`extra_data` must be of type `list`.')
    if spark_args and not isinstance(spark_args, dict):
        raise ValueError('`spark_args` must be of tyope `dict`.')

    dist = __make_tmp_dir()
    try:
        libs_dir = __install_libs(dist)
        __package(package, dist, libs_dir, extra_data)
        __submit(pipeline, dist, spark_args)
    except Exception as e:
        __clean_up(dist)
        raise e
    __clean_up(dist)


def submit_to_spark_cli():
    args = __parse_and_validate_args()
    if args['spark_args']:
        args['spark_args'] = __prep_spark_args_cli(args['spark_args'])

    logging.debug(args)

    dist = __make_tmp_dir()
    try:
        libs_dir = __install_libs(dist)
        __package(args['package'], dist, libs_dir, args['extra_data'])
        __submit(args['lane'], dist, args['spark_args'])
    except Exception as e:
        __clean_up(dist)
        raise e
    __clean_up(dist)


def __prep_spark_args(spark_args):
    subcmd = []
    for k, v in spark_args:
        subcmd += ['--%s' % k, v]

    return subcmd


def __prep_spark_args_cli(spark_args):
    subcmd = []
    for sa in spark_args:
        sas = sa.split('=')
        sas[0] = '--' + sas[0]
        subcmd += sas

    return subcmd


def __parse_and_validate_args():
    parser = argparse.ArgumentParser(description='Submitting a lane to spark.')
    parser.add_argument('lane', type=str,
                        help='Path to the yaml lane definition file.')
    parser.add_argument('-p', '--package', type=str, required=True,
                        help='Path to the python package containing your processors.')
    parser.add_argument('-e', '--extra-data', nargs='*', required=False,
                        help='Path to any additional files or directories that should be packaged and sent to Spark.')
    parser.add_argument('-s', '--spark-args', nargs='*', required=False,
                        help='Any additional arguments that should be sent to Spark via spark-submit.'
                             'e.g. `--spark-args executor-memory=20G total-executor-cores=100`')

    args = parser.parse_args().__dict__

    def fix_path(p):
        return os.path.join(os.path.abspath(os.curdir), p) if not os.path.isabs(p) else p

    # Validate package
    args['package'] = fix_path(args['package'])
    if not os.path.isdir(args['package']):
        raise ValueError('`%s` is not a directory.' % args['package'])
    if not os.path.isfile(os.path.join(args['package'], '__init__.py')):
        # Even though __init__.py is not required anymore in Python 3.3+, for now it is considered needed
        raise ValueError('Could not confirm `%s` is a python package. Make sure it contains an `__init__.py`.')

    # Validate lane
    if args['lane']:  # Fix file path
        args['lane'] = fix_path(args['lane'])
        if not os.path.isfile(args['lane']):
            raise ValueError('File `%s` does not seem to exist' % args['lane'])

    # Validate optional additional data
    if args['extra_data']:
        for i in range(len(args['extra_data'])):
            args['extra_data'][i] = fix_path(args['extra_data'][i])
            if not os.path.isfile(args['extra_data'][i]) and not os.path.isdir(args['extra_data'][i]):
                raise ValueError('`%s` is neither a directory, nor a file.' % args['extra_data'][i])

    # Check spark args
    if args['spark_args']:
        pattern = re.compile('[\w\-_]+=.+')
        for sa in args['spark_args']:
            if not pattern.match(sa):
                raise ValueError('Spark argument `%s` does not seem to be in the correct format (ARG_NAME=ARG_VAL).')

    return args


def __make_tmp_dir():
    tmp_dir = tempfile.mkdtemp()
    logging.debug('Created temporary dir: `%s`' % tmp_dir)

    return tmp_dir


def __install_libs(dist_dir):
    logging.info('Installing dependencies')
    libs_dir = os.path.join(dist_dir, 'libs')
    if not os.path.isdir(libs_dir):
        os.mkdir(libs_dir)
    for p in REQUIREMENTS:
        cmd = ['pip', 'install', p, '-t', libs_dir]
        logging.debug('Calling `%s`' % str(cmd))
        call(cmd)

    return libs_dir


def __package(procs_pkg, dist_dir, libs_dir, extra_data=None):
    logging.info('Packaging application')
    # Package libs
    shutil.make_archive(os.path.join(dist_dir, 'libs'), 'zip', libs_dir, './')

    # Package processors
    procs_dir_splits = os.path.split(os.path.realpath(procs_pkg))
    print(procs_dir_splits)
    shutil.make_archive(os.path.join(dist_dir, 'procs'),
                        'zip',
                        procs_dir_splits[0],
                        procs_dir_splits[1])

    # Package main.py
    shutil.copy(os.path.join(os.path.dirname(os.path.realpath(__file__)), '..', 'main.py'),
                os.path.join(dist_dir, 'main.py'))

    # Package framework
    shutil.make_archive(os.path.join(dist_dir, 'framework'),
                        'zip',
                        os.path.join(os.path.dirname(os.path.realpath(__file__)), '..', '..'),
                        './sparklanes/')

    # Package extra data
    if extra_data:
        for d in extra_data:
            real_path = os.path.realpath(d)
            target = os.path.join(dist_dir, os.path.split(real_path)[1])
            if os.path.isfile(real_path):
                shutil.copy(real_path, target)
            elif os.path.isdir(real_path):
                shutil.copytree(real_path, target)
            else:
                raise IOError('File `%s` not found at `%s`.' % (d, real_path))


def __submit(pipeline_yaml, dist_dir, spark_args):
    cmd = ['spark-submit']

    # Supplied spark arguments
    if spark_args:
        cmd += spark_args

    # Packaged App & lane
    cmd += ['--py-files', 'libs.zip,framework.zip,procs.zip', 'main.py']
    cmd += ['--lane', pipeline_yaml]

    logging.info('Submitting to Spark')
    logging.debug(str(cmd))

    # Submit
    call(cmd, cwd=dist_dir)


def __clean_up(dist_dir):
    shutil.rmtree(dist_dir)

