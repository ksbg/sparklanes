import argparse
import logging
import os
import re
import shutil
import sys
import tempfile
from subprocess import call, STDOUT

from six import PY2


def spark_submit():
    _package_and_submit(sys.argv[1:])


def _package_and_submit(args):
    args = _parse_and_validate_args(args)

    if args['spark_args']:
        args['spark_args'] = __prep_spark_args(spark_args=args['spark_args'])

    logging.debug(args)
    dist = __make_tmp_dir()
    try:
        libs_dir = __install_libs(dist_dir=dist, additional_reqs=args['requirements'], silent=args['silent'])
        __package(tasks_pkg=args['package'],
                  dist_dir=dist,
                  libs_dir=libs_dir,
                  custom_main=args['main'],
                  extra_data=args['extra_data'])
        __call_spark_submit(lane_yaml=args['yaml'], dist_dir=dist, spark_args=args['spark_args'], silent=args['silent'])

    except Exception as e:
        __clean_up(dist)
        raise e
    __clean_up(dist)


def _parse_and_validate_args(args):
    parser = argparse.ArgumentParser(description='Submitting a lane to spark.')
    parser.add_argument('-y', '--yaml', type=str, required=True,
                        help='Path to the yaml definition file.')
    parser.add_argument('-m', '--main', type=str, required=False,
                        help='Path to a custom main python file')
    parser.add_argument('-p', '--package', type=str, required=True,
                        help='Path to the python package containing your tasks.')
    parser.add_argument('-e', '--extra-data', nargs='*', required=False,
                        help='Path to any additional files or directories that should be packaged and sent to Spark.')
    parser.add_argument('-s', '--spark-args', nargs='*', required=False,
                        help='Any additional arguments that should be sent to Spark via spark-_submit.'
                             'e.g. `--spark-args executor-memory=20G total-executor-cores=100`')
    parser.add_argument('-r', '--requirements', type=str, required=False,
                        help='Path to a `requirements.txt` specifying any additional dependencies of your tasks.')
    parser.add_argument('--silent', help='If set, no output will be sent to console',
                        action='store_true')
    args = parser.parse_args(args).__dict__

    def validate_path(p, check_file=True, check_dir=False):
        if not p:
            return p
        else:
            abs_path = os.path.join(os.path.abspath(os.curdir), p) if not os.path.isabs(p) else p
            if not (os.path.isfile(abs_path) if check_file else False) \
                    and not (os.path.isdir(abs_path) if check_dir else False):
                raise SystemExit('`%s` does not exist' % p)

        return abs_path

    # Check/fix files/dirs
    args['package'] = validate_path(args['package'], False, True)
    for p in ('yaml', 'requirements', 'main'):
        args[p] = validate_path(args[p])
    if args['extra_data']:
        for i in range(len(args['extra_data'])):
            args['extra_data'][i] = validate_path(args['extra_data'][i], True, True)
    if PY2 and not os.path.isfile(os.path.join(args['package'], '__init__.py')):
        raise SystemExit('Could not confirm `%s` is a python package. Make sure it contains an `__init__.py`.')

    # Check spark args
    if args['spark_args']:
        pattern = re.compile('[\w\-_]+=.+')
        for sa in args['spark_args']:
            if not pattern.match(sa):
                raise SystemExit('Spark argument `%s` does not seem to be in the correct format `ARG_NAME=ARG_VAL`.')

    return args


def __prep_spark_args(spark_args):
    subcmd = []
    for sa in spark_args:
        sas = sa.split('=')
        sas[0] = '--' + sas[0]
        subcmd += sas

    return subcmd


def __make_tmp_dir():
    tmp_dir = tempfile.mkdtemp()
    logging.debug('Created temporary dir: `%s`' % tmp_dir)

    return tmp_dir


def __install_libs(dist_dir, additional_reqs, silent):
    logging.info('Installing dependencies')
    libs_dir = os.path.join(dist_dir, 'libs')
    if not os.path.isdir(libs_dir):
        os.mkdir(libs_dir)

    # Get requirements
    with open(os.path.join(os.path.dirname(os.path.abspath(__file__)), 'requirements-submit.txt'), 'r') as req:
        requirements = req.read().splitlines()
    if additional_reqs:
        with open(additional_reqs, 'r') as req:
            for row in req:
                requirements.append(row)

    # Remove duplicates
    requirements = list(set(requirements))

    devnull = open(os.devnull, 'w')
    outp = {'stderr': STDOUT, 'stdout': devnull} if silent else {}
    for p in requirements:
        cmd = ['pip', 'install', p, '-t', libs_dir]
        logging.debug('Calling `%s`' % str(cmd))
        call(cmd, **outp)
    devnull.close()

    return libs_dir


def __package(tasks_pkg, dist_dir, libs_dir, custom_main=None, extra_data=None):
    logging.info('Packaging application')
    # Package libs
    shutil.make_archive(os.path.join(dist_dir, 'libs'), 'zip', libs_dir, './')

    # Package tasks
    tasks_dir_splits = os.path.split(os.path.realpath(tasks_pkg))
    shutil.make_archive(os.path.join(dist_dir, 'tasks'),
                        'zip',
                        tasks_dir_splits[0],
                        tasks_dir_splits[1])

    # Package main.py
    if custom_main is None:
        from . import _main
        main_path = _main.__file__
        if main_path[-3:] == 'pyc':
            main_path = main_path[:-1]
        shutil.copy(os.path.realpath(main_path),
                    os.path.join(dist_dir, 'main.py'))
    else:
        shutil.copy(os.path.realpath(custom_main),
                    os.path.join(dist_dir, 'main.py'))

    # Package _framework
    shutil.make_archive(os.path.join(dist_dir, '_framework'),
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


def __call_spark_submit(lane_yaml, dist_dir, spark_args, silent):
    cmd = ['spark-submit']

    # Supplied spark arguments
    if spark_args:
        cmd += spark_args

    # Packaged App & lane
    cmd += ['--py-files', 'libs.zip,_framework.zip,tasks.zip', 'main.py']
    if lane_yaml:
        cmd += ['--lane', lane_yaml]

    logging.info('Submitting to Spark')
    logging.debug(str(cmd))

    # Submit
    devnull = open(os.devnull, 'w')
    outp = {'stderr': STDOUT, 'stdout': devnull} if silent else {}
    call(cmd, cwd=dist_dir, **outp)
    devnull.close()


def __clean_up(dist_dir):
    shutil.rmtree(dist_dir)
