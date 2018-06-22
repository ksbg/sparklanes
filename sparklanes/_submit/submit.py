"""Module that allows submitting lanes to spark using YAML definitions"""
import argparse
import logging
import os
import re
import shutil
import sys
import tempfile
from subprocess import call, STDOUT

SPARK_SUBMIT_FLAGS = ['verbose', 'supervised']
MY_ENV = os.environ.copy()

def submit_to_spark():
    """Console-script entry point"""
    _package_and_submit(sys.argv[1:])


def _package_and_submit(args):
    """
    Packages and submits a job, which is defined in a YAML file, to Spark.

    Parameters
    ----------
    args (List): Command-line arguments
    """
    args = _parse_and_validate_args(args)

    logging.debug(args)
    dist = __make_tmp_dir()
    try:
        __package_dependencies(dist_dir=dist, additional_reqs=args['requirements'],
                               silent=args['silent'])
        __package_app(tasks_pkg=args['package'],
                      dist_dir=dist,
                      custom_main=args['main'],
                      extra_data=args['extra_data'])
        __run_spark_submit(lane_yaml=args['yaml'],
                           dist_dir=dist,
                           spark_home=args['spark_home'],
                           spark_args=args['spark_args'],
                           silent=args['silent'])

    except Exception as exc:
        __clean_up(dist)
        raise exc
    __clean_up(dist)


def _parse_and_validate_args(args):
    """
    Parse and validate arguments. During validation, it is checked whether the given
    files/directories exist, while also converting relative paths to absolute ones.

    Parameters
    ----------
    args (List): Command-line arguments
    """
    class ExtendAction(argparse.Action):
        def __call__(self, parser, namespace, values, option_string=None):
            if getattr(namespace, self.dest, None) is None:
                setattr(namespace, self.dest, [])
            getattr(namespace, self.dest).extend(values)

    parser = argparse.ArgumentParser(description='Submitting a lane to spark.')
    parser.add_argument('-y', '--yaml', type=str, required=True,
                        help='Path to the yaml definition file.')
    parser.add_argument('-p', '--package', type=str, required=True,
                        help='Path to the python package containing your tasks.')
    parser.add_argument('-r', '--requirements', type=str, required=False,
                        help='Path to a `requirements.txt` specifying any additional dependencies '
                             'of your tasks.')
    parser.add_argument('-e', '--extra-data', nargs='*', required=False, action=ExtendAction,
                        help='Path to any additional files or directories that should be packaged '
                             'and sent to Spark.')
    parser.add_argument('-m', '--main', type=str, required=False,
                        help='Path to a custom main python file')
    parser.add_argument('-d', '--spark-home', type=str, required=False,
                        help='Custom path to the directory containing your Spark installation. If '
                             'none is given, sparklanes will try to use the `spark-submit` command '
                             'from your PATH')
    parser.add_argument('-s', '--spark-args', nargs='*', required=False,
                        help='Any additional arguments that should be sent to Spark via '
                             'spark-submit. '
                             '(e.g. `--spark-args executor-memory=20G total-executor-cores=100`)')
    parser.add_argument('--silent', help='If set, no output will be sent to console',
                        action='store_true')
    args = parser.parse_args(args).__dict__

    # Check/fix files/dirs
    for param in ('package', 'spark_home'):
        args[param] = __validate_and_fix_path(args[param], check_dir=True)
    for param in ('yaml', 'requirements', 'main'):
        args[param] = __validate_and_fix_path(args[param], check_file=True)
    if args['extra_data']:
        for i in range(len(args['extra_data'])):
            args['extra_data'][i] = __validate_and_fix_path(args['extra_data'][i],
                                                            check_file=True, check_dir=True)

    # Check if python package
    if not os.path.isfile(os.path.join(args['package'], '__init__.py')):
        raise SystemExit('Could not confirm `%s` is a python package. Make sure it contains an '
                         '`__init__.py`.')

    # Check/fix spark args
    if args['spark_args']:
        args['spark_args'] = __validate_and_fix_spark_args(args['spark_args'])

    return args


def __validate_and_fix_path(path, check_file=False, check_dir=False):
    """Check if a file/directory exists and converts relative paths to absolute ones"""
    # pylint: disable=superfluous-parens
    if path is None:
        return path
    else:
        if not (os.path.isfile(path) if check_file else False) \
                and not (os.path.isdir(path) if check_dir else False):
            raise SystemExit('Path `%s` does not exist' % path)
        if not os.path.isabs(path):
            path = os.path.abspath(os.path.join(os.path.abspath(os.curdir), path))

    return path


def __validate_and_fix_spark_args(spark_args):
    """
    Prepares spark arguments. In the command-line script, they are passed as for example
    `-s master=local[4] deploy-mode=client verbose`, which would be passed to spark-submit as
    `--master local[4] --deploy-mode client --verbose`

    Parameters
    ----------
    spark_args (List): List of spark arguments

    Returns
    -------
    fixed_args (List): List of fixed and validated spark arguments
    """
    pattern = re.compile(r'[\w\-_]+=.+')
    fixed_args = []
    for arg in spark_args:
        if arg not in SPARK_SUBMIT_FLAGS:
            if not pattern.match(arg):
                raise SystemExit('Spark argument `%s` does not seem to be in the correct format '
                                 '`ARG_NAME=ARG_VAL`, and is also not recognized to be one of the'
                                 'valid spark-submit flags (%s).' % (arg, str(SPARK_SUBMIT_FLAGS)))
            eq_pos = arg.find('=')
            fixed_args.append('--' + arg[:eq_pos])
            fixed_args.append(arg[eq_pos + 1:])
        else:
            fixed_args.append('--' + arg)

    return fixed_args


def __make_tmp_dir():
    """
    Create a temporary directory where the packaged files will be located

    Returns
    -------
    tmp_dir (str): Absolute path to temporary directory
    """
    tmp_dir = tempfile.mkdtemp()
    logging.debug('Created temporary dir: `%s`', tmp_dir)

    return tmp_dir


def __package_dependencies(dist_dir, additional_reqs, silent):
    """
    Installs the app's dependencies from pip and packages them (as zip), to be submitted to spark.

    Parameters
    ----------
    dist_dir (str): Path to directory where the packaged libs shall be located
    additional_reqs (str): Path to a requirements.txt, containing any of the app's additional
        requirements
    silent (bool): Flag indicating whether pip output should be printed to console
    """
    logging.info('Packaging dependencies')
    libs_dir = os.path.join(dist_dir, 'libs')
    if not os.path.isdir(libs_dir):
        os.mkdir(libs_dir)

    # Get requirements
    req_txt = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'requirements-submit.txt')
    with open(req_txt, 'r') as req:
        requirements = req.read().splitlines()
    if additional_reqs:
        with open(additional_reqs, 'r') as req:
            for row in req:
                requirements.append(row)

    # Remove duplicates
    requirements = list(set(requirements))

    # Install
    devnull = open(os.devnull, 'w')
    outp = {'stderr': STDOUT, 'stdout': devnull} if silent else {}
    for pkg in requirements:
        cmd = ['pip', 'install', pkg, '-t', libs_dir]
        logging.debug('Calling `%s`', str(cmd))
        call(cmd, **outp)
    devnull.close()

    # Package
    shutil.make_archive(libs_dir, 'zip', libs_dir, './')


def __package_app(tasks_pkg, dist_dir, custom_main=None, extra_data=None):
    """
    Packages the `tasks_pkg` (as zip) to `dist_dir`. Also copies the 'main' python file to
    `dist_dir`, to be submitted to spark. Same for `extra_data`.

    Parameters
    ----------
    tasks_pkg (str): Path to the python package containing tasks
    dist_dir (str): Path to the directory where the packaged code should be stored
    custom_main (str): Path to a custom 'main' python file.
    extra_data (List[str]): List containing paths to files/directories that should also be packaged
        and submitted to spark
    """
    logging.info('Packaging application')

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
        for dat in extra_data:
            real_path = os.path.realpath(dat)
            target = os.path.join(dist_dir, os.path.split(real_path)[1])
            if os.path.isfile(real_path):
                shutil.copy(real_path, target)
            elif os.path.isdir(real_path):
                shutil.copytree(real_path, target)
            else:
                raise IOError('File `%s` not found at `%s`.' % (dat, real_path))


def __run_spark_submit(lane_yaml, dist_dir, spark_home, spark_args, silent):
    """
    Submits the packaged application to spark using a `spark-submit` subprocess

    Parameters
    ----------
    lane_yaml (str): Path to the YAML lane definition file
    dist_dir (str): Path to the directory where the packaged code is located
    spark_args (str): String of any additional spark config args to be passed when submitting
    silent (bool): Flag indicating whether job output should be printed to console
    """
    # spark-submit binary
    cmd = ['spark-submit' if spark_home is None else os.path.join(spark_home, 'bin/spark-submit')]

    # Supplied spark arguments
    if spark_args:
        cmd += spark_args

    # Packaged App & lane
    cmd += ['--py-files', 'libs.zip,_framework.zip,tasks.zip', 'main.py']
    cmd += ['--lane', lane_yaml]

    logging.info('Submitting to Spark')
    logging.debug(str(cmd))

    # Submit
    devnull = open(os.devnull, 'w')
    outp = {'stderr': STDOUT, 'stdout': devnull} if silent else {}
    call(cmd, cwd=dist_dir, env=MY_ENV, **outp)
    devnull.close()


def __clean_up(dist_dir):
    """Delete packaged app"""
    shutil.rmtree(dist_dir)
