"""Lane and Branch classes. TODO: Better logging"""
from importlib import import_module
from inspect import isclass

import yaml
from schema import SchemaError

from six import string_types

from sparklanes._framework.errors import LaneSchemaError, LaneImportError
from sparklanes._framework.validation import validate_schema

from .env import INTERNAL_LOGGER_NAME
from .errors import LaneExecutionError
from .log import make_default_logger
from .task import LaneTask, LaneTaskThread
from .validation import validate_params


class Lane(object):
    """Used to build and run data processing lanes (i.e. pipelines).
    Public methods are chainable."""

    def __init__(self, name='UnnamedLane', run_parallel=False):
        """
        Parameters
        ----------
        name : str
            Custom name of the lane
        run_parallel : bool
            Indicates, whether the tasks in a Lane shall be executed in parallel.
            Does not affect branches inside the lane (`run_parallel` must be indicated in the
            branches themselves)
        """
        if not isinstance(name, string_types):
            raise TypeError('`name` must be a string')
        self.name = name
        self.run_parallel = run_parallel

        self.tasks = []

    def __str__(self):
        """Generates a readable string using the tasks/branches inside the lane, i.e. builds a
        string the showing the tasks and branches in a lane"""
        task_str = '=' * 80 + '\n'

        def generate_str(lane_or_branch, prefix='\t', out=''):
            """Recursive string generation"""
            out += prefix + lane_or_branch.name
            if lane_or_branch.run_parallel:
                out += ' (parallel)'
            out += '\n'

            for task in lane_or_branch.tasks:
                if isinstance(task, Branch):
                    out += generate_str(task, prefix + prefix[0])
                elif isinstance(task['cls_or_branch'], Branch):
                    out += generate_str(task['cls_or_branch'], prefix + prefix[0])
                else:
                    out += prefix + ' >' + task['cls_or_branch'].__name__ + '\n'

            return out

        task_str += generate_str(self) + '=' * 80

        return task_str

    def __validate_task(self, cls, entry_mtd_name, args, kwargs):
        """Checks if a class is a task, i.e. if it has been decorated with `sparklanes.Task`, and if
        the supplied args/kwargs match the signature of the task's entry method.

        Parameters
        ----------
        cls : LaneTask
        entry_mtd_name : str
            Name of the method, which is called when the task is run
        args : list
        kwargs : dict
        """
        if not isclass(cls) or not issubclass(cls, LaneTask):
            raise TypeError('Tried to add non-Task `%s` to a Lane. Are you sure the task was '
                            'decorated with `sparklanes.Task`?' % str(cls))

        validate_params(cls, entry_mtd_name, *args, **kwargs)

    def add(self, cls_or_branch, *args, **kwargs):
        """Adds a task or branch to the lane.

        Parameters
        ----------
        cls_or_branch : Class
        *args
            Variable length argument list to be passed to `cls_or_branch` during instantiation
        **kwargs
            Variable length keyword arguments to be passed to `cls_or_branch` during instantiation

        Returns
        -------
        self: Returns `self` to allow method chaining
        """
        if isinstance(cls_or_branch, Branch):
            self.tasks.append(cls_or_branch)  # Add branch with already validated tasks
        else:
            # Validate
            self.__validate_task(cls_or_branch, '__init__', args, kwargs)
            # Append
            self.tasks.append({'cls_or_branch': cls_or_branch, 'args': args, 'kwargs': kwargs})

        return self

    def run(self):
        """Executes the tasks in the lane in the order in which they have been added, unless
        `self.run_parallel` is True, then a thread is spawned for each task and executed in
        parallel (note that task threads are still spawned in the order in which they were added).
        """
        logger = make_default_logger(INTERNAL_LOGGER_NAME)
        logger.info('\n%s\nExecuting `%s`\n%s\n', '-'*80, self.name, '-'*80)
        logger.info('\n%s', str(self))

        threads = []

        if not self.tasks:
            raise LaneExecutionError('No tasks to execute!')

        for task_def_or_branch in self.tasks:
            if isinstance(task_def_or_branch, Branch):
                task_def_or_branch.run()
            elif isinstance(task_def_or_branch['cls_or_branch'], Branch):  # Nested Branch
                task_def_or_branch['cls_or_branch'].run()
            else:
                task = task_def_or_branch['cls_or_branch'](*task_def_or_branch['args'],
                                                           **task_def_or_branch['kwargs'])
                if self.run_parallel:
                    threads.append(LaneTaskThread(task))
                else:
                    task()

        if threads:
            for thread in threads:
                thread.start()
            for thread in threads:
                thread.join()

        logger.info('\n%s\nFinished executing `%s`\n%s', '-'*80, self.name, '-'*80)

        return self


class Branch(Lane, object):
    """Branches can be used to split task lanes into branches, which is e.g. useful if part of the
    data processing pipeline should be executed in parallel, while other parts should be run in
    subsequent order."""

    def __init__(self, name='UnnamedBranch', run_parallel=False):
        """
        Parameters
        ----------
        name (str): Custom name of the branch
        run_parallel (bool): Indicates if the task in the branch shall be executed in parallel
        args (List[object])
        """
        super(Branch, self).__init__(name=name, run_parallel=run_parallel)


def build_lane_from_yaml(path):
    """Builds a `sparklanes.Lane` object from a YAML definition file.

    Parameters
    ----------
    path: str
        Path to the YAML definition file

    Returns
    -------
    Lane
        Lane, built according to definition in YAML file
    """
    # Open
    with open(path, 'rb') as yaml_definition:
        definition = yaml.load(yaml_definition)

    # Validate schema
    try:
        validate_schema(definition)
    except SchemaError as exc:
        raise LaneSchemaError(**exc.__dict__)

    def build(lb_def, branch=False):
        """Function to recursively build the `sparklanes.Lane` object from a YAML definition"""
        init_kwargs = {k: lb_def[k] for k in (a for a in ('run_parallel', 'name') if a in lb_def)}
        lane_or_branch = Lane(**init_kwargs) if not branch else Branch(**init_kwargs)

        for task in lb_def['tasks']:
            if 'branch' in task:
                branch_def = task['branch']
                lane_or_branch.add(build(branch_def, True))
            else:
                sep = task['class'].rfind('.')
                if sep == -1:
                    raise LaneImportError('Class must include its parent module')
                mdl = task['class'][:sep]
                cls_ = task['class'][sep + 1:]

                try:
                    cls = getattr(import_module(mdl), cls_)
                except ImportError:
                    raise LaneImportError('Could not find module %s' % mdl)
                except AttributeError:
                    raise LaneImportError('Could not find class %s' % cls_)

                args = task['args'] if 'args' in task else []
                args = [args] if not isinstance(args, list) else args
                kwargs = task['kwargs'] if 'kwargs' in task else {}
                lane_or_branch.add(cls, *args, **kwargs)

        return lane_or_branch

    return build(definition['lane'])
