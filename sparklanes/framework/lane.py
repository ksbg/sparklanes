import os
import pickle
import sys
from datetime import timedelta
from functools import WRAPPER_ASSIGNMENTS
from inspect import isclass
from threading import Thread, Lock
from time import time

from six import string_types, PY2, PY3
from tabulate import tabulate
from types import MethodType

from .errors import CacheError, TaskInitializationError, LaneExecutionError
from .utils import _validate_params
from ..spark import get_logger

logger = get_logger('SparklanesTaskBuilder')


class Lane(object):
    def __init__(self, name='UnnamedLane', run_parallel=False, *args):
        if not isinstance(name, string_types):
            raise TypeError('`name` must be a string')
        self.name = name
        self.run_parallel = run_parallel

        self.tasks = []
        if len(args) > 0:
            for task in args:
                self.add(task)

        self.logger = get_logger(name)
        self.log_lock = Lock()

    def __str__(self):
        task_str = '=' * 50 + '\n'

        def generate_str(lane_or_branch, prefix='\t', out=''):
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

        task_str += generate_str(self) + '\n' + '=' * 50

        return task_str

    def save_to_file(self, path, overwrite=True):
        if not overwrite and os.path.exists(path):
            raise IOError('Path `%s` already exists.' % path)

        with open(path, 'wb') as f:
            pickle.dump(self, f)

        logger.info('Successfully dumped Lane to `%s`' % path)

    def load_from_file(self, path):
        with open(path, 'rb') as f:
            lane = pickle.load(f)

        logger.info('Successfully loaded Lane from `%s`' % path)

        return lane

    def __validate_task(self, cls, entry_mtd_name, args, kwargs):
        if not isclass(cls) or not issubclass(cls, _LaneTask):
            raise TypeError('Tried to add non-Task `%s` to a Lane. Are you sure you it\'s a `Task`-decorated class?'
                            % str(cls))

        _validate_params(cls, entry_mtd_name, *args, **kwargs)

    def add(self, cls_or_branch, *args, **kwargs):
        if isinstance(cls_or_branch, Branch):
            self.tasks.append(cls_or_branch)  # Add branch with already validated tasks
        else:
            # Validate
            self.__validate_task(cls_or_branch, '__init__', args, kwargs)
            # Append
            self.tasks.append({'cls_or_branch': cls_or_branch, 'args': args, 'kwargs': kwargs})

        return self

    def run(self):
        self.logger.info('\n' + tabulate((('Executing `%s`' % self.name,),)))
        threads = []

        if len(self.tasks) == 0:
            raise LaneExecutionError('No tasks to execute!')

        for task_def_or_branch in self.tasks:
            if isinstance(task_def_or_branch, Branch):
                task_def_or_branch.run()
            elif isinstance(task_def_or_branch['cls_or_branch'], Branch):  # Nested Branch
                task_def_or_branch['cls_or_branch'].run()
            else:
                task = task_def_or_branch['cls_or_branch'](*task_def_or_branch['args'], **task_def_or_branch['kwargs'])
                if self.run_parallel:
                    threads.append(_LaneTaskThread(task))
                else:
                    task()

        if len(threads) > 0:
            for thread in threads:
                thread.start()
            for thread in threads:
                thread.join()

        self.logger.info('\n' + tabulate((('Finished executing `%s`' % self.name,),)))


class Branch(Lane, object):
    def __init__(self, name='UnnamedBranch', run_parallel=False, *args):
        super(Branch, self).__init__(name=name, run_parallel=run_parallel, *args)


def Task(entry):
    if not isinstance(entry, string_types):
        # In the event that no argument is supplied to the decorator, python passes the decorated class itself as an
        # argument. That way, we can detect if no argument (or an argument of invalid type) was supplied.
        # This allows passing of the entry as both a named kwarg, and as an arg. Isn't neat, but for now it suffices.
        raise TypeError('When decorating a class with `Task`, a single string argument must be supplied, which '
                        'specifies the "main" task method, i.e. the class\'s entry point to the task.')
    else:
        def wrapper(cls):
            if isclass(cls):
                if not hasattr(cls, entry):  # Check if cls has the specified entry method
                    raise TypeError('Method `%s` not found in class `%s`.' % (entry, cls.__name__))

                # We will have to inspect the task class's `__init__` method later (before it is instantiated). In
                # various circumstances, classes will not have an unbound `__init__` method. Let's deal with that now
                # already, by assigning an empty, unbound `__init__` method manually, in order to prevent errors later
                # on during method inspection:
                # - Whenever a class is not defined as a new-style class in Python 2.7, i.e. a child of object, and
                #   it does not have a `__init__` method definition, the class will not have an attribute `__init__`
                # - If a classes misses a `__init__` method definition, but is defined as a new-style class,
                #   attribute `__init__` will be of type `slot wrapper`, which cannot be inspected (and it also doesn't
                #   seem possible to check if a method is of type `slot wrapper`, which is why we manually define one
                if not hasattr(cls, '__init__') or (hasattr(cls, '__init__') and cls.__init__ == object.__init__):
                    init = MethodType(lambda self: None, None, cls) if PY2 else MethodType(lambda self: None, cls)
                    setattr(cls, '__init__', init)

                # Check for attributes that will be overwritten
                reserved_attributes = ('__getattr__', '__call__', '_entry_mtd', 'cache', 'uncache', 'uncache_all')
                for attr in dir(cls):
                    if attr in reserved_attributes:
                        logger.warning('Attribute `%s` of class `%s` will be overwritten when decorated with `Task`! A '
                                       'number of method names are reserved for sparklane\'s internals. Avoid '
                                       'assigning any of the following attributes `%s`'
                                       % (attr, cls.__name__, str(reserved_attributes)))

                assignments = {'_entry_mtd': entry,
                               '__getattr__': lambda self, name: _TaskCache.get(name),
                               '__init__': cls.__init__}
                for attr in WRAPPER_ASSIGNMENTS:
                    assignments[attr] = getattr(cls, attr)

                # Build task as a subclass of LaneTask
                return type('Task_%s' % cls.__name__, (_LaneTask, cls, object), assignments)
            else:
                raise TypeError('Only classes can be decorated with `Task`')

        return wrapper


class _LaneTask(object):
    def __new__(cls, *args, **kwargs):
        if cls is _LaneTask:
            raise TaskInitializationError("Task base `LaneTask` may not be instantiated on its own.")

        return object.__new__(cls, *args, **kwargs)

    def __call__(self):
        logger.info('\n' + tabulate((('Executing task `%s`' % (self.__name__ + '.' + self._entry_mtd),),)))
        start = time()
        res = getattr(self, self._entry_mtd)()
        passed = str(timedelta(seconds=(time() - start)))
        logger.info('\n' + tabulate((('Finished executing task `%s`. Execution time: %s'
                                      % (self.__name__ + '.' + self._entry_mtd, passed),),)))

        return res

    def cache(self, name, val, overwrite=True):
        if name in _TaskCache.cached and not overwrite:
            raise CacheError('Object with name `%s` already in cache.' % name)
        _TaskCache.cached[name] = val

    def uncache(self, name):
        try:
            del _TaskCache.cached[name]
        except KeyError:
            raise CacheError('Attribute `%s` not found in cache.' % name)

    def uncache_all(self):
        _TaskCache.cached = {}


class _LaneTaskThread(Thread):
    def __init__(self, task):
        Thread.__init__(self)
        self.task = task
        self.exc = None
        self.daemon = True

    def run(self):
        self.exc = None
        try:
            self.task()
        except BaseException:
            self.exc = sys.exc_info()

    def join(self):
        Thread.join(self)
        if self.exc:
            msg = "Thread '%s' threw an exception `%s`: %s" % (self.getName(), self.exc[0].__name__, self.exc[1])
            new_exc = LaneExecutionError(msg)
            if PY3:
                raise new_exc.with_traceback(self.exc[2])
            else:
                raise new_exc.__class__, new_exc, self.exc[2]


class _TaskCache(object):
    cached = {}

    @staticmethod
    def get(name):
        try:
            return _TaskCache.cached[name]
        except KeyError:
            raise AttributeError('Attribute `%s` not found in cache or object.' % name)
