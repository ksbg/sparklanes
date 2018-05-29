import sys
from datetime import timedelta
from functools import WRAPPER_ASSIGNMENTS
from inspect import isclass
from threading import Thread, Lock
from time import time
from types import MethodType

from six import string_types, PY2, PY3

import utils
from .errors import CacheError, TaskInitializationError, LaneExecutionError


class Lane(object):
    """Used to build and run data processing lanes (i.e. pipelines)."""

    def __init__(self, name='UnnamedLane', run_parallel=False):
        """
        Parameters
        ----------
        name (str): Custom name of the lane
        run_parallel (bool): Indicates, whether the tasks in a Lane shall be executed in parallel. Does not affect
                             branches inside the lane (`run_parallel` must be indicated in the branches themselves)
        """
        if not isinstance(name, string_types):
            raise TypeError('`name` must be a string')
        self.name = name
        self.run_parallel = run_parallel

        self.tasks = []

        self._log_lock = Lock()

    def __str__(self):
        """Generates a readable string using the tasks/branches inside the lane"""
        task_str = '=' * 80 + '\n'

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

        task_str += generate_str(self) + '=' * 80

        return task_str

    def __validate_task(self, cls, entry_mtd_name, args, kwargs):
        """
        Checks if a class is a task, i.e. if it has been decorated with `Task`, and if the supplied args/kwargs match
        the signature of the task's entry method.

        Parameters
        ----------
        cls (_LaneTask)
        entry_mtd_name (str): Name of the method, which is called when the task is run
        args (List[object])
        kwargs (Dict{str: object})
        """
        if not isclass(cls) or not issubclass(cls, _LaneTask):
            raise TypeError('Tried to add non-Task `%s` to a Lane. Are you sure you it\'s a `Task`-decorated class?'
                            % str(cls))

        utils._validate_params(cls, entry_mtd_name, *args, **kwargs)

    def add(self, cls_or_branch, *args, **kwargs):
        """
        Adds a task or branch to the lane (chainable).

        Parameters
        ----------
        cls_or_branch (_LaneTask|Branch)
        args (List[object])
        kwargs (Dict{str: object})

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
        """Executes the tasks in the lane in the order in which they have been added, unless `self.run_parallel` is True,
        then each task is assigned a Thread and executed in parallel (note that task threads are still spawned in the
        order in which they were added).
        """
        logger = utils.make_logger('sparklanes')
        logger.info(('\n' + '-' * 80 + '\nExecuting `%s`\n' + '-' * 80) % self.name)
        logger.info('\n' + str(self))

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

        logger.info(('\n' + '-' * 80 + '\nFinished executing `%s`\n' + '-' * 80) % self.name)


class Branch(Lane, object):
    """Used to split task lanes into branches, which is e.g. useful if part of the data processing pipeline should be
    executed in parallel, while other parts should be run in subsequent order."""

    def __init__(self, name='UnnamedBranch', run_parallel=False):
        """
        Parameters
        ----------
        name (str): Custom name of the branch
        run_parallel (bool): Indicates if the task in the branch shall be executed in parallel
        args (List[object])
        """
        super(Branch, self).__init__(name=name, run_parallel=run_parallel)


def Task(entry):
    """
    Decorator with which classes, who act as tasks in a `Lane`, must be decorated. When a class is being decorated,
    it becomes a child of `_LaneTask`.

    Parameters
    ----------
    entry: The name of the task's "main" method, i.e. the method which is executed when a task is run

    Returns
    -------
    wrapper (function): The actual decorator function
    """
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
                reserved_attributes = ('__getattr__', '__call__', '_entry_mtd', 'cache', 'uncache', 'clear_cache')
                for attr in dir(cls):
                    if attr in reserved_attributes:
                        utils.make_logger('sparklanes').warning(
                            'Attribute `%s` of class `%s` will be overwritten when decorated with `Task`! A number of '
                            'method names are reserved for sparklane\'s internals. Avoid assigning any of the following '
                            'attributes `%s`' % (attr, cls.__name__, str(reserved_attributes))
                        )

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
    """The super class of each task, from which all tasks inherit when being decorated with `Task`"""

    def __new__(cls, *args, **kwargs):
        """Used to make sure the class will not be instantiated on its own. Instances should only exist as parents."""
        if cls is _LaneTask:
            raise TaskInitializationError("Task base `LaneTask` may not be instantiated on its own.")

        return object.__new__(cls, *args, **kwargs)

    def __call__(self):
        """Used to make each task object callable, in order to execute tasks in a consistent fashion. Calls the task's
        entry method"""
        logger = utils.make_logger('sparklanes')

        task_name = self.__name__ + '.' + self._entry_mtd
        logger.info(('\n' + '-' * 80 + '\nExecuting task `%s`\n' + '-' * 80) % task_name)
        start = time()
        res = getattr(self, self._entry_mtd)()
        passed = str(timedelta(seconds=(time() - start)))
        logger.info(('\n' + '-' * 80 + '\nFinished executing task `%s`. Execution time: %s\n' + '-' * 80)
                    % (task_name, passed))

        return res

    def cache(self, name, val, overwrite=True):
        """
        Assigns an attribute reference to all subsequent tasks. For example, if a task caches a dataframe `df` using
        `self.cache('some_df', df)`, all tasks that follow can access the dataframe using `self.some_df`. Note that
        manually assigned attributes that share the same name have precedence over cached attributes.

        Parameters
        ----------
        name (str): Name of the attribute
        val (object): Attribute value
        overwrite (bool): Indicates if the attribute shall be overwritten, or not (if `False`, and a cached attribute
                          with the given name already exists, `sparklanes.errors.CacheError` will be thrown.
        """
        if name in _TaskCache.cached and not overwrite:
            raise CacheError('Object with name `%s` already in cache.' % name)
        _TaskCache.cached[name] = val

    def uncache(self, name):
        """
        Removes an attribute from the cache, i.e. it will be deleted and becomes unavailable for all tasks that follow.

        Parameters
        ----------
        name (str): Name of the cached attribute, which shall be deleted
        """
        try:
            del _TaskCache.cached[name]
        except KeyError:
            raise CacheError('Attribute `%s` not found in cache.' % name)

    def clear_cache(self):
        """Clears the entire cache"""
        _TaskCache.cached = {}


class _LaneTaskThread(Thread):
    """Used to spawn tasks as threads to be run in parallel."""

    def __init__(self, task):
        Thread.__init__(self)
        self.task = task
        self.exc = None
        self.daemon = True

    def run(self):
        """Overwrites `threading.Thread.run`, to allow handling of exceptions thrown by threads from within the
        main app."""
        self.exc = None
        try:
            self.task()
        except BaseException:
            self.exc = sys.exc_info()

    def join(self, timeout=None):
        """Overwrites `threading.Thread.join`, to allow handling of exceptions thrown by threads from within the
        main app."""
        Thread.join(self, timeout=timeout)
        if self.exc:
            msg = "Thread '%s' threw an exception `%s`: %s" % (self.getName(), self.exc[0].__name__, self.exc[1])
            new_exc = LaneExecutionError(msg)
            if PY3:
                raise new_exc.with_traceback(self.exc[2])
            else:
                raise new_exc.__class__, new_exc, self.exc[2]


class _TaskCache(object):
    """Serves as the attribute cache of tasks, which is accessed using the tasks' `__getattr__` method."""
    cached = {}

    def __new__(cls, *args, **kwargs):
        """Used to make sure that _TaskCache will not be instantiated."""
        if cls is _TaskCache:
            raise CacheError("`_TaskCache` may not be instantiated and only provides static access.")

        return object.__new__(cls, *args, **kwargs)

    @staticmethod
    def get(name):
        """
        Retrieves an object from the cache.

        Parameters
        ----------
        name (str): Name of the object to be retrieved

        Returns
        -------
        object
        """
        try:
            return _TaskCache.cached[name]
        except KeyError:
            raise AttributeError('Attribute `%s` not found in cache or object.' % name)
