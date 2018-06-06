"""Includes the `Task` decorator, the parent class `LaneTask` from which all tasks will inherit,
as well as the `_TaskCache`, which is used to share attributes between Task objects."""
import sys
from datetime import timedelta
from functools import WRAPPER_ASSIGNMENTS
from inspect import isclass
from threading import Thread, Lock
from time import time
from types import MethodType

from six import string_types, PY2, PY3

from .env import INTERNAL_LOGGER_NAME
from .errors import CacheError, TaskInitializationError, LaneExecutionError
from .log import make_default_logger


def Task(entry):  # pylint: disable=invalid-name
    """
    Decorator with which classes, who act as tasks in a `Lane`, must be decorated. When a class is
    being decorated, it becomes a child of `LaneTask`.

    Parameters
    ----------
    entry: The name of the task's "main" method, i.e. the method which is executed when task is run

    Returns
    -------
    wrapper (function): The actual decorator function
    """
    if not isinstance(entry, string_types):
        # In the event that no argument is supplied to the decorator, python passes the decorated
        # class itself as an argument. That way, we can detect if no argument (or an argument of
        # invalid type) was supplied. This allows passing of `entry` as both a named kwarg, and
        # as an arg. Isn't neat, but for now it suffices.
        raise TypeError('When decorating a class with `Task`, a single string argument must be '
                        'supplied, which specifies the "main" task method, i.e. the class\'s entry '
                        'point to the task.')
    else:
        def wrapper(cls):
            """The actual decorator function"""
            if isclass(cls):
                if not hasattr(cls, entry):  # Check if cls has the specified entry method
                    raise TypeError('Method `%s` not found in class `%s`.' % (entry, cls.__name__))

                # We will have to inspect the task class's `__init__` method later (by inspecting
                # the arg signature, before it is instantiated). In various circumstances, classes
                # will not have an unbound `__init__` method. Let's deal with that now already, by
                # assigning an empty, unbound `__init__` method manually, in order to prevent
                # errors later on during method inspection (not an issue in Python 3):
                # - Whenever a class is not defined as a new-style class in Python 2.7, i.e. a
                # sub-class of object, and it does not have a `__init__` method definition, the
                # class will not have an attribute `__init__`
                # - If a class misses a `__init__` method definition, but is defined as a
                # new-style class, attribute `__init__` will be of type `slot wrapper`, which
                # cannot be inspected (and it also doesn't seem possible to check if a method is of
                # type `slot wrapper`, which is why we manually define one).
                if not hasattr(cls, '__init__') or cls.__init__ == object.__init__:
                    init = MethodType(lambda self: None, None, cls) \
                        if PY2 else MethodType(lambda self: None, cls)
                    setattr(cls, '__init__', init)

                # Check for attributes that will be overwritten, in order to warn the user
                reserved_attributes = ('__getattr__', '__call__', '_entry_mtd', 'cache', 'uncache',
                                       'clear_cache', '_log_lock')
                for attr in dir(cls):
                    if attr in reserved_attributes:
                        make_default_logger(INTERNAL_LOGGER_NAME).warning(
                            'Attribute `%s` of class `%s` will be overwritten when decorated with '
                            '`sparklanes.Task`! Avoid assigning any of the following attributes '
                            '`%s`', attr, cls.__name__, str(reserved_attributes)
                        )

                assignments = {'_entry_mtd': entry,
                               '__getattr__': lambda self, name: TaskCache.get(name),
                               '__init__': cls.__init__,
                               '_log_lock': Lock()}
                for attr in WRAPPER_ASSIGNMENTS:
                    try:
                        assignments[attr] = getattr(cls, attr)
                    except AttributeError:
                        pass

                # Build task as a subclass of LaneTask
                return type('Task_%s' % cls.__name__, (LaneTask, cls, object), assignments)
            else:
                raise TypeError('Only classes can be decorated with `Task`')

        return wrapper


class LaneTask(object):
    """The super class of each task, from which all tasks inherit when being decorated with
    `sparklanes.Task`"""
    # pylint: disable=no-member
    def __new__(cls, *args, **kwargs):
        """Used to make sure the class will not be instantiated on its own. Instances of LaneTask
        should only exist as parents."""
        if cls is LaneTask:
            raise TaskInitializationError(
                "Task base `LaneTask` may not be instantiated on its own.")

        return object.__new__(cls, *args, **kwargs) if PY2 else object.__new__(cls)

    def __call__(self):
        """Used to make each task object callable, in order to execute tasks in a consistent
        manner. Calls the task's entry method and provides some logging."""
        logger = make_default_logger(INTERNAL_LOGGER_NAME)

        task_name = self.__name__ + '.' + self._entry_mtd
        logger.info('\n%s\nExecuting task `%s`\n%s',
                    '-'*80, task_name, '-'*80)
        start = time()
        res = getattr(self, self._entry_mtd)()
        passed = str(timedelta(seconds=(time() - start)))
        logger.info('\n%s\nFinished executing task `%s`. Execution time: %s\n%s',
                    '-'*80, task_name, passed, '-'*80)

        return res

    def cache(self, name, val, overwrite=True):
        """Assigns an attribute reference to all subsequent tasks. For example, if a task caches a
        DataFrame `df` using `self.cache('some_df', df)`, all tasks that follow can access the
        DataFrame using `self.some_df`. Note that manually assigned attributes that share the same
        name have precedence over cached attributes.

        Parameters
        ----------
        name : str
            Name of the attribute
        val
            Attribute value
        overwrite : bool
            Indicates if the attribute shall be overwritten, or not (if `False`, and
            a cached attribute with the given name already exists, `sparklanes.errors.CacheError`
            will be thrown).
        """
        if name in TaskCache.cached and not overwrite:
            raise CacheError('Object with name `%s` already in cache.' % name)
        TaskCache.cached[name] = val

    def uncache(self, name):
        """Removes an attribute from the cache, i.e. it will be deleted and becomes unavailable for
        all subsequent tasks.

        Parameters
        ----------
        name : str
            Name of the cached attribute, which shall be deleted
        """
        try:
            del TaskCache.cached[name]
        except KeyError:
            raise CacheError('Attribute `%s` not found in cache.' % name)

    def clear_cache(self):
        """Clears the entire cache"""
        TaskCache.cached = {}


class LaneTaskThread(Thread):
    """Used to spawn tasks as threads to be run in parallel."""

    def __init__(self, task):
        Thread.__init__(self)
        self.task = task
        self.exc = None
        self.daemon = True

    def run(self):
        """Overwrites `threading.Thread.run`, to allow handling of exceptions thrown by threads
        from within the main app."""
        self.exc = None
        try:
            self.task()
        except BaseException:
            self.exc = sys.exc_info()

    def join(self, timeout=None):
        """Overwrites `threading.Thread.join`, to allow handling of exceptions thrown by threads
        from within the main app."""
        Thread.join(self, timeout=timeout)
        if self.exc:
            msg = "Thread '%s' threw an exception `%s`: %s" \
                  % (self.getName(), self.exc[0].__name__, self.exc[1])
            new_exc = LaneExecutionError(msg)

            if PY3:
                raise new_exc.with_traceback(self.exc[2])  # pylint: disable=no-member
            else:
                raise (new_exc.__class__, new_exc, self.exc[2])  # pylint: disable=raising-bad-type


class TaskCache(object):
    """Serves as the attribute cache of tasks, which is accessed using the tasks'
    `__getattr__` method."""
    cached = {}

    def __new__(cls, *args, **kwargs):
        """Used to make sure that TaskCache will not be instantiated."""
        if cls is TaskCache:
            raise CacheError("`TaskCache` may not be instantiated and only provides static access.")

        return object.__new__(cls, *args, **kwargs) if PY2 else object.__new__(cls)

    @staticmethod
    def get(name):
        """Retrieves an object from the cache.

        Parameters
        ----------
        name : str
            Name of the object to be retrieved

        Returns
        -------
        object
        """
        try:
            return TaskCache.cached[name]
        except KeyError:
            raise AttributeError('Attribute `%s` not found in cache or object.' % name)
