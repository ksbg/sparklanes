import inspect
from datetime import timedelta
from functools import WRAPPER_ASSIGNMENTS
from time import time
from types import MethodType

from six import string_types, PY2
from tabulate import tabulate

from .errors import CacheError
from ..spark import get_logger

logger = get_logger('SparklanesTaskBuilder')


class _TaskCache(object):
    cached = {}

    @staticmethod
    def _get(name):
        try:
            return _TaskCache.cached[name]
        except KeyError:
            raise AttributeError('Attribute `%s` not found in cache or object.' % name)


class LaneTask(object):
    def __call__(self):
        logger.info('\n' + tabulate((('Executing task `%s`' % self.__name__ + '.' + self._entry_mtd,),)))
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


def Task(entry):
    if not isinstance(entry, string_types):
        # In the event that no argument is supplied to the decorator, python passes the decorated class itself as an
        # argument. That way, we can detect if no argument (or an argument of invalid type) was supplied.
        # This allows passing of the entry as both a named kwarg, and as an arg. Isn't neat, but for now it suffices.
        raise TypeError('When decorating a class with `Task`, a single string argument must be supplied, which '
                        'specifies the "main" task method, i.e. the class\'s entry point to the task.')
    else:
        def wrapper(cls):
            if inspect.isclass(cls):
                if not hasattr(cls, entry):  # Check if cls has the specified entry method
                    raise TypeError('Method `%s` not found in class `%s`.' % (entry, cls.__name__))

                # We will have to inspect the task class's `__init__` method later (before it is instantiated). In
                # various circumstances, classes will not have an unbound `__init__` method. Let's deal with that now
                # already, by assigning an empty, unbound `__init__` method manually, in order to prevent errors later
                # on during method inspection:
                # - Whenever a class is not defined as a new-style class in Python 2.7, i.e. a child of object, and
                #   it does not have a `__init__` method definition, the class will not have an attribute `__init__`
                # - If a classes misses a `__init__` method definition, but is defined as a new-style class,
                #   attribute `__init__` will be of type `slot wrapper`, which cannot be inspected
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
                               '__getattr__': lambda self, name: _TaskCache._get(name),
                               '__init__': cls.__init__}
                for attr in WRAPPER_ASSIGNMENTS:  # Copy __name__, __module__ and __doc__ from cls
                    assignments[attr] = getattr(cls, attr)

                # Build task as a subclass of LaneTask
                return type('Task_', (LaneTask, cls, object), assignments)
            else:
                raise TypeError('Only classes can be decorated with `Task`')

        return wrapper
