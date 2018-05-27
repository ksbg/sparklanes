import inspect
import yaml

from six import string_types, PY2, PY3

from sparklanes.framework.errors import TaskInitializationError


def _arg_spec(cls, mtd_name):
    """Cross-version compatible argspec inspection"""
    mtd = getattr(cls, mtd_name)

    required_params = []
    optional_params = []
    if hasattr(inspect, 'signature'):  # Python 3
        params = inspect.signature(mtd).parameters
        for k in params.keys():
            if params[k].default == inspect.Parameter.empty:
                optional_params.append(k)
            else:
                required_params.append(k)
    else:  # Python 2
        params = inspect.getargspec(mtd)
        n = len(params[0]) if params[0] else 0
        n_opt = len(params[3]) if params[3] else 0
        n_req = (n - n_opt) if n_opt <= n else 0
        for i in range(0, n_req):
            required_params.append(params[0][i])
        for i in range(n_req, n):
            optional_params.append(params[0][i])

    if is_regular_method_or_class_method(cls, mtd_name) and len(required_params) > 0:
        del required_params[0]  # Remove first param

    return required_params, optional_params


def _validate_params(cls, mtd_name, *args, **kwargs):
    mtd = getattr(cls, mtd_name)

    if (PY2 and not inspect.ismethod(mtd)) or (PY3 and not inspect.isfunction(mtd)):
        raise TypeError('Attribute `%s` of class `%s` must be a method. Got type `%s` instead.'
                        % (mtd_name, cls.__name__, type(mtd)))

    required_params, optional_params = _arg_spec(cls, mtd_name)
    n_params = len(required_params) + len(optional_params)
    n_args_kwargs = len(args) + len(kwargs)
    if n_args_kwargs < n_params:
        raise TaskInitializationError('Not enough args/kwargs supplied for callable `%s`. Required args: %s'
                                      % (mtd.__name__, ' '.join(required_params)))
    if n_args_kwargs > n_params:
        raise TaskInitializationError('Too many args/kwargs supplied for callable `%s`. Required args: %s'
                                      % (mtd.__name__, ' '.join(required_params)))
    redundant_p = [p for p in kwargs.keys() if p not in required_params + optional_params]
    if len(redundant_p) > 0:
        raise TaskInitializationError('Supplied one or more kwargs that in the signature of callable `%s`. Redundant '
                                      'kwargs: %s' % (mtd.__name__, ' '.join(redundant_p)))

    for k in kwargs.keys():
        if not isinstance(k, string_types):
            raise TypeError('kwarg keys must be strings. Got type `%s` instead.' % type(k))
        if k not in required_params and k not in optional_params:
            raise TaskInitializationError('kwarg `%s` is not a parameter of callable `%s`.' % (k, mtd.__name__))

from schema import Schema, SchemaError, Optional, Or

def _validate_schema(yd):
    pass


def is_regular_method_or_class_method(cls, mtd_name):
    if inspect.isroutine(getattr(cls, mtd_name)):
        bound_mtd = cls.__dict__[mtd_name]
        if isinstance(bound_mtd, classmethod):  # class method?
            return True
        elif isinstance(bound_mtd, staticmethod):  # static method?
            return False
        else:  # regular method?
            return True

    return False



def load_yaml(path):
    pass