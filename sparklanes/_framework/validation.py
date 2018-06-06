"""Contains helper functions, used for class and schema validation."""
import inspect

from schema import Schema, Optional, Or
from six import PY2, PY3

from .errors import TaskInitializationError, SchemaError


def validate_schema(yaml_def, branch=False):
    """Validates the schema of a dict

    Parameters
    ----------
    yaml_def : dict
        dict whose schema shall be validated
    branch : bool
        Indicates whether `yaml_def` is a dict of a top-level lane, or of a branch
        inside a lane (needed for recursion)

    Returns
    -------
    bool
        True if validation was successful
    """
    schema = Schema({
        'lane' if not branch else 'branch': {
            Optional('name'): str,
            Optional('run_parallel'): bool,
            'tasks': list
        }
    })

    schema.validate(yaml_def)
    from schema import And, Use
    task_schema = Schema({
        'class': str,
        Optional('kwargs'): Or({str: object}),
        Optional('args'): Or([object], And(Use(lambda a: isinstance(a, dict)), False))
    })

    def validate_tasks(tasks):  # pylint: disable=missing-docstring
        for task in tasks:
            try:
                Schema({'branch': dict}).validate(task)
                validate_schema(task, True)
            except SchemaError:
                task_schema.validate(task)

        return True

    return validate_tasks(yaml_def['lane']['tasks'] if not branch else yaml_def['branch']['tasks'])


def validate_params(cls, mtd_name, *args, **kwargs):
    """Validates if the given args/kwargs match the method signature. Checks if:
    - at least all required args/kwargs are given
    - no redundant args/kwargs are given

    Parameters
    ----------
    cls : Class
    mtd_name : str
        Name of the method whose parameters shall be validated
    args: list
        Positional arguments
    kwargs : dict
        Dict of keyword arguments
    """
    mtd = getattr(cls, mtd_name)

    py3_mtd_condition = (not (inspect.isfunction(mtd) or inspect.ismethod(mtd))
                         and hasattr(cls, mtd_name))
    py2_mtd_condition = (not inspect.ismethod(mtd)
                         and not isinstance(cls.__dict__[mtd_name], staticmethod))
    if (PY3 and py3_mtd_condition) or (PY2 and py2_mtd_condition):
        raise TypeError('Attribute `%s` of class `%s` must be a method. Got type `%s` instead.'
                        % (mtd_name, cls.__name__, type(mtd)))

    req_params, opt_params = arg_spec(cls, mtd_name)
    n_params = len(req_params) + len(opt_params)
    n_args_kwargs = len(args) + len(kwargs)

    for k in kwargs:
        if k not in req_params and k not in opt_params:
            raise TaskInitializationError('kwarg `%s` is not a parameter of callable `%s`.'
                                          % (k, mtd.__name__))

    if n_args_kwargs < len(req_params):
        raise TaskInitializationError('Not enough args/kwargs supplied for callable `%s`. '
                                      'Required args: %s' % (mtd.__name__, str(req_params)))
    if len(args) > n_params or n_args_kwargs > n_params or len(kwargs) > n_params:
        raise TaskInitializationError('Too many args/kwargs supplied for callable `%s`. '
                                      'Required args: %s' % (mtd.__name__, str(req_params)))

    redundant_p = [p for p in kwargs if p not in req_params[len(args):] + opt_params]
    if redundant_p:
        raise TaskInitializationError('Supplied one or more kwargs that in the signature of '
                                      'callable `%s`. Redundant kwargs: %s'
                                      % (mtd.__name__, str(redundant_p)))

    needed_kwargs = req_params[len(args):]
    if not all([True if p in kwargs else False for p in needed_kwargs]):
        raise TaskInitializationError('Not enough args/kwargs supplied for callable `%s`. '
                                      'Required args: %s' % (mtd.__name__, str(req_params)))


def arg_spec(cls, mtd_name):
    """Cross-version argument signature inspection

    Parameters
    ----------
    cls : class
    mtd_name : str
        Name of the method to be inspected

    Returns
    -------
    required_params : list of str
        List of required, positional parameters
    optional_params : list of str
        List of optional parameters, i.e. parameters with a default value
    """
    mtd = getattr(cls, mtd_name)

    required_params = []
    optional_params = []

    if hasattr(inspect, 'signature'):  # Python 3
        params = inspect.signature(mtd).parameters  # pylint: disable=no-member

        for k in params.keys():
            if params[k].default == inspect.Parameter.empty:  # pylint: disable=no-member
                # Python 3 does not make a difference between unbound methods and functions, so the
                # only way to distinguish if the first argument is of a regular method, or a class
                # method, is to look for the conventional argument name. Yikes.
                if not (params[k].name == 'self' or params[k].name == 'cls'):
                    required_params.append(k)
            else:
                optional_params.append(k)
    else:  # Python 2
        params = inspect.getargspec(mtd)  # pylint: disable=deprecated-method
        num = len(params[0]) if params[0] else 0
        n_opt = len(params[3]) if params[3] else 0
        n_req = (num - n_opt) if n_opt <= num else 0
        for i in range(0, n_req):
            required_params.append(params[0][i])
        for i in range(n_req, num):
            optional_params.append(params[0][i])

        if inspect.isroutine(getattr(cls, mtd_name)):
            bound_mtd = cls.__dict__[mtd_name]
            if not isinstance(bound_mtd, staticmethod):
                del required_params[0]

    return required_params, optional_params
