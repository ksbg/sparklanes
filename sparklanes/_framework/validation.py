from six import PY2, PY3
from schema import Schema, Optional, Or
from .errors import TaskInitializationError, SchemaError
import inspect


def validate_schema(yd, branch=False):
    """
    Validates if a dictionary matches the following schema.

    lane:
      name: str {Custom name of the lane/branch} (optional)
      run_parallel: bool {Indicates whether the tasks in lane/branch should be executed in parallel)
      tasks: list {list of tasks or branches} (required)
        - class: str {name of the class, with full path under which it is accessible, e.g. pkg.module.cls} (required)
          args: List[object] {list of arguments} (optional)
          kwargs: Dict{str: object} {dict of keyword arguments} (optional)
        - branch: {same definition as `lane`. Can be nested infinitely with more branches in the branch's `tasks`}
            name: ...
            ...
        ...

    Parameters
    ----------
    yd (dict): Dict whose schema shall be validated
    branch (bool): Indicates whether `yd` is a dict of a top-level lane, or of a branch inside a lane (needed for
                   recursive validation)

    Returns
    -------
    bool: True if validation was successful
    """
    schema = Schema({
        'lane' if not branch else 'branch': {
            Optional('name'): str,
            Optional('run_parallel'): bool,
            'tasks': list
        }
    })

    schema.validate(yd)
    from schema import And, Use
    task_schema = Schema({
        'class': str,
        Optional('kwargs'): Or({str: object}),
        Optional('args'): Or([object], And(Use(lambda a: isinstance(a, dict)), False))
    })

    def validate_tasks(tasks):
        for t in tasks:
            try:
                Schema({'branch': dict}).validate(t)
                validate_schema(t, True)
            except SchemaError:
                task_schema.validate(t)

        return True

    return validate_tasks(yd['lane']['tasks'] if not branch else yd['branch']['tasks'])


def validate_params(cls, mtd_name, *args, **kwargs):
    """
    Validates if the given args/kwargs match the method signature. Checks if:
    - at least all required args/kwargs are given
    - no redundant args/kwargs are given

    Parameters
    ----------
    cls (class)
    mtd_name (str): Name of the method whose parameters shall be validated
    args (List): Positional arguments
    kwargs (dict): Dict of keyword arguments
    """
    mtd = getattr(cls, mtd_name)

    if (PY3 and not (inspect.isfunction(mtd) or inspect.ismethod(mtd)) and hasattr(cls, mtd_name)) or \
            PY2 and not inspect.ismethod(mtd) and not isinstance(cls.__dict__[mtd_name], staticmethod):
        raise TypeError('Attribute `%s` of class `%s` must be a method. Got type `%s` instead.'
                        % (mtd_name, cls.__name__, type(mtd)))

    required_params, optional_params = arg_spec(cls, mtd_name)
    n_params = len(required_params) + len(optional_params)
    n_req_params = len(required_params)
    n_args_kwargs = len(args) + len(kwargs)

    for k in kwargs.keys():
        if k not in required_params and k not in optional_params:
            raise TaskInitializationError('kwarg `%s` is not a parameter of callable `%s`.' % (k, mtd.__name__))

    if n_args_kwargs < n_req_params:
        raise TaskInitializationError('Not enough args/kwargs supplied for callable `%s`. Required args: %s'
                                      % (mtd.__name__, ' '.join(required_params)))
    if len(args) > n_params or n_args_kwargs > n_params or len(kwargs) > n_params:
        raise TaskInitializationError('Too many args/kwargs supplied for callable `%s`. Required args: %s'
                                      % (mtd.__name__, ' '.join(required_params)))

    redundant_p = [p for p in kwargs.keys() if p not in required_params[len(args):] + optional_params]
    if len(redundant_p) > 0:
        raise TaskInitializationError('Supplied one or more kwargs that in the signature of callable `%s`. Redundant '
                                      'kwargs: %s' % (mtd.__name__, ' '.join(redundant_p)))

    needed_kwargs = required_params[len(args):]
    if not all([True if p in kwargs.keys() else False for p in needed_kwargs]):
        raise TaskInitializationError('Not enough args/kwargs supplied for callable `%s`. Required args: %s'
                                       % (mtd.__name__, ' '.join(required_params)))


def arg_spec(cls, mtd_name):
    """
    Inspects the argument signature of a method (compatible with both PY2/3)

    Parameters
    ----------
    cls (class)
    mtd_name (str): Name of the method to be inspected

    Returns
    -------
    required_params (List[str]): List of required, positional parameters
    optional_params (List[str]): List of optional parameters, i.e. parameters with a default value
    """
    mtd = getattr(cls, mtd_name)

    required_params = []
    optional_params = []

    if hasattr(inspect, 'signature'):  # Python 3
        params = inspect.signature(mtd).parameters

        for k in params.keys():
            if params[k].default == inspect.Parameter.empty:
                # Python 3 does not make a difference between unbound methods and functions, so the only way to check if
                # the first argument is of a regular method, or class method, is to look for the common name. Yikes.
                if not (params[k].name == 'self' or params[k].name == 'cls'):
                    required_params.append(k)
            else:
                optional_params.append(k)
    else:  # Python 2
        params = inspect.getargspec(mtd)
        n = len(params[0]) if params[0] else 0
        n_opt = len(params[3]) if params[3] else 0
        n_req = (n - n_opt) if n_opt <= n else 0
        for i in range(0, n_req):
            required_params.append(params[0][i])
        for i in range(n_req, n):
            optional_params.append(params[0][i])

        if inspect.isroutine(getattr(cls, mtd_name)):
            bound_mtd = cls.__dict__[mtd_name]
            if not isinstance(bound_mtd, staticmethod):
                del required_params[0]

    return required_params, optional_params
