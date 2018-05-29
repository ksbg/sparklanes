import inspect
import logging
import sys
from importlib import import_module

import yaml
from schema import Schema, SchemaError, Optional, Or
from six import PY2, PY3

import lane
from .errors import TaskInitializationError, LaneSchemaError, LaneImportError


def _arg_spec(cls, mtd_name):
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

    if _is_regular_method_or_class_method(cls, mtd_name) and len(required_params) > 0:
        del required_params[0]  # Remove first param

    return required_params, optional_params


def _validate_params(cls, mtd_name, *args, **kwargs):
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

    if ((PY2 and not inspect.ismethod(mtd)) or (PY3 and not inspect.isfunction(mtd))) \
            and not isinstance(cls.__dict__[mtd_name], staticmethod):
        raise TypeError('Attribute `%s` of class `%s` must be a method. Got type `%s` instead.'
                        % (mtd_name, cls.__name__, type(mtd)))

    required_params, optional_params = _arg_spec(cls, mtd_name)
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


def _validate_schema(yd, branch=False):
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
                _validate_schema(t, True)
            except SchemaError:
                task_schema.validate(t)

        return True

    return validate_tasks(yd['lane']['tasks'] if not branch else yd['branch']['tasks'])


def _is_regular_method_or_class_method(cls, mtd_name):
    """
    Checks if a method is either a regular method, or a class method.

    Parameters
    ----------
    cls (class)
    mtd_name (str): Name of the method to be checked

    Returns
    -------
    bool: `True` if the method is a regular method or a class method. `False` if otherwise
    """
    if inspect.isroutine(getattr(cls, mtd_name)):
        bound_mtd = cls.__dict__[mtd_name]
        if isinstance(bound_mtd, classmethod):  # class method?
            return True
        elif isinstance(bound_mtd, staticmethod):  # static method?
            return False
        else:  # regular method?
            return True

    return False


def make_logger(name, level=logging.INFO, fmt='%(asctime)s - %(name)s - %(levelname)s - %(message)s'):
    # TODO: better logging
    logger = logging.getLogger(name)
    logger.setLevel(level)
    if not logger.handlers:
        ch = logging.StreamHandler(sys.stderr)
        ch.setLevel(level)
        formatter = logging.Formatter(fmt)
        ch.setFormatter(formatter)
        logger.addHandler(ch)

    return logger


def build_lane_from_yaml(path):
    """
    Builds a `sparklanes.Lane` object from a YAML definition file.

    Parameters
    ----------
    path (str): Path to the YAML definition file

    Returns
    -------
    sparklanes.Lane: Lane, built according to instructions in YAML file
    """
    with open(path, 'rb') as yaml_definition:
        definition = yaml.load(yaml_definition)

    try:
        _validate_schema(definition)
    except SchemaError as e:
        raise LaneSchemaError(**e.__dict__)

    def build(lb_def, branch=False):
        init_kwargs = {k: lb_def[k] for k in [kk for kk in 'run_parallel', 'name' if kk in lb_def]}
        lb = lane.Lane(**init_kwargs) if not branch else lane.Branch(**init_kwargs)

        for task in lb_def['tasks']:
            try:
                Schema({'branch': dict}).validate(task)
                branch_def = task['branch']
                lb.add(build(branch_def, True))
            except SchemaError:
                sep = task['class'].rfind('.')
                if sep == -1:
                    raise LaneImportError('Class must include its parent module')
                mdl = task['class'][:sep]
                cls_ = task['class'][sep + 1:]

                try:
                    cls = getattr(import_module(mdl), cls_)
                except ImportError:
                    raise LaneImportError('Could not find module %s' % mdl)  # TODO better error
                except AttributeError:
                    raise LaneImportError('Could not find class %s' % cls_)  # TODO better error

                args = task['args'] if 'args' in task else []
                args = [args] if not isinstance(args, list) else args
                kwargs = task['kwargs'] if 'kwargs' in task else {}
                lb.add(cls, *args, **kwargs)

        return lb

    return build(definition['lane'])