import inspect
from importlib import import_module

from schema import Schema, SchemaError, Optional, Or

from core import error, PipelineProcess


def validate_pipeline_dict(pipeline_dict):
    proc_format = {'class': str, 'kwargs': {str: object}}
    shared_format = {'shared_resource_name': str, 'class': str, 'kwargs': {str: object}}

    schema = Schema({'processes': {'extract': Or([proc_format], proc_format),
                                   'transform': Or([proc_format], proc_format),
                                   'load': Or([proc_format], proc_format)},
                     Optional('shared'): Or([shared_format], shared_format)})
    try:
        return schema.validate(pipeline_dict)
    except SchemaError as e:
        raise error.PipelineSchemaError(**e.__dict__)


def validate_and_get_class(cls_path, shared=False):
    sep = cls_path.rfind('.')
    try:
        cls = getattr(import_module(cls_path[:sep]), cls_path[sep + 1:])
    except ImportError:
        raise error.PipelineModuleNotFoundError('Could not find module %s' % cls_path[:sep])
    except AttributeError:
        raise error.PipelineClassNotFoundError('Could not find class %s in module %s'
                                               % (cls_path[sep + 1:], cls_path[:sep]))

    if not shared:
        if not issubclass(cls, PipelineProcess):
            raise error.PipelineInvalidClassError('Class `%s` is not a valid class (not a child of etl.PipelineProcess)'
                                                  % cls.__name__)

    return cls


def validate_class_args(cls, cls_args):
    # Check arguments
    arg_spec = inspect.getargspec(cls.__init__)
    args = arg_spec[0]
    args.remove('self')
    args_defaults = arg_spec[3]
    for cls_arg in cls_args.keys():
        if cls_arg not in args:
            raise error.PipelineInvalidClassArgumentsError('Argument `%s` is not an argument of `%s.__init__`'
                                                           % (cls_arg, cls.__name__))

    min_arg_no = (len(args) - len(args_defaults)) if args_defaults else len(args)
    if len(cls_args) < min_arg_no:
        raise error.PipelineInvalidClassArgumentsError('Not enough arguments supplied! Class `%s` expects at least the '
                                                       'following arguments: `%s`'
                                                       % (cls.__name__,
                                                          '` `'.join([args[i] for i in range(min_arg_no)])))

    return cls_args
