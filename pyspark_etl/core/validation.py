import inspect
from importlib import import_module

from schema import Schema, SchemaError, Optional, Or

from core import errors
from core.base import PipelineProcessBase


def validate_pipeline_dict_schema(pipeline_dict):
    extract_format = {'data_frame_name': str, 'class': str, Optional('kwargs'): Or({str: object}, None)}
    transform_load_format = {'class': str, Optional('kwargs'): Or({str: object}, None)}
    shared_format = {'resource_name': str, 'class': str, Optional('kwargs'): Or({str: object}, None)}

    schema = Schema({'processes': {'extract': Or([extract_format], extract_format),
                                   Optional('transform'): Or([transform_load_format], transform_load_format),
                                   'load': Or([transform_load_format], transform_load_format)},
                     Optional('shared'): Or([shared_format], shared_format)})
    try:
        return schema.validate(pipeline_dict)
    except SchemaError as e:
        raise errors.PipelineSchemaError(**e.__dict__)


def validate_and_get_class(cls_path, shared=False):
    sep = cls_path.rfind('.')
    try:
        cls = getattr(import_module(cls_path[:sep]), cls_path[sep + 1:])
    except ImportError:
        raise errors.PipelineModuleNotFoundError('Could not find module %s' % cls_path[:sep])
    except AttributeError:
        raise errors.PipelineClassNotFoundError('Could not find class %s in module %s'
                                                % (cls_path[sep + 1:], cls_path[:sep]))

    if not shared:
        if not issubclass(cls, PipelineProcessBase):
            raise errors.PipelineInvalidClassError('Class `%s` is not valid (not a child of etl.PipelineProcessBase)'
                                                   % cls.__name__)

    return cls


def validate_class_args(cls, passed_args):
    # Inspect class arguments
    super_cls_args = inspect.getargspec(PipelineProcessBase.__init__)[0]
    arg_spec = inspect.getargspec(cls.__init__)
    args = arg_spec[0]
    for arg in args:
        if arg in super_cls_args:
            args.remove(arg)
    args_defaults = arg_spec[3]
    min_arg_no = (len(args) - len(args_defaults)) if args_defaults else len(args)

    # Check if minimum amount arguments is present
    if len(passed_args) < min_arg_no:
        raise errors.PipelineInvalidClassArgumentsError('Not enough arguments supplied! Class `%s` expects at least the'
                                                        ' following arguments: `%s`'
                                                        % (cls.__name__,
                                                           '` `'.join([args[i] for i in range(min_arg_no)])))
    else:  # Check if all required arguments are present
        for required_arg in args[:min_arg_no]:
            if required_arg not in passed_args.keys():
                raise errors.PipelineInvalidClassArgumentsError(
                    'Required argument `%s` of `%s.__init__` is not present in '
                    'the pipeline definition' % (required_arg, cls.__name__))

    # Check if non-existing arguments are passed
    for passed_arg in passed_args.keys():
        if passed_arg not in args:
            raise errors.PipelineInvalidClassArgumentsError('Argument `%s` is not an argument of `%s.__init__`'
                                                            % (passed_args, cls.__name__))

    return passed_args