from importlib import import_module

import yaml
from pyspark import SparkContext
from pyspark.sql import SparkSession
from schema import Schema, SchemaError, SchemaMissingKeyError

from .errors import LaneSchemaError, LaneImportError
from .lane import Branch, Lane
from .validation import validate_schema


def set_spark_context(new_sc):
    """
    Reassigns the default, globally accessible SparkContext

    Parameters
    ----------
    new_sc (pyspark.SparkContext)
    """
    if not isinstance(new_sc, SparkContext):
        raise TypeError('`new_sc` must be an instance of `pyspark.SparkContext`.')

    global spark_context
    spark_context = new_sc


def set_spark_session(new_spark):
    """
    Reassigns the default, globally accessible SparkSession

    Parameters
    ----------
    new_spark (pyspark.sql.SparkSession)
    """
    if not isinstance(new_spark, SparkSession):
        raise TypeError('`new_spark` must be an instance of `pyspark.sql.SparkSession`.')

    global spark_session
    spark_session = new_spark


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
        validate_schema(definition)
    except SchemaError as e:
        raise LaneSchemaError(**e.__dict__)

    def build(lb_def, branch=False):
        init_kwargs = {k: lb_def[k] for k in [kk for kk in ('run_parallel', 'name') if kk in lb_def]}
        lb = Lane(**init_kwargs) if not branch else Branch(**init_kwargs)

        for task in lb_def['tasks']:
            if 'branch' in task:
                branch_def = task['branch']
                lb.add(build(branch_def, True))
            else:
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
