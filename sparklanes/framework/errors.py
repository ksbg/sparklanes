from schema import SchemaError


class PipelineError(Exception):
    pass


class PipelineSchemaError(SchemaError):
    def __init__(self, *args, **kwargs):
        super(PipelineSchemaError, self).__init__(*args, **kwargs)

    @property
    def code(self):
        c = super(PipelineSchemaError, self).code
        return 'PipelineSchemaError: ' + c + '\nThe lane YAML file does not match the defined schema.'


class PipelineModuleNotFoundError(Exception):
    pass


class PipelineClassNotFoundError(Exception):
    pass


class PipelineInvalidClassError(Exception):
    pass


class PipelineInvalidClassArgumentsError(Exception):
    pass


class PipelineSharedResourceError(Exception):
    pass


class PipelineInvalidResourceNameError(Exception):
    pass


class PipelineSharedResourceNotFound(Exception):
    pass


class PipelineSharedResourceTypeInvalid(Exception):
    pass

class CacheError(AttributeError):
    pass

class TaskInitializationError(Exception):
    pass

class TaskBuilderError(Exception):
    pass

class LaneExecutionError(Exception):
    pass