"""Exceptions"""
from schema import SchemaError


class LaneSchemaError(SchemaError):
    """Should be thrown when a YAML definition does not match the required schema."""
    def __init__(self, *args, **kwargs):
        super(LaneSchemaError, self).__init__(*args, **kwargs)

    @property
    def code(self):
        """Show the specific error from the super class"""
        error = super(LaneSchemaError, self).code
        return 'PipelineSchemaError: ' + error + '\nYAML file does not match the defined schema.'


class LaneImportError(Exception):
    """Should be thrown when a module or class in a YAML definition file cannot be imported."""
    pass


class CacheError(AttributeError):
    """Should be thrown whenever task-cache access fails."""
    pass


class TaskInitializationError(Exception):
    """Should be thrown whenever transformation of a class into a task fails (during decoration)."""
    pass


class LaneExecutionError(Exception):
    """Should be thrown whenever execution of a lane fails."""
    pass
