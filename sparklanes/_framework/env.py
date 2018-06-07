"""Environment configuration variables that can be passed when executing a lane."""
import os

INTERNAL_LOGGER_NAME = 'SPARKLANES'
"""The logger's name under which internal events will be logged."""

SPARK_APP_NAME = 'sparklanes.app'
"""The app name using which the default SparkContext/SparkSession will be instantiated"""

UNNAMED_LANE_NAME = 'UnnamedLane'
"""Default name of a lane, if no custom name is specified."""

UNNAMED_BRANCH_NAME = 'UnnamedBranch'
"""Default name of a branch, if no custom branch is specified"""

VERBOSE_TESTING = False
"""Specifies if output should be printed to console when running tests"""

INIT_SPARK_ON_IMPORT = True
"""Specifies if a default SparkContext/SparkSession should be instantiated upon import sparklanes"""

for v in locals().copy():
    if not v.startswith('__') and os.getenv(v):
        env_var = os.getenv(v)
        if env_var:
            var_type = type(locals()[v])
            try:
                if var_type == bool:
                    locals()[v] = bool(int(env_var))
                else:
                    locals()[v] = type(var_type)(env_var)
            except (ValueError, TypeError) as exc:
                raise ValueError('Invalid value for environment variable `%s`. Must be `%s`.'
                                 % (v, str(var_type) if not var_type == bool else '0/1'))
