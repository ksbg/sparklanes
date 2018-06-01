"""Environment configuration variables that can be passed when executing a lane"""
import os

INTERNAL_LOGGER_NAME = 'SPARKLANES'  # Name of the logger to be used for internal logging
SPARK_APP_NAME = 'sparklanes.app'
UNNAMED_LANE_NAME = 'UnnamedLane'  # Default name if a lane isn't given a name
UNNAMED_BRANCH_NAME = 'UnnamedBranch'  # Default name if a branch isn't given a name
VERBOSE_TESTING = False  # Do not show console output during test if False
INIT_SPARK_ON_IMPORT = True  # Initiate a spark sc & spark on import

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
