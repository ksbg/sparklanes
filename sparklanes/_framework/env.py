import os

INTERNAL_LOGGER_NAME = 'SPARKLANES.LOG'
UNNAMED_LANE_NAME = 'UnnamedLane'
UNNAMED_BRANCH_NAME = 'UnnamedBranch'
VERBOSE_TESTING = False
INIT_SPARK_ON_START = True

for v in locals().copy():
    if not v.startswith('__') and os.getenv(v):
        locals()[v] = os.getenv(v)
