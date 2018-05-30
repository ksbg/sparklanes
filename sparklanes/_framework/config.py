import os

INTERNAL_LOGGER_NAME = 'sparklanes'
UNNAMED_LANE_NAME = 'UnnamedLane'
UNNAMED_BRANCH_NAME = 'UnnamedBranch'
VERBOSE_TESTING = False

for v in locals().copy():
    if not v.startswith('__') and os.getenv(v):
        locals()[v] = os.getenv(v)
