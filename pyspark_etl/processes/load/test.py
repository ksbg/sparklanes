from core.base import PipelineProcessBase
from core.shared import Shared


class TestLoader(PipelineProcessBase):
    def __init__(self, arg1, arg2, arg3, opt_arg_2=1):
        super(TestLoader, self).__init__()

    def run(self):
        print('TestLoader running')
        Shared.test += 100
        print(Shared.test)
