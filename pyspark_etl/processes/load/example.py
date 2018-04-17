from core.base import PipelineProcessBase
from core.shared import Shared


class TestLoader(PipelineProcessBase):
    def __init__(self, opt_arg1=1, opt_arg2=2):
        super(TestLoader, self).__init__()

    def run(self):
        print('TestLoader running')
        Shared.resources['tests'] += 100
        print(Shared.resources['tests'])
