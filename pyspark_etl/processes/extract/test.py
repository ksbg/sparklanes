from core.base import PipelineProcessBase
from core.shared import Shared


class TestExtractor(PipelineProcessBase):
    def __init__(self, arg1, arg2, opt_arg_1=None, opt_arg_2=None):
        super(TestExtractor, self).__init__()

    def run(self):
        print('TestExtractor running')
        Shared.add_resource('test', 100)
        print(Shared.resources['test'])

