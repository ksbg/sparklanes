from core.base import PipelineProcessBase
from core.shared import Shared


class TestTransformer(PipelineProcessBase):
    def __init__(self, arg1, arg2):
        super(TestTransformer, self).__init__()

    def run(self):
        print('TestTransformer running')

