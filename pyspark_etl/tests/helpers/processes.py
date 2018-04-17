from core.base import PipelineProcessBase
from core.shared import Shared


class ProcessNotInherited(object):
    pass


class ProcessWithoutArgs(PipelineProcessBase):
    def __init__(self):
        super(ProcessWithoutArgs, self).__init__()

    def run(self):
        pass


class ProcessWithOnePositionalArg(PipelineProcessBase):
    def __init__(self, a):
        super(ProcessWithOnePositionalArg, self).__init__()

    def run(self):
        pass


class ProcessWithOneOptionalArg(PipelineProcessBase):
    def __init__(self, a=None):
        super(ProcessWithOneOptionalArg, self).__init__()

    def run(self):
        pass


class ProcessWithOnePositionalAndTwoOptionalArgs(PipelineProcessBase):
    def __init__(self, a, b=None, c=None):
        super(ProcessWithOnePositionalAndTwoOptionalArgs, self).__init__()

    def run(self):
        pass


class ProcessWithThreePositionalArgs(PipelineProcessBase):
    def __init__(self, a, b, c):
        super(ProcessWithThreePositionalArgs, self).__init__()

    def run(self):
        pass


class ProcessWithThreeOptionalArgs(PipelineProcessBase):
    def __init__(self, a=None, b=None, c=None):
        super(ProcessWithThreeOptionalArgs, self).__init__()

    def run(self):
        pass


class ProcessIncreaseSharedValueBy1(PipelineProcessBase):
    def __init__(self, resource_name):
        self.resource_name = resource_name
        super(ProcessIncreaseSharedValueBy1, self).__init__()

    def run(self):
        Shared.resources[self.resource_name] += 1


class ProcessSetSharedValueToString(PipelineProcessBase):
    def __init__(self, resource_name):
        self.resource_name = resource_name
        super(ProcessSetSharedValueToString, self).__init__()

    def run(self):
        Shared.resources[self.resource_name] = 'a string'


class ProcessAddSharedValue(PipelineProcessBase):
    def __init__(self, resource_name):
        self.resource_name = resource_name
        super(ProcessAddSharedValue, self).__init__()

    def run(self):
        Shared.resources[self.resource_name] = 'new value'


class ProcessDeleteSharedValue(PipelineProcessBase):
    def __init__(self, resource_name):
        self.resource_name = resource_name
        super(ProcessDeleteSharedValue, self).__init__()

    def run(self):
        del Shared.resources[self.resource_name]


class ProcessCheckIfSharedValueExists(PipelineProcessBase):
    def __init__(self, resource_name):
        self.resource_name = resource_name
        super(ProcessCheckIfSharedValueExists, self).__init__()

    def run(self):
        try:
            Shared.resources[self.resource_name]
        except KeyError as e:
            raise e


class ProcessAlphabetBuilderABCDE(PipelineProcessBase):
    def __init__(self):
        super(ProcessAlphabetBuilderABCDE, self).__init__()

    def run(self):
        Shared.data_frames['Alphabet'] = 'ABCDE'


class ProcessAlphabetBuilderFGHIJ(PipelineProcessBase):
    def __init__(self):
        super(ProcessAlphabetBuilderFGHIJ, self).__init__()

    def run(self):
        Shared.data_frames['Alphabet'] += 'FGHIJ'


class ProcessAlphabetBuilderKLMNO(PipelineProcessBase):
    def __init__(self):
        super(ProcessAlphabetBuilderKLMNO, self).__init__()

    def run(self):
        Shared.data_frames['Alphabet'] += 'KLMNO'


class ProcessAlphabetBuilderPQRST(PipelineProcessBase):
    def __init__(self):
        super(ProcessAlphabetBuilderPQRST, self).__init__()

    def run(self):
        Shared.data_frames['Alphabet'] += 'PQRST'


class ProcessAlphabetBuilderUVWXY(PipelineProcessBase):
    def __init__(self):
        super(ProcessAlphabetBuilderUVWXY, self).__init__()

    def run(self):
        Shared.data_frames['Alphabet'] += 'UVWXY'


class ProcessAlphabetBuilderZ(PipelineProcessBase):
    def __init__(self):
        super(ProcessAlphabetBuilderZ, self).__init__()

    def run(self):
        Shared.data_frames['Alphabet'] += 'Z'
