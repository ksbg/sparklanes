from pysparketl import PipelineProcessorBase
from pysparketl import Shared


class SaveAsJSON(PipelineProcessorBase):
    def __init__(self, output_folder, record_time=False, **kwargs):
        self.output_folder = output_folder
        self.record_time = record_time
        super(SaveAsJSON, self).__init__(**kwargs)

    def run(self):
        # Record start time
        if self.record_time:
            Shared.get_resource('time_recorder').register_start(self.__class__.__name__)

        # Get DataFrame
        df = Shared.get_data_frame('iris')
        df.write.format('json').save(self.output_folder)

        # Record end time
        if self.record_time:
            Shared.get_resource('time_recorder').register_end(self.__class__.__name__)


class SaveTimeLogsFromTimeRecorder(PipelineProcessorBase):
    def __init__(self, log_file_path, **kwargs):
        self.log_file_path = log_file_path
        super(SaveTimeLogsFromTimeRecorder, self).__init__(**kwargs)

    def run(self):
        logs = str(Shared.get_resource('time_recorder'))
        with open(self.log_file_path, 'wt') as time_log_file:
            time_log_file.write(logs)
