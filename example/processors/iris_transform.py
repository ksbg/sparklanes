from __future__ import division

from pyspark.sql.functions import monotonically_increasing_id

from pysparketl import PipelineProcessorBase
from pysparketl import Shared


class AddRowIndex(PipelineProcessorBase):
    def __init__(self, record_time, **kwargs):
        self.record_time = record_time
        super(AddRowIndex, self).__init__(**kwargs)

    def run(self):
        # Record start time
        if self.record_time:
            Shared.get_resource('time_recorder').register_start(self.__class__.__name__)

        # Get DataFrame
        df = Shared.get_data_frame('iris')

        # Add id column
        df = df.withColumn('id', monotonically_increasing_id())

        # Update DataFrame
        Shared.update_data_frame('iris', df)

        # Record end time
        if self.record_time:
            Shared.get_resource('time_recorder').register_end(self.__class__.__name__)


class NormalizeColumns(PipelineProcessorBase):
    def __init__(self, record_time, **kwargs):
        self.record_time = record_time
        super(NormalizeColumns, self).__init__(**kwargs)

    def run(self):
        # Record start time
        if self.record_time:
            Shared.get_resource('time_recorder').register_start(self.__class__.__name__)

        # Get DataFrame
        df = Shared.get_data_frame('iris')

        # Add normalized columns
        columns = df.columns
        columns.remove('species')
        for col in columns:
            col_min = float(df.agg({col: "min"}).collect()[0]['min(%s)' % col])
            col_max = float(df.agg({col: "max"}).collect()[0]['max(%s)' % col])
            df = df.withColumn(col + '_norm', (df[col] - col_min) / (col_max - col_min))

        # Update DataFrame
        Shared.update_data_frame('iris', df)

        # Record end time
        if self.record_time:
            Shared.get_resource('time_recorder').register_end(self.__class__.__name__)
