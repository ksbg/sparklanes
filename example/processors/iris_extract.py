from sparklanes import Task, Shared
from sparklanes.spark import spark_session as spark


@Task('extract_data')
class ExtractIrisCSVData(object):
    def __init__(self, iris_csv_path, record_time=False):
        self.iris_csv_path = iris_csv_path
        self.record_time = record_time

    def extract_data(self):
        # Record start time
        if self.record_time:
            Shared.get_resource('time_recorder').register_start(self.__class__.__name__)

        # Read the csv
        iris_df = spark.read.csv(path=self.iris_csv_path,
                                 sep=',',
                                 header=True,
                                 inferSchema=True)

        # Store in Shared class, so it can be accessed by other processors
        Shared.add_data_frame(name='iris', df=iris_df)

        # Record end time
        if self.record_time:
            Shared.get_resource('time_recorder').register_end(self.__class__.__name__)
