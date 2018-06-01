from pyspark.sql.functions import monotonically_increasing_id

from sparklanes import Task
from sparklanes._framework.spark import session as spark


@Task('extract_data')
class ExtractIrisCSVData(object):
    def __init__(self, iris_csv_path):
        self.iris_csv_path = iris_csv_path

    def extract_data(self):
        # Read the csv
        iris_df = spark.read.csv(path=self.iris_csv_path,
                                 sep=',',
                                 header=True,
                                 inferSchema=True)

        # Make it available to tasks that follow
        self.cache('iris_df', iris_df)


@Task('add_index')
class AddRowIndex(object):
    def add_index(self):
        # Add id column
        self.iris_df = self.iris_df.withColumn('id', monotonically_increasing_id())

        # Update cache
        self.cache('iris_df', self.iris_df)


@Task('normalize')
class NormalizeColumns(object):
    def normalize(self):
        # Add normalized columns
        columns = self.iris_df.columns
        columns.remove('species')
        for col in columns:
            col_min = float(self.iris_df.agg({col: "min"}).collect()[0]['min(%s)' % col])
            col_max = float(self.iris_df.agg({col: "max"}).collect()[0]['max(%s)' % col])
            self.iris_df = self.iris_df.withColumn(col + '_norm', (self.iris_df[col] - col_min) / (col_max - col_min))

        # Update Cache
        self.cache('iris_df', self.iris_df)


@Task('write_to_json')
class SaveAsJSON(object):
    def __init__(self, output_folder):
        self.output_folder = output_folder

    def write_to_json(self):
        self.iris_df.write.format('json').save(self.output_folder)

        # Clear cache
        self.uncache('iris_df')
