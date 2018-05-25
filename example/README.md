Iris example
=====================

This example shows how a ETL lane can be implemented using the pyspark-etl framework.

We are going to use the [Iris flower data set](https://en.wikipedia.org/wiki/Iris_flower_data_set)
and apply some very simple transformations to it, before writing the result to disk.

Our ETL lane will use a sharable object `TimeRecorder`, which can be used to measure
execution time of each of our processors. The lane will perform the following steps:

- _Extract_: Read the data set CSV file from disk and store it as a spark DataFrame
- _Transform_: Normalize numerical columns
- _Transform_: Add a row-index column
- _Load_: Write the transformed data set to disk as JSON

Our `TimeRecorder` is a simple object with two methods `register_start` and `register_end`, which 
can be used to indicate when a processor starts and finishes execution.

Its implementation can be found [here](../pyspark_etl/shared/example/iris.py)

Writing the Processor
=====================

Unlike generic shared objects, processors must inherit from the abstract class `PipelineProcessorBase`
 and implement the abstract method `run()`, which will be called upon lane execution.

First, let's write our extract processor. We'll name it `ExtractIrisCSVData`.

```python
from etl.base import PipelineProcessorBase
from etl.shared import Shared


class ExtractIrisCSVData(PipelineProcessorBase):
    def __init__(self, iris_csv_path, record_time=False, **kwargs):
        self.iris_csv_path = iris_csv_path
        self.record_time = record_time
        super(ExtractIrisCSVData, self).__init__(**kwargs)

    def run(self):
        # Record start time
        if self.record_time:
            Shared.get_resource('time_recorder').register_start(self.__class__.__name__)

        # Read the csv
        iris_df = self.spark.read.csv(path=self.iris_csv_path,
                                      sep=',',
                                      header=True,
                                      inferSchema=True)

        # Store in Shared class, so it can be accessed by other processors
        Shared.add_data_frame(name='iris',
                              df=iris_df)

        # Record end time
        if self.record_time:
            Shared.get_resource('time_recorder').register_end(self.__class__.__name__)
```

Note that the super class constructor *has* to be called using the not further specified, unpacked `kwargs`.

Our processor has one required argument `iris_csv_path`, and one optional one, `record_time`. 
This means, that when we later define our lane, we have to at least specify the required
argument for validation to succeed.

In the run method, we access our shared `TimeRecorder` object using `Shared.get_resource`, to let it
know that our processor has started execution. We then create a new DataFrame using pyspark's API
and make the resulting DataFrame accessible to other processors using `Shared.add_data_frame`. Finally
we let it be known that execution has stopped.

Next, we normalize our data using a transformer, which we'll simply call `NormalizeColumns`:

```python
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
```

The code speaks for itself. The other transformer, and the two loaders follow the same pattern. Just always make sure to
update any shared objects that are being changed (as it is done here: `Shared.update_data_frame('iris', df)`)

Implementaitons here: [ExtractIrisCSVData](../pyspark_etl/processes/extract/example/iris.py) 
[NormalizeColumns](../pyspark_etl/processes/extract/example/iris.py)
[AddRowIndex](../pyspark_etl/processes/extract/example/iris.py)
[SaveAsJSON](../pyspark_etl/processes/extract/example/iris.py)

Defining the pipeline
-------

We'll define the lane configuration file as follows:

```yaml
processors:
  extract:
    class: processors.extract.example.iris.ExtractIrisCSVData
    kwargs:
      iris_csv_path: ../examples/iris.csv                             
  transform:
    - class: processors.transform.example.iris.NormalizeColumns
      kwargs:
        record_time: true                                        
    - class: processors.transform.example.iris.AddRowIndex
      kwargs:
        record_time: true                                            
  load:
    - class: processors.load.example.iris.SaveAsJSON
      kwargs:
        output_folder: ../dist/out                                     
    - class: processors.load.example.iris.SaveTimeLogsFromTimeRecorder
      kwargs:
        log_file_path: ../dist/time_logs.txt                           
shared:
  - resource_name: time_recorder                                       
    class: shared.example.iris.TimeRecorder                            
```

Each of the processors will be run in subsequent order. The shared object will be made accessible
to all processors.

After that, we can check out the folder we defined in `output_folder` as a kwarg for the `SaveAsJSON` processor, in order
to find the generated JSON file, as well as the logs generated by `TimeRecorder`.

Run it
-------

In the project root, simply run make build submit `examples/iris.yaml` to execute the lane.