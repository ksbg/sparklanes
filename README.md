pyspark-etl
===========

pyspark-etl is an [ETL](https://en.wikipedia.org/wiki/Extract,_transform,_load) (Extract -> Transform -> Load) framework 
for _pyspark_ (Apache Spark's python API). Its goal is to allow you
to focus on the important tasks - writing data processors - without having to spend much time dealing with things such as
packaging your code for spark submission, stringing processors together, or designing your application architecture.

At their core, the data processors you will write are encapsulated and work independently from one another.
This means you can define pipelines with an arbitrary process order, and easily remove, add or swap out processors.

You can define pipelines using _pipeline configuration files_ (or manually, using the framework's API), to then package 
& submit it to Spark in a single command.

Contents
===============
* [pyspark-etl](#pyspark-etl)
* [Usage](#usage)
  * [Getting Started](#getting-started)
  * [Defining a Pipeline](#defining-a-pipeline)
  * [Writing Custom Processes](#writing-custom-processes)
  * [Sharing Objects between Processes](#sharing-objects-between-processes)
  * [Packaging and Submitting a Pipeline to Spark](#packaging-and-submitting-a-pipeline-to-spark)
  * [Spark Configuration](#spark-configuration)
* [Running tests](#running-tests)
* [Dependencies](#dependencies)      

Usage
=====
     
Getting Started
---------------

Check out the [example](examples/README.md), which shows how a simple ETL pipeline can be built.

Defining a Pipeline
-------------------
The simplest way to define pipeline is via _pipeline configuration
files_, which are schematized YAML files. The layout of a pipeline
configuration should look as follows:

```yaml
processes:                                        # (dict) (required) ETL processes
  extract:                                        # (list[dict] | dict) (required) Extract processes
    data_frame_name: df_name                      # (str) (required) The data frame name which the extractor will create
    class: processes.extract.ExtractClass         # (str) (required) Full path to the class, relative to module root
    kwargs:                                       # (dict) (optional) Keyword arguments used to instantiate the class
      kwarg_name_a: value_a                       # (str) (optional)  Keyword argument and its value
      kwarg_name_b: value_b                       # (str) (optional)  Keyword argument and its value
  transform:                                      # (list[dict] | dict) (optional) Transform processes
    - class: processes.transform.TransformClass1  # (required) (str) Full path to the class, relative to module root
      kwargs:                                     # (dict) (optional) Keyword arguments used to instantiate the class
        kwarg_name: value                         # (*) (optional) Keyword argument and its value
    - class: processes.transform.TransformClass2  # (required) (str) Full path to the class, relative to module root
  load:                                           # (dict) (required) Load processes
    - class: processes.load.LoadClass             # (str) (required) Full path to the class, relative to module root
      kwargs:                                     # (dict) (optional) Keyword arguments used to instantiate the class
        kwarg_name: value                         # (*) (optional)                                           
shared:                                           # (list[dict] | dict) (optional) Sharable objects
  - resource_name: res_name                       # (str) (required) Resource name under which process can access the object
    class: shared.SharedClass1                    # (str) (required) Full path to the class, relative to module root
    kwargs:                                       # (dict) (optional) Keyword arguments used to instantiate the class
        kwarg_name: value                         # (*) (optional) Keyword argument and its value                       
```

From that file, the pipeline will be created automatically. Each of the
classes will instantiated using the supplied keyword arguments, after
which the processes will run in subsequent order. If shared classes are
specified, they will also be instantiated and made accessible to each process.

Alternatively, the pipeline can be built and run using the framework's
API. The same pipeline could be built as follows:

```python
import processes 
import shared 
from core.pipeline import PipelineDefinition, Pipeline

# Define the pipeline
pd = PipelineDefinition()
pd.add_extractor(cls=processes.extract.ExtractClass, 
                 data_frame_name='df_name', 
                 kwargs={'kwarg_name_a': 'value_a', 'kwarg_name_b': 'value_b'})
pd.add_transformer(cls=processes.transform.TransformClass1,
                   kwargs={'kwarg_name': 'value'})
pd.add_transformer(cls=processes.transform.TransformClass2)
pd.add_loader(cls=processes.load.LoadClass, 
              kwargs={'kwarg_name': 'value'})
pd.add_shared(cls=shared.SharedClass1, 
              resource_name='res_name',
              kwargs={'kwarg_name': 'value'})

# Build the pipeline from the definition
pipeline = Pipeline(definition=pd)

# Run it
pipeline.run()
```

Writing Custom Processes
------------------------

Each process must inherit from the abstract class
`core.base.PipelineProcessBase` and implement the abstract method
`run()`, which will be called during pipeline execution. For example:

```python
from core.base import PipelineProcessBase


class CustomProcess(PipelineProcessBase):
    def __init__(self, custom_kwarg_1, custom_kwarg_2, **kwargs):
        self.custom_kwarg_1 = custom_kwarg_1
        self.custom_kwarg_2 = custom_kwarg_2
        super(CustomProcess, self).__init__(**kwargs)

    def run(self):
        # do stuff
        pass
```

That code has to be in some file inside the *processes* folder. This makes
sure that it will be packaged when being submitted to spark.

Sharing Objects between Processes
---------------------------------
As can be seen above, *shared* classes can be specified when defining a
pipeline. These will be instantiated and their objects made sharable
among processes. Shared classes can be any valid python classes and have no special requirements.

Inside a process, they can be accessed and manipulated as follows:

```python
from core.shared import Shared

Shared.get_resource(name='resource_name')  # Retrieve a shared resource
Shared.update_resource(name='resource_name', res=updated_resource)  # Update an existing shared resource
Shared.add_resource(name='new_resource_name', res=new_resource)  # Add a new shared resource
Shared.delete_resource(name='resource_name')  # Delete an existing shared resource
```

`Shared` also provides methods to share Spark `DataFrames` and `RDD`s
between processes (see the [shared module](pyspark_etl/core/shared.py)).


Packaging and Submitting a Pipeline to Spark
--------------------------------------------

First package the application:

    make build

Then submit a pipeline to spark

    make submit path/to/pipeline.yaml

Or do both in one command

    make build submit path/to/pipeline.yaml

Spark Configuration
-------------------

Any custom spark configurations should be specified in
[spark-config/spark.conf](spark-config/spark.conf). The log4j
configuration file is located at [spark-config/log4j-spark.properties](spark-config/log4j-spark.properties)


Running tests
=============

`make test` runs the entire test suite

Dependencies
============
Just make sure you have _pip_ installed, and the dependencies in
_submit-requirements.txt_ will be solved during packaging. Packages
mentioned in _requirements.txt_ are required to run the tests (install
using `pip install -r requirements.txt`)
