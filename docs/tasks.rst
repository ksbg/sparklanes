=============================
Writing Data Processing Tasks
=============================

Creating a Task
===============

In sparklanes, data processing tasks exist as decorated classes. Best practice suggests, that a task
should depend as little as possible on other tasks, in order to allow for lane definitions
with an arbitrary processor order (up to a certain extent, because of course there will always be some
dependence, since e.g. a task extracting data most likely comes before one transforming it).

For example:

.. code-block:: python

    from sparklanes import Task

    @Task('extract_data')
    class ExtractIrisCSVData(object):
        def __init__(self, iris_csv_path):
            self.iris_csv_path = iris_csv_path

        def extract_data(self):
            # Read the csv
            iris_df = conn.spark.read.csv(path=self.iris_csv_path,
                                          sep=',',
                                          header=True,
                                          inferSchema=True)

The class :code:`ExtractIrisCSVData` above becomes a *Task* by being decorated with
:func:`sparklanes.Task`. Tasks have an *entry*-method, which is the method that will be run during
lane execution, and is specified using the :code:`Task` decorator's sole argument
(in this case, :code:`extract_data`).

The entry-method itself should not take arguments, however custom arguments can be passed to the
class during instantiation.

.. todo::

    The functionality to pass args/kwargs to both the constructor, as well as to the entry method,
    might be added in future versions.

Sharing Resources between Tasks
===============================

By being decorated, the class becomes a child of the internal
:class:`sparklanes._framework.task.LaneTask` class and inherits the
:meth:`sparklanes._framework.task.LaneTask.cache`,
:meth:`sparklanes._framework.task.LaneTask.uncache` and
:meth:`sparklanes._framework.task.LaneTask.clear_cache` methods, which can be used to add an
object to the `TaskCache`.

When object is cached from within a task (e.g.
using :code:`self.cache('some_df', df)`, it becomes an attribute to all tasks that follow and is
accessible from within each task object as :code:`self.some_df` (that is, until it is uncached).

Accessing the Pyspark API from within Tasks
===========================================

To allow for easy access to the Pyspark API across tasks, sparklanes offers means to avoid having
to "getOrCreate" a :code:`SparkContext`/:code:`SparkSession` in each task requiring access to one. A
module containing tasks can simply import :class:`sparklanes.conn` (an alias of
:class:`sparklanes.SparkContextAndSessionContainer`) and have immediate access to a SparkContext
and SparkSession object:

.. code-block:: python

    from sparklanes import conn

    conn.sc     # SparkContext instance
    conn.spark  # SparkSession instance

The currently active Context/Session can be changed using its methods
:meth:`sparklanes.SparkContextAndSessionContainer.set_sc` and
:meth:`sparklanes.SparkContextAndSessionContainer.set_spark`

If it is preferred to handle SparkContexts/SparkSessions manually, without making use of the shared
container, this can be done by setting an environment variable :code:`INIT_SPARK_ON_IMPORT` to
:code:`0` when submitting the application to spark.
