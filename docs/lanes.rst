=========================
Defining Processing Lanes
=========================

Lanes (i.e. data processing pipelines) can be defined by either writing YAML definition files, or
by using sparklane's API.

YAML definition files
------------------------

Lane definition files must adhere to the following schema:

.. code-block:: yaml

    lane:
      name: SimpleLane          # str (optional): Name under which the lane will be referred to during logging
      run_parallel: false       # bool (optional): Indicates whether tasks should be run in parallel
      tasks:                    # list (required): List of processor classes
        - class: pkg.mdl.Task1  # str (required): Full python module path to the processor class
          args: [arg1, arg2]    # list (optional): List of arguments passed when instantiating the class
          kwargs:               # dict (optional): Dict of kwargs passed when instantiating the class
            kwarg1: val1
            kwarg2: val2
        - class: pkg.mdl.Task2
          args: [arg1, arg2]
        ...

Attention should be placed on using the correct class path. Let's say we have the following
directory structure:

.. code-block:: yaml

    tasks/
      extract/
        __init__.py
        extractors.py   # Contains class 'SomeExtractorClass'
      load/
          ...
      ...

The exact path to be used then depends on which folder will be packaged and submitted to spark. To
reference :code:`SomeExtractorClass`, the correct class path would be
:code:`tasks.extract.extractors.SomeExtractorClass` if the entire :code:`tasks` folder would be packaged and
submitted to Spark, whereas just packaging the :code:`extract` folder would result in the correct class
path :code:`extract.extractors.SomeExtractorClass` (see :doc:`submitting`).


Using the API
-------------

Lanes can also be defined and executed using sparklane's API, for example:

.. code-block:: python

    from sparklanes import Lane, Task

    @Task('main_mtd')
    class Task1(object):
        def main_mtd(self, a, b, c):
            pass

    @Task('main_mtd')
    class Task2(object):
        def main_mtd(self, a, b):
            pass

    # Building the lane
    lane = (Lane(name='ExampleLane', run_parallel=False)
            .add(Task1, 1, 2, c=3)
            .add(Task2, a=1, b=2))

    # Execute it
    lane.run()

Refer to the API documentation for :class:`sparklanes.Lane`.

Branching & Running Tasks in Parallel
-------------------------------------

Lanes can be branched infinitely deep, which is especially useful if part of the lane should be
executed in parallel. As stated in the
`Spark documentation
<http://spark.apache.org/docs/latest/job-scheduling.html#scheduling-within-an-application>`_:

.. code-block:: bash

    Inside a given Spark application (SparkContext instance), multiple parallel jobs can run
    simultaneously if they were submitted from separate threads.

If parameter :code:`run_parallel` is true when instantiating a :code:`Lane` or :code:`Branch`, a separate thread will
be spawned for each of the tasks it contains, ensuring that Spark will execute them in
parallel.

For example, a lane containing branches could look like this:

.. code-block:: python

   from sparklanes import Lane, Branch
   from pkg.mdl import Task1, Task2, Task3, SubTaskA, SubTaskB1, SubTaskB2, SubTaskC

   lane = (Lane(name='BranchedLane', run_parallel=False)
           .add(Task1)
           .add(Task2)
           .add(Branch(name='ExampleBranch', run_parallel=True)
                .add(SubTaskA)
                .add(Branch(name='SubBranch', run_parallel=False)
                     .add(SubTaskB1)
                     .add(SubTaskB2))
                .add(SubTaskC))
           .add(Task3))

Or the same lane defined as YAML:

.. code-block:: yaml

    lane:
      name: BranchedLane
      run_parallel: false
      tasks:
        - class: pkg.mdl.Task1
        - class: pkg.mdl.Task2
        - branch:
          name: ExampleBranch
          run_parallel: true
          tasks:
            - class: pkg.mdl.SubTaskA
            - branch:
              name: ExampleSubBranch
              run_parallel: false
              tasks:
              - class: pkg.mdl.SubTaskB1
              - class: pkg.mdl.SubTaskB2
            - class: pkg.mdl.SubTaskC

In this lane, :code:`SubTaskA`, Branch :code:`SubBranch` and :code:`SubTaskC` would be executed in parallel, whereas
the tasks within :code:`SubBranch` wouldn't be. This way, complex processing pipelines can be built.

Refer to :class:`sparklanes.Branch`.
