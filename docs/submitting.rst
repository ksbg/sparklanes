Submitting lanes to Spark
===================================

Console script
--------------
sparklanes comes with a console script to package and submit a YAML lane to Spark:

.. program-output:: lane-submit -h


Packaging
---------

When submitting a YAML lane configuration file to spark, the python package containing the tasks
(i.e. the data processors) has to be specified. While there is no strict requirement anymore for
python packages to have a :code:`__init__.py` for Python version :code:`3.4+`, it remains a
requirement here.

For example, if a YAML file contains tasks like:

.. code-block:: yaml

    tasks:
    - class: tasks.extract.LoadFromS3
    - class: tasks.extract.LoadFromMySQL
    - class: tasks.transform.NormalizeColumns
    - class: tasks.transform.EngineerFeatures
    - class: tasks.load.DumpToFTP

Then the directory structure of the python package specified using :code:`-p` should look something
like this:

.. code-block:: yaml

    tasks/
      __init__.py
      extract.py  # Contains LoadFromS3 and LoadFromMySQL classes
      transform.py  # Contains NormalizeColumns and EngineerFeatures classes
      load.py  # Contains DumpToFTP class

Extra Data
----------

If any additional data needs to be accessible locally from within the spark cluster, they can be
specified using :code:`-e/--extra-data`. Both files and directories are supported, and they will be
accessible relative to the application's root directory.

For example, a single file, as in :code:`-e example.csv`, will be made accessible from spark at
:code:`./example.csv`, regardless from the original directory structure. If a directory is
specified, e.g. :code:`-e extra/data`, that folder will be accessible from spark at :code:`./data`.


Spark Configuration
---------------------------

Any flags and configuration arguments accepted by
`spark-submit <https://spark.apache.org/docs/latest/submitting-applications.html>`_ can also be
used using :code:`lane-submit`.

For example, :code:`spark-submit` configuration arguments could look like:

.. code-block:: bash

    spark-submit --properties-file ./spark.conf --executor-memory 20G --supervise [...]

Then those same arguments could be passed using :code:`lane-submit` like:

.. code-block:: bash

    lane-submit -s properties-file=./spark.conf executor-memory=20G supervise [...]



Custom main
-----------

The default main python file is a simple script loading and executing the lane:

.. literalinclude:: ../sparklanes/_submit/_main.py

If this is not sufficient, the script can be extended and the new python script specified as a
custom main file as in :code:`-m new_main.py`.
