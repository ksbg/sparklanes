Welcome to sparklanes's documentation!
======================================

sparklanes is a lightweight data processing framework for Apache Spark.
It was built with the intention to make building complex spark processing
pipelines simpler, by shifting the focus towards writing data processing
code without having to spent much time on the surrounding application
architecture.

Data processing pipelines, or *lanes*, are built by stringing together
encapsulated processor classes, which allows creation of lane definitions
with an arbitrary processor order, where processors can be easily
removed, added or swapped.

Processing pipelines can be defined using *lane configuration YAML files*,
to then be packaged and submitted to spark using a single command.
Alternatively, the same can be achieved manually by using the framework's
API.

.. toctree::
   :maxdepth: 2
   :caption: Contents:

   tasks
   lanes
   submitting
   api/index
   iris_example.ipynb


Indices and tables
==================

* :ref:`genindex`
* :ref:`modindex`
* :ref:`search`
