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
  * [Writing Custom Processors](#writing-custom-processors)
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

Writing Custom Processors
-------------------------

Sharing Objects between Processors
----------------------------------

Packaging and Submitting a Pipeline to Spark
--------------------------------------------

Spark Configuration
-------------------

Running tests
=============

Simply run `python -m unittest discover -v` inside the module root (`./pyspark_etl`) to run the entire test suite.

Dependencies
============
Just make sure you have _pip_ installed, and the dependencies in _requirements.txt_ will be solved during packaging.
