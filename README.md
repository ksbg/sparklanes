sparklanes
==========

[![PyPI version](https://badge.fury.io/py/sparklanes.svg)](https://badge.fury.io/py/sparklanes)
[![Build Status](https://travis-ci.org/ksbg/sparklanes.svg?branch=master)](https://travis-ci.org/ksbg/sparklanes?branch=master)
[![Coverage Status](https://coveralls.io/repos/github/ksbg/sparklanes/badge.svg?branch=master)](https://coveralls.io/github/ksbg/sparklanes?branch=master)
[![Doc status](https://sparklanes.readthedocs.io/en/latest/?badge=latest)](https://sparklanes.readthedocs.io)
![pylint Score](https://mperlet.github.io/pybadge/badges/9.88.svg)
![license](https://img.shields.io/github/license/ksbg/sparklanes.svg)


sparklanes is a lightweight data processing framework for Apache Spark
written in Python. It was built with the intention to make building
complex spark processing pipelines simpler, by shifting the focus
towards writing data processing code without having to spent much time
on the surrounding application architecture.

Data processing pipelines, or *lanes*, are built by stringing together
encapsulated processor classes, which allows creation of lane definitions
with an arbitrary processor order, where processors can be easily
removed, added or swapped.

Processing pipelines can be defined using *lane configuration YAML files*,
to then be packaged and submitted to spark using a single command.
Alternatively, the same can be achieved manually by using the framework's
API.

Documentation
-------------

Find the documentation on [sparklanes.readthedocs.io](https://sparklanes.readthedocs.io)
