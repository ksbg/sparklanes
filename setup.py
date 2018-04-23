from setuptools import setup, find_packages

setup(
    name='pyspark-etl',
    version='0.1',
    url='https://github.com/ksbg/pyspark-etl',
    author='Kevin Baumgarten',
    author_email='kevin@ksbg.io',
    description='A framework to build ETL pipelines using Apache Spark\'s python API',
    packages=find_packages(),
)