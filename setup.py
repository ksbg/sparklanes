from setuptools import setup, find_packages

with open('requirements.txt', 'r') as req:
    requirements = req.read().splitlines()

setup(
    name='pyspark-etl',
    version='0.1',
    url='https://github.com/ksbg/pyspark-etl',
    author='Kevin Baumgarten',
    author_email='kevin@ksbg.io',
    description='A lightweight ETL framework for pyspark (Apache Spark\'s python API)',
    packages=find_packages(exclude=['pysparketl.tests']),
    package_data={'pysparketl.submit': ['default_log4j-spark.properties',
                                         'default_spark.conf']},
    install_requires=requirements,
    entry_points={'console_scripts': ['etl-submit=pysparketl.submit:submit_to_spark_cmd']}
)