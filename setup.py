from setuptools import setup

with open('requirements.txt', 'r') as req:
    requirements = req.read().splitlines()

setup(
    name='sparklanes',
    version='0.2',
    url='https://github.com/ksbg/sparklanes',
    author='Kevin Baumgarten',
    author_email='kevin@ksbg.io',
    description='A lightweight framework to build and execute data processing pipelines in pyspark '
                '(Apache Spark\'s python API)',
    packages=['sparklanes', 'sparklanes.submit', 'sparklanes.framework'],
    package_data={'sparklanes.submit': ['default_log4j-spark.properties', 'default_spark.conf']},
    install_requires=requirements,
    entry_points={'console_scripts': ['lane-submit=sparklanes.submit.submit:_submit_to_spark']}
)
