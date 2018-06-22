from setuptools import setup

setup(
    name='sparklanes',
    version='0.2.3',
    url='https://github.com/ksbg/sparklanes',
    project_urls={
        'sparklanes documentation': 'https://sparklanes.readthedocs.io/',
    },
    author='Kevin Baumgarten',
    author_email='kevin@ksbg.io',
    description='A lightweight framework to build and execute data processing pipelines in pyspark '
                '(Apache Spark\'s python API)',
    long_description='sparklanes is a lightweight data processing framework for Apache Spark'
                     'written in Python. It was built with the intention to make building'
                     'complex spark processing pipelines simpler, by shifting the focus'
                     'towards writing data processing code without having to spent much time'
                     'on the surrounding application architecture.'
                     '\n'
                     'Data processing pipelines, or *lanes*, are built by stringing together'
                     'encapsulated processor classes, which allows creation of lane definitions'
                     'with an arbitrary processor order, where processors can be easily'
                     'removed, added or swapped.'
                     '\n'
                     'Processing pipelines can be defined using *lane configuration YAML files*,'
                     'to then be packaged and submitted to spark using a single command.'
                     'Alternatively, the same can be achieved manually by using the framework'
                     'API.',
    long_description_content_type='text/markdown',
    packages=['sparklanes', 'sparklanes._submit', 'sparklanes._framework'],
    install_requires=[
        'py4j==0.10.6',
        'pyspark==2.3.0',
        'PyYAML==3.12',
        'schema==0.6.7',
        'six==1.11.0',
    ],
    package_data={'sparklanes._submit': ['requirements-submit.txt']},
    entry_points={'console_scripts': ['lane-submit=sparklanes._submit.submit:submit_to_spark']},
    classifiers=[
        'Development Status :: 3 - Alpha',
        'Intended Audience :: Developers',
        'Topic :: Scientific/Engineering :: Artificial Intelligence',
        'License :: OSI Approved :: MIT License',
        'Operating System :: OS Independent',
        'Programming Language :: Python :: 2.7',
        'Programming Language :: Python :: 3.4',
        'Programming Language :: Python :: 3.5',
        'Programming Language :: Python :: 3.6',
    ],
    keywords=['spark', 'pyspark', 'data', 'processing', 'preprocessing', 'pipelines'],
    python_requires='>=2.7, !=3.0.*, !=3.1.*, !=3.2.*, !=3.3.*, <4',

)
