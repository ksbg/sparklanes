import os
from setuptools import setup

with open('requirements.txt', 'r') as req_file:
    requirements = req_file.read().splitlines()

with open(os.path.join(os.path.dirname(os.path.abspath(__file__)), 'README.md')) as readme_file:
    long_description = readme_file.read()

setup(
    name='sparklanes',
    version='0.2.0',
    url='https://github.com/ksbg/sparklanes',
    download_url='https://github.com/ksbg/sparklanes/archive/0.2.zip',
    author='Kevin Baumgarten',
    author_email='kevin@ksbg.io',
    description='A lightweight framework to build and execute data processing pipelines in pyspark '
                '(Apache Spark\'s python API)',
    long_description=long_description,
    long_description_content_type='text/markdown',
    packages=['sparklanes', 'sparklanes._submit', 'sparklanes._framework'],
    install_requires=requirements,
    package_data={'sparklanes._submit': ['requirements-submit.txt']},
    entry_points={'console_scripts': ['lane-submit=sparklanes._submit.submit']},
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
