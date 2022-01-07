# -*- coding: utf-8 -*-


from setuptools import setup, find_packages


with open('README.md') as f:
    readme = f.read()

with open('LICENSE') as f:
    license = f.read()

setup(
    name='MonSter',
    version='0.1.0',
    description='Package for Collecting telemetry metrics from iDRAC9 and Slurm',
    long_description=readme,
    author='Jie Li',
    author_email='jie.li@ttu.edu',
    url='https://github.com/nsfcac/MonSter',
    license=license,
    packages=find_packages(exclude=('tests', 'docs')),
    install_requires=[
        "PyYAML",
        "tqdm",
        "requests",
        "psycopg2",
        "pgcopy",
        "aiohttp",
        "schedule",
        "async_retrying",
        "python-hostlist",
        "python-dateutil",
    ]
)