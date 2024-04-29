from setuptools import setup

with open('requirements.txt') as f:
    requirements = f.read().splitlines()

setup(
    name='MonSter',
    version='1.0.0',
    packages=['monster', 'mbuilder'],
    install_requires=requirements,
    url='https://github.com/nsfcac/MonSter',
    license='LICENSE',
    author='Jie Li',
    author_email='jie.li@ttu.edu',
    description='README.md'
)
