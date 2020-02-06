from setuptools import setup

setup(
    name='MonSTer',
    version='0.1',
    description='Monitoring Framework for SLURM',
    author='Jie Li',
    author_email='jie.li@ttu.edu',
    packages=['monster'],
    include_package_data=True,
    py_modules=[
        'pyslurm'
    ]
)