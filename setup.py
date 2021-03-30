from os import path, system
from setuptools import setup, Command, find_packages

from pip._internal.req import parse_requirements

here = path.abspath(path.dirname(__file__))

# Get the long description from the README file
with open(path.join(here, 'README.md'), encoding='utf-8') as f:
    long_description = f.read()


requirements = [str(ir.requirement) for ir in parse_requirements(
    'requirements.txt', session=False)]


class CleanCommand(Command):
    """Custom clean command to tidy up the project root."""

    user_options = []

    def initialize_options(self):
        pass

    def finalize_options(self):
        pass

    def run(self):
        system(
            'rm -vrf ./build ./dist ./*.pyc ./*.tgz ./*.egg-info ./htmlcov '
            './spark-warehouse ./driver/spark-warehouse ./metastore_db ./coverage_html ./.pytest_cache ./derby.log ./tests/local_results ./tasks/__pycache__')


setup(
    name='aws-glue-local-development',
    version='1.0.0',
    description='Calculate movie analytic job',
    long_description=long_description,
    author='David Greenshtein',
    author_email='dgreensh@amazon.de',
    packages=find_packages(exclude=('contrib', 'docs', 'tests',)),
    install_requires=requirements,
    include_package_data=True,
    platforms='any',
    license='MIT-0',
    zip_safe=False,
    cmdclass={
         'clean_all': CleanCommand
    })
