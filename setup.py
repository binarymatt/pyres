import sys
from setuptools import setup, find_packages
from setuptools.command.test import test as TestCommand

requires=[
    item for item in
    open("requirements.txt").read().split("\n")
    if item]

if sys.version_info[0:2] == (2,6):
    requires.append('ordereddict')

class PyTest(TestCommand):
    def finalize_options(self):
        TestCommand.finalize_options(self)
        self.test_args = []
        self.test_suite = True

    def run_tests(self):
        #import here, cause outside the eggs aren't loaded
        import pytest
        result = pytest.main(self.test_args)
        sys.exit(result)

version='1.5'
setup(
    name='pyres',
    version=version,
    description='Python resque clone',
    author='Matt George',
    author_email='mgeorge@gmail.com',
    maintainer='Matt George',
    license='MIT',
    url='http://github.com/binarydud/pyres',
    packages=find_packages(exclude=['ez_setup', 'examples', 'tests']),
    download_url='http://pypi.python.org/packages/source/p/pyres/pyres-%s.tar.gz' % version,
    include_package_data=True,
    package_data={'': ['requirements.txt']},
    entry_points = """\
    [console_scripts]
    pyres_manager=pyres.scripts:pyres_manager
    pyres_scheduler=pyres.scripts:pyres_scheduler
    pyres_worker=pyres.scripts:pyres_worker
    """,
    tests_require=requires + ['pytest',],
    cmdclass={'test': PyTest},
    install_requires=requires,
    classifiers = [
            'Development Status :: 4 - Beta',
            'Environment :: Console',
            'Intended Audience :: Developers',
            'License :: OSI Approved :: MIT License',
            'Operating System :: OS Independent',
            'Programming Language :: Python :: 2.6',
            'Programming Language :: Python :: 2.7',
            'Programming Language :: Python :: 3.3',
            'Programming Language :: Python'],
)
