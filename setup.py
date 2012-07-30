from setuptools import setup, find_packages

version='1.4.1'
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
    install_requires=[
            item for item in
            open("requirements.txt").read().split("\n")
            if item],
    classifiers = [
            'Development Status :: 4 - Beta',
            'Environment :: Console',
            'Intended Audience :: Developers',
            'License :: OSI Approved :: MIT License',
            'Operating System :: OS Independent',
            'Programming Language :: Python'],
)
