from setuptools import setup, find_packages
    
version='0.8'
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
    download_url='http://cloud.github.com/downloads/binarydud/pyres/pyres-%s.tar.gz' % version,
    include_package_data=True,
    package_data={'resweb': ['templates/*.mustache','media/*']},
    scripts=[
        'scripts/pyres_worker', 
        'scripts/pyres_web', 
        'scripts/pyres_scheduler',
        'scripts/pyres_manager'],
    install_requires=[
        'simplejson>=2.0.9',
        'itty>=0.6.2',
        'redis==1.34.1',
        'pystache>=0.1.0',
        'setproctitle==1.0'
    ],
    classifiers = [
            'Development Status :: 4 - Beta',
            'Environment :: Console',
            'Intended Audience :: Developers',
            'License :: OSI Approved :: MIT License',
            'Operating System :: OS Independent',
            'Programming Language :: Python'],
)