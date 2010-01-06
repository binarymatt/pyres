from distutils.core import setup

setup(
    name='PyRes',
    version='0.4.0',
    description='Python Resque clone',
    author='Matt George',
    license='MIT',
    author_email='mgeorge@gmail.com',
    url='http://github.com/binarydud/pyres',
    packages=['pyres', 'resweb'],
    package_data={'resweb': ['templates/*.mustache','media/*']},
    scripts=['scripts/pyres_worker', 'scripts/pyres_web'],
    zip_safe = True,
    install_requires=[
        'simplejson>=2.0.9',
        'itty>=0.6.2',
        'redis>=0.6.0',
        'pystache>=0.1.0'
    ],
)
