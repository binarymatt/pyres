try:
    from setuptools import setup
except ImportError:
    from distutils.core import setup
version='0.5.0'
setup(
    name='pyres',
    version=version,
    description='Python Resque clone',
    author='Matt George',
    author_email='mgeorge@gmail.com',
    maintainer='Matt George',
    license='MIT',
    url='http://github.com/binarydud/pyres',
    packages=['pyres', 'resweb', 'pyres/failure'],
    download_url='http://github.com/binarydud/pyres/tarball/v%s' % version,
    package_data={'resweb': ['templates/*.mustache','media/*']},
    include_package_data=True,
    scripts=['scripts/pyres_worker', 'scripts/pyres_web'],
    zip_safe = True,
    install_requires=[
        'simplejson>=2.0.9',
        'itty>=0.6.2',
        'redis>=0.6.0',
        'pystache>=0.1.0'
    ],
    classifiers = [
            'Development Status :: 4 - Beta',
            'Environment :: Console',
            'Intended Audience :: Developers',
            'License :: OSI Approved :: MIT License',
            'Operating System :: OS Independent',
            'Programming Language :: Python'],
)
