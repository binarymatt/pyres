Pyres - a Resque clone
======================

[Resque](http://github.com/defunkt/resque) is a great implementation of a job queue by the people at github. It's written in ruby, which is great, but I primarily work in python. So I took on the task of porting over the code to python and PyRes was the result


## Project Goals

Because of some differences between ruby and python, there are a couple of places where I chose speed over correctness. The goal will be to eventually take the application and make it more pythonic without sacrificing the awesome functionality found in resque. At the same time, I hope to stay within the bounds of the original api and web interface.

## Travis CI

Currently, pyres is being tested via travis ci for python version 2.6, 2.7, and 3.3: 
[![Build Status](https://secure.travis-ci.org/binarydud/pyres.png)](http://travis-ci.org/binarydud/pyres)

## Running Tests

 1. Install nose: `$ easy_install nose`
 2. Start redis: `$ redis-server [PATH_TO_YOUR_REDIS_CONFIG]`
 3. Run nose: `$ nosetests` Or more verbosely: `$ nosetests -v`


##Mailing List

To join the list simply send an email to <pyres@librelist.com>. This
will subscribe you and send you information about your subscription,
include unsubscribe information.

The archive can be found at <http://librelist.com/browser/>.


## Information

* Code: `git clone git://github.com/binarydud/pyres.git`
* Home: <http://github.com/binarydud/pyres>
* Docs: <http://binarydud.github.com/pyres/>
* Bugs: <http://github.com/binarydud/pyres/issues>
* List: <pyres@librelist.com>

