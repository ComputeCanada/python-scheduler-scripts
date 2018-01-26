Overview
========

A collection of scripts for examining slurm scheduler data.


Requirements
============

Installation
===============================

To install to your home directory run the below command from inside the `python-schedular-scripts` directory:

```
$ python setup.py install --user
```

The `--record filename` option will output a list of files created during install to the given `filename`.


Usage
=====

queuewait
---------
Adds a column to sacct indicating the time spent waiting in the queue

To see usage and options information run:

```
queuewait -h
```

sacct-all
---------
Shows all sacct info for a given job in an easier to read format

To see usage and options information run:

```
sacct-all -h
```

Development/Debugging Notes
===========================
