Dee, makes Python Relational
Copyright (C) 2007 Greg Gaughan
http://www.quicksort.co.uk

See DeeDoc.html for the user guide.


Installation
============
Run the setup.py program with the install option, e.g.

    python setup.py install
    
This makes the standard Dee modules available to your Python programs.


Quick start
===========
Dee can be used straight from the Python shell, or via a local web server and client.

Local Web Server
----------------
Run the demo web server. To start the server and a client run:

    python DeeWebDemo.py

The default browser should launch, pointing at http://localhost:8080, and a text box will be presented to allow Dee expressions to be entered. The expression can be evaluated by pressing the 'Evaluate' button. The default database can be changed by selecting from the drop-down list and pressing the 'Change database' button.

Python
------
From within the Python interpreter or from a Python program, first import the module:

    >>> from Dee import *

Then you can create sample relations, e.g.

    >>> r = Relation(['a', 'b'],
    ...              [(1,   2),
    ...               (3,   4)],
    ...              {'pk':(Key, ['a'])}
    ...             )
    >>> print r
    +---+---+
    | a | b |
    +===+---+
    | 1 | 2 |
    | 3 | 4 |
    +---+---+


Or you can import the sample cluster and show its databases:

    >>> from demoCluster import *

    >>> print demoCluster.databases
    +---------------+
    | database_name |
    +===============+
    | date          |
    | darwen        |
    +---------------+

Display a database catalog:

    >>> print demoCluster.date.relations
    +-----------------------+
    | relation_name         |
    +=======================+
    | SP                    |
    | relations             |
    | P                     |
    | S                     |
    | constraint_attributes |
    | attributes            |
    | constraints           |
    +-----------------------+

Perform a natural join:

    >>> print demoCluster.date.S & demoCluster.date.SP
    +--------+--------+-------+----+----+-----+
    | STATUS | CITY   | SNAME | P# | S# | QTY |
    +========+========+=======+====+====+=====+
    | 20     | London | Smith | P1 | S1 | 300 |
    | 20     | London | Smith | P2 | S1 | 200 |
    | 20     | London | Smith | P3 | S1 | 400 |
    | 20     | London | Smith | P4 | S1 | 200 |
    | 20     | London | Smith | P5 | S1 | 100 |
    | 20     | London | Smith | P6 | S1 | 100 |
    | 10     | Paris  | Jones | P1 | S2 | 300 |
    | 10     | Paris  | Jones | P2 | S2 | 400 |
    | 30     | Paris  | Blake | P2 | S3 | 200 |
    | 20     | London | Clark | P2 | S4 | 200 |
    | 20     | London | Clark | P4 | S4 | 300 |
    | 20     | London | Clark | P5 | S4 | 400 |
    +--------+--------+-------+----+----+-----+
