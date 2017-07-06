from __future__ import print_function

"""Dee: makes Python relational (http://www.quicksort.co.uk)
"""
__version__ = "0.2.0"
__author__ = "Greg Gaughan"
__copyright__ = "Copyright (C) 2007-2010 Greg Gaughan"
__license__ = "GPL" #see Licence.txt for licence information

from . import relation

__all__ = ['relation', 'database']

DEE = relation.Relation([], [{}])
DUM = relation.Relation([], [])
