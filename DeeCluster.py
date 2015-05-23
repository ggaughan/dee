"""DeeCluster: provides a namespace for a set of DeeDatabases"""
__version__ = "0.1"
__author__ = "Greg Gaughan"
__copyright__ = "Copyright (C) 2007 Greg Gaughan"
__license__ = "MIT" #see Licence.txt for licence information

from Dee import Relation, Tuple
from DeeDatabase import Database

class Cluster(dict):
    """A namespace for databases"""
    def __init__(self, name="nemo"):
        """Create a Cluster

           Define initial databases here
           (Called once on cluster creation)
        """
        dict.__init__(self)

        self.name=name

        self.databases = Relation(['database_name'], self.vdatabases)
        #todo should really have relations, attributes etc. to define this...

    def __getattr__(self, key):
        if self.has_key(key):
            return self[key]
        raise AttributeError, repr(key)
    def __setattr__(self, key, value):
        #todo reject non-Database?
        self[key] = value

    #todo delattr

    def __contains__(self, item):
        if item in self.__dict__:
            if isinstance(self.__dict__[item], Database):
                return True
        return False

    def __iter__(self):
        for (k, v) in self.items():
        #for (k, v) in self.__dict__.items():
            if isinstance(v, Database):
                yield (k, v)





    def vdatabases(self):
        return [Tuple(database_name=k)
                for (k, v) in self]
