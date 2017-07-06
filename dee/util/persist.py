"""Database persistence interface
"""
from abc import ABCMeta, abstractmethod
from ..relation import Relation
import os

#todo make pluggable
class Persist(object):
    __metaclass__ = ABCMeta

    @abstractmethod
    def __init__(self, database, persistence_location=None, debug=False):
        pass
        
    @abstractmethod
    def connect(self):
        pass
        
    @abstractmethod
    def disconnect(self):
        pass
        
    @abstractmethod
    def delete(self):
        pass

    @abstractmethod
    def store(self, item, before, after):
        pass

    @abstractmethod
    def drop(self, item):
        pass
        
    @abstractmethod
    def begin(self):
        pass
    
    @abstractmethod
    def commit(self):
        pass

    @abstractmethod
    def rollback(self):
        pass
    
    @abstractmethod
    def begin_outer(self):
        pass
    
    @abstractmethod
    def commit_outer(self):
        pass

    @abstractmethod
    def rollback_outer(self):
        pass
        
    @abstractmethod
    def get_relations(self):
        pass
