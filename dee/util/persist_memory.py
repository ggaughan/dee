"""Dummy database persistence plug-in
"""

#todo implement commit/rollback via memory versioning

from ..relation import Relation
from .persist import Persist
import os

class Memory(Persist):
    def __init__(self, database, persistence_location=None, debug=False):
        """
        """
        self.database = database
        self.persistence_location = persistence_location
        self.debug = debug

    def connect(self):
        pass

    def disconnect(self):
        pass

    def delete(self):
        pass

    def store(self, item, before, after):
        pass

    def drop(self, item):
        pass

    def begin(self):
        pass

    def commit(self):
        pass

    def rollback(self):
        pass

    def begin_outer(self):
        #todo start memory version
        pass

    def commit_outer(self):
        #todo commit memory version
        pass

    def rollback_outer(self):
        #todo rollback memory version
        pass

    def get_relations(self):
        return
