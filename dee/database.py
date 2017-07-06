"""The Database is a namespace for Relation variables and Constraints 
   and provides hooks for:
     * atomic updates (with)
     * plug-in persistence mechanism
     * a catalog
"""

from .util.orderedset import OrderedSet
from .util.persist_memory import Memory
from .util.persist_sqlite import SQLite

from .relation import Relation, View

import copy

class InvalidDatabaseItem(Exception):
    def __init__(self, *args):
        pass
    
class Catalog(object):
    """Catalog for a Database"""
    def __init__(self, database):
        self.database = database
        #todo self.relvars = view->self._relvars() (or view->db-catalog to allow modification, though mappings needed)
        
    def _relvars(self):
        #todo temp
        return Relation(['name'],
                        [[r] for r in self.database if isinstance(self.database[r], Relation)]
                       )
    relvars = property(_relvars)
       
    
class Database(dict):
    """Groups Relation variables and Constraints into a persisted namespace
    """
    def __init__(self, _indict=None, debug=False, persistence_engine=Memory, persistence_location=None, **args):
        if _indict is None:
            _indict = args
        dict.__init__(self, _indict)
        
        self.debug = debug
        
        self.catalog = Catalog(self)
        
        self.constraints = []   #todo use supporting class and put in catalog
        self._constraint_checking = True
        
        self._reserved = ['catalog', 'constraints', '_constraint_checking']
        
        self.persist = persistence_engine(self, persistence_location, debug=self.debug)

        self.persist.connect()

        #Load any persisted relations
        rels = self.persist.get_relations()
        if rels:
            for (name, r) in rels:
                self[name] = r
            
        self.__initialised = True

    def __del__(self):
        """Destroys the database instance (but not the persistece store)"""
        del self.catalog
        del self.persist
        dict.__del__(self)
        
    def close(self):
        self.persist.disconnect()
        
    def delete(self):
        """Deletes the persistece store"""
        #Note this removes the persistence file
        self.close()
        self.persist.delete()
        
    #todo simplify from web.py Storage
    def __getattr__(self, item):
        """Retrieves an item"""
        try:
            #todo ok? return copy.deepcopy(self.__getitem__(item))
            return self.__getitem__(item)
        except KeyError:
            raise AttributeError(item)

    def __setattr__(self, item, value):
        """Sets/updates an item"""
        if '_Database__initialised' not in self.__dict__:
            return dict.__setattr__(self, item, value)
        else:
            if item in self._reserved:
                #we're trying to close
                dict.__setattr__(self, item, value)
            else:            
                if not isinstance(value, Relation):
                    #todo unless Constraint etc.
                    raise InvalidDatabaseItem()
                else:
                    #todo implicit with if none detected?
                    if isinstance(value, View):
                        if item in self:
                            #update
                            #todo persist via the view definition
                            dict.__setattr__(self, item, copy.deepcopy(value))
                        else:
                            #create
                            #todo persist via the view definition
                            self.__setitem__(item, copy.deepcopy(value))
                            i = self.__getitem__(item)
                            i._database = self  #link to owner  #todo use weakref
                            i._database_item = item
                    else:
                        if value.is_deferred:
                            value._materialise()
                        if item in self:
                            #update
                            before = copy.deepcopy(self[item])  #note: deep copy only needed for potential constraint rollback (so if no failable constraints, shortcut?)
                            after = value
                            #todo: if with - defer constraint checks (and we didn't need to deep-copy the before)
                            try:
                                dict.__setattr__(self, item, after)
                                self.check_constraints()  #will raise an exception if a constraint is violated
                            finally:
                                dict.__setattr__(self, item, before)
                            
                            self.persist.begin()
                            self.persist.store(item, before, after)
                            #todo if fail: rollback and raise and will remain as before
                            self.persist.commit()
                            #todo: could also insert/delete before/after diff's here too, i.e. |= -= instead of setattr
                            dict.__setattr__(self, item, copy.deepcopy(value))
                        else:
                            #create
                            before = Relation(value.heading)
                            after = value
                            #todo: if with - defer constraint checks
                            try:
                                self.__setitem__(item, after)
                                self.check_constraints()  #will raise an exception if a constraint is violated
                            finally:
                                self.__delitem__(item)

                            self.persist.begin()
                            self.persist.store(item, before, after)
                            #todo if fail: rollback and raise and will remain as before
                            self.persist.commit()
                            self.__setitem__(item, copy.deepcopy(value))
                            i = self.__getitem__(item)
                            i._database = self  #link to owner  #todo use weakref
                            i._database_item = item
                    #todo end sub-transaction/lock
                    
    def __delattr__(self, item):
        """Deletes an item"""
        if '_Database__initialised' not in self.__dict__:
            return dict.__delattr__(self, item)
        else:
            if item in self._reserved:
                #todo raise permission error instead
                raise InvalidDatabaseItem()
            else:            
                if item in self:
                    #todo implicit with if none detected?
                    if isinstance(self.__getitem__(item), Relation):
                        #todo implicit with if none detected?
                        if isinstance(self.__getitem__(item), View):
                            #delete
                            #todo persist via the view definition
                            self.__delitem__(item)
                        else:
                            #delete
                            before = copy.deepcopy(self[item])  #note: deep copy only needed for potential constraint rollback (so if no failable constraints, shortcut?)
                            #todo: if with - defer constraint checks
                            try:
                                self.__delitem__(item)
                                self.check_constraints()  #will raise an exception if a constraint is violated
                            finally:
                                self.__setitem__(item, before)
                                i = self.__getitem__(item)
                                i._database = self  #link to owner  #todo use weakref
                                i._database_item = item
    
                            self.persist.begin()
                            self.persist.drop(item)
                            #todo if fail: rollback and raise and will remain as before
                            self.persist.commit()
                            #todo: call any item cleanup code here
                            i = self.__getitem__(item)
                            i._database = None
                            i._database_item = None
                            self.__delitem__(item)
                    else:
                        #todo raise unhandled item error instead
                        raise InvalidDatabaseItem()
                    #todo end sub-transaction/lock
                else:
                    #todo raise unknown item error instead
                    raise InvalidDatabaseItem()
        
    def __enter__(self):
        """Starts atomic update (i.e. with)"""
        #note: return value will be assigned to any 'as' part
        #todo get deep copy of database: before
        #todo set self._constraint_checking=False so constraint checking can be deferred
        return self.persist.begin_outer()
        
    def __exit__(self, exc_type, exc_value, traceback):
        """Ends atomic update (i.e. exit with)"""
        #todo database level constraint checks & if fail, revert db to before and rollback
        if exc_type:
            self.persist.rollback_outer()
        else:
            self.persist.commit_outer()
        #todo set self._constraint_checking=True
        #note: return True to swallow exception (e.g. depending on exc_value)
    
    def check_constraints(self):
        """Check the database constraints
        
           Raises an exception if any one is violated
        """
        if not self._constraint_checking:
            return
        
        for c in self.constraints:
            if not c():
                raise Exception("Constraint failed")  #todo more detail and more refined type
