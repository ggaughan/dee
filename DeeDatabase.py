"""DeeDatabase: provides a namespace and a persitent container for Dee relation variables"""
__version__ = "0.1"
__author__ = "Greg Gaughan"
__copyright__ = "Copyright (C) 2007 Greg Gaughan"
__license__ = "GPL" #see Licence.txt for licence information

from Dee import * #todo remove: just needed constraints? Relation, Key, Tuple, IS_EMPTY
import cPickle
import atexit
import sys

dumpDebug = True    #todo turn off when live?: speed

class Database(dict):
    """A namespace and container for relation variables"""
    def __init__(self, name="nemo"):
        """Create a non-persistent Database
           (use Database.load() to create/reload a persistent one)

           Define initial relvars and their initial values here
           (Called once on Database creation)
        """
        self.failedToLoad = False
        self._readonly=[]

        self.name=name

        self.transactions = {0: {}}     #each dict is a namespace
        self.transactionId = 0

        atexit.register(self._fnexit)

        self._vinit()

    def _vinit(self):
        """Define virtual relvars/relconsts
           (Called repeatedly, e.g. after Database load from disk or commit)
        """
        self._postLoad()    #initialise all constraints now that we have the complete database avaiable in the namespace
        
        self._readonly = []

        self.relations = Relation(['relation_name'], self.vrelations)
        self.attributes = Relation(['relation_name', 'attribute_name'], self.vattributes)
        self.constraints = Relation(['relation_name', 'constraint_name', 'constraint_type'], self.vconstraints)
        self.constraint_attributes = Relation(['constraint_name', 'attribute_name'], self.vconstraint_attributes)

        self._readonly = ['relations', 'attributes', 'constraints', 'constraint_attributes']

    def debugStatus(self):
        try:
            #print>>sys.stderr, dict.__getattribute__(self, 'transactionId'), dict.__getattribute__(self, 'transactions')[dict.__getattribute__(self, 'transactionId')].keys() ##"%s accessing %s" % ("database", item)
            pass
        except:
            #print>>sys.stderr, "no trans"
            pass


    def __getattribute__(self, item):
        try:
            #self.debugStatus()

            if item not in ['transactionId', 'transactions'] and dict.__getattribute__(self, '__dict__').has_key('transactionId') and item in dict.__getattribute__(self, 'transactions')[dict.__getattribute__(self, 'transactionId')]:
                return dict.__getattribute__(self, 'transactions')[dict.__getattribute__(self, 'transactionId')][item]
            else:
                return dict.__getattribute__(self, item)
        except KeyError:
            raise AttributeError(item)

    def __setattr__(self, item, value):
        #print "%s setting %s" % ("database", item)

        if self.__dict__.has_key('_readonly') and item in self._readonly:
            raise AttributeError(item)  #todo define/find Read-only/const relvar

        if isinstance(value, Relation):
            self.transactions[self.transactionId][item]=value
        else:
            dict.__setattr__(self, item, value)

    def __delattr__(self, item):
        #print "%s deleting %s" % ("database", item)

        if self.__dict__.has_key('_readonly') and item in self._readonly:
            raise AttributeError(item)  #todo define/find Read-only/const relvar

        if isinstance(self.transactions[self.transactionId][item], Relation):
            del self.transactions[self.transactionId][item]
        else:
            dict.__delattr__(self, item)

    def _preDump(self, d):
        """Removes callable (non-picklable) relation references from the dictionary, e.g. catalog"""
        #print "predump:", d.keys()
        res = d.copy()
        for k in d:
            if isinstance(d[k], Relation):
                if callable(d[k]._body):    #todo? and, therefore?, or just if, in self.__dict__
                    del res[k]
        return res

    def _postLoad(self):
        """Evaluates constraint functions now that all references are available"""
        for (rn, r) in self:
            r.constraints={}
            for cname, (kn, p) in r.constraint_definitions.items():
                #print "Setting %s after database loaded on %s (%s)" % (cname, rn, r._heading)
                r.constraints[cname] = eval("%s(r, %s, self.transactions[self.transactionId])" % (kn, str(p)))       #todo? use python apply(fn, args)... but first need to map kn to the fn
        

    #todo allow Python 2.5 'with' to avoid try..finally syntax overhead:
    #e.g. with wh: wh.update
    def begin(self):
        """Start a transaction"""
        assert(self.transactionId == 0, "No nesting allowed yet")

        #print>>sys.stderr, "begin %s" % self.transactionId
        self.debugStatus()

        self.transactionId += 1
        self.transactions[self.transactionId]={}
        clones=cPickle.dumps(self._preDump(self.transactions[self.transactionId-1]))
        clone=cPickle.loads(clones)
        self.transactions[self.transactionId] = clone
        self._vinit() #re-add views etc.

        self.debugStatus()
        #return self.transactionId

    def commit(self):
        """Commit a transaction"""
        assert(self.transactionId > 0, "Not in a transaction")

        #print>>sys.stderr, "commit %s" % self.transactionId
        self.debugStatus()

        clones=cPickle.dumps(self._preDump(self.transactions[self.transactionId]))
        clone=cPickle.loads(clones)
        self.transactionId -= 1
        del self.transactions[self.transactionId+1] #must be after -=1 because getattr checks if item in there first
        self.transactions[self.transactionId] = clone

        #todo always self.dump to persist/flush?
        self._dump()

        self._vinit() #re-add views etc. Note: must be after _dump


        self.debugStatus()
        #return self.transactionId

    def rollback(self):
        """Rollback a transaction"""
        assert(self.transactionId > 0, "Not in a transaction")

        #print>>sys.stderr, "rollback %s" % self.transactionId
        self.debugStatus()

        self.transactionId -= 1
        del self.transactions[self.transactionId+1] #must be after -=1 because getattr checks if item in there first
        self._vinit() #re-add views etc.

        self.debugStatus()
        #return self.transactionId


    def _filename(self):
        #todo removed for inherited database: return '%s_%s' % (self.name, self.__class__.__name__)
        return '%s' % self.__class__.__name__

    def __contains__(self, item):
        if item not in ['transactionId', 'transactions'] and self.__dict__.has_key('transactionId') and item in self.transactions[self.transactionId] and isinstance(self.transactions[self.transactionId][item], Relation):
           return True
        if item in self.__dict__:
            if isinstance(self.__dict__[item], Relation):
                return True
        return False

    def __iter__(self):
        if self.__dict__.has_key('transactionId'):
            for (k, v) in self.transactions[self.transactionId].items():
                if isinstance(v, Relation):
                    yield (k, v)

    def load(name):
        """Loads a Database from disk, if not found raises an exception"""
        try:
            #print "Loading", '%s_%s' % (name, Database.__name__)
            f = open('%s_%s' % (name, Database.__name__), 'rb')

            #exec("import %s" % name)
            result = cPickle.load(f)
            #todo check this member does exist first:-

            result.__dict__['transactions'][result.transactionId] = result.__dict__['transactions'][result.transactionId]

            result._vinit()

            result.failedToLoad = False
        except IOError: #todo specifically "file not found" else error
            #result = Database(name)
            #result.failedToLoad = True
            raise #for inherited Database

        return result
    load = staticmethod(load)


    def _dump(self):
        """Saves the Database to disk, unless it originally failed to load"""
        #print "Dump Database for %s to %s" % (self.name, self._filename())
        if self.failedToLoad:
            raise "Save aborted after original load failure"

        f = open(self._filename(), 'wb')
        cPickle.dump(self, f)

        if dumpDebug:
            f = open(self._filename()+'_script.py', 'w')
            #todo: prefix with class def based on Database so that any references to self will work
            #f.write("from Dee import *\n\n")
            #f.write("class %s(object):\n" % self.name)
            #f.write("\tdef __init__(self):\n")
            for (r,v) in self:
                #print >>sys.stderr,r,`v`
                #f.write("\t\tself.%s = %s\n\n" % (r,`v`))        #todo prefix relation name with self.
                f.write("%s = %s\n\n" % (r,`v`))        #todo prefix relation name with self.
            #todo close class def



    def _fnexit(self):
        self._dump()


    def __getstate__(self):
        odict = self.__dict__.copy() # copy the dict since we change it
        #print "__getstate__:", odict.keys()
        for r in self.__dict__:
            if r=='_readonly':
                del odict[r]    #remove so _vinit on reload can re-assign relations etc.
            else:
                if r=="transactionId":
                    odict[r]=0 #reset transactionId so any reload is not in a transaction
                elif r=="transactions":
                    odict[r][0]=self._preDump(odict[r][0])
                    #Reset rest of odict[r] entries because not needed to persist (and in case unpicklable garbage left)
                    t=odict[r].copy()
                    for k in odict[r]:
                        if k>0:
                            del t[k]
                    odict[r]=t
                elif isinstance(odict[r], Relation):
                    if callable(odict[r]._body):
                        #print "Removing %s from storage" % r
                        del odict[r]
        return odict

    #def __setstate__(self,dict):
    #    self.__dict__.update(dict)   # update attributes


#        def sortTupleList(tl):
#            k=[x['order'] for x in tl]
#            k.sort()
#            return [y for x in k for y in tl if y['order']==x]



    def vrelations(self):
        return [Tuple(relation_name=k)
                for (k, v) in self]

    def vattributes(self):
        return [Tuple(relation_name=k, attribute_name=ka)
                for (k, v) in self for ka in v.heading()]

    def vconstraints(self):
        return [Tuple(relation_name=k, constraint_name="%s_%s" % (k, rn), constraint_type=rf.func_name)
                for (k, v) in self
                for (rn, rf) in v.constraints.items()]

    def vconstraint_attributes(self):
        return [Tuple(constraint_name="%s_%s" % (k, rn), attribute_name=attr)
                for (k, v) in self
                for (rn, (rf, rp)) in v.constraint_definitions.items()
                for attr in (rp or v.heading())
                ]


    def open(database, database_name):
        """Loads or creates a new Database"""
        try:
            return database.load(database_name)
        except IOError: #todo specifically "file not found" else error:
            #print "Creating", database_name
            return database(database_name)
    open = staticmethod(open)



if __name__=='__main__':
    pass
