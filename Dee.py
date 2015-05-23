"""Dee: makes Python relational (http://www.quicksort.co.uk)"""
__version__ = "0.12"
__author__ = "Greg Gaughan"
__copyright__ = "Copyright (C) 2007 Greg Gaughan"
__license__ = "MIT" #see Licence.txt for licence information

import sys
import types
from datetime import datetime
import inspect

#############
class RelationException(Exception):
    pass

class RelationConstraintException(RelationException):
    def __init__(self, obj, constraint_name):
        self.obj = obj
        self.constraint_name = constraint_name

    def __str__(self):
        return "Constraint %s failed on {%s}" % (self.constraint_name, ", ".join(self.obj.heading()))

class RelationInvalidOperationException(RelationException):
    def __init__(self, obj, explanation):
        self.obj = obj
        self.explanation = explanation

    def __str__(self):
        return "%s on {%s}" % (self.explanation, ", ".join(self.obj.heading()))

class RelationUnsupportedOperandTypesException(RelationException):
    def __init__(self, obj, explanation):
        self.obj = obj
        self.explanation = explanation

    def __str__(self):
        return "Unsupported operand type(s) %s on {%s}" % (self.explanation, ", ".join(self.obj.heading()))


class TupleException(Exception):
    pass

class TupleInvalidOperationException(TupleException):
    def __init__(self, obj, explanation):
        self.obj = obj
        self.explanation = explanation

    def __str__(self):
        return "%s on {%s}" % (self.explanation, ", ".join(self.obj.keys()))

#############

def dictToTuple(heading, d):
    """Convert dict into an ordered tuple of values, ordered by the heading"""
    return tuple([d[attr] for attr in heading])

def validateHeading(heading):
    if heading:
        if len(heading) != len(set(heading)):
                raise RelationException("Heading attributes must be unique")
        for s in heading:
            if not isinstance(s, str):
                raise RelationException("Heading attributes must be strings")
    #else empty heading

####################
#Constraint wrappers

def constraintFromCandidateKeyFactory(r, Hk=None, scope={}):
    '''Returns a constraint function to check a candidate key constraint'''
    def constraintFromCandidateKey():
        if Hk is None:
            return True #i.e. Hk=list(r.heading()) but that would cause infinite loop since COUNT(r(Hk)) would return a relation with the same constraint...
                         #so instead we assume that None = all attributes and this key constraint is satisfied by the definition of a Relation body, i.e. no duplicate tuples
        return COUNT(r) == COUNT(r(Hk))

    #Catch any errors early
    if Hk and not isinstance(Hk, list):
        raise RelationException("Key attribute(s) should be a list (%s)" % str(Hk))

    return constraintFromCandidateKey

Key = constraintFromCandidateKeyFactory


def constraintFromForeignKeyFactory(r1, (r2, map), scope={}):
    '''Returns a constraint function to check a foreign key constraint
       r2 = referenced relation reference e.g. "r1"
       scope = locals() from caller
       map = foreign key to candidate key mapping, e.g. {'ref_id': 'id'}
    '''
    #self = scope   #i.e. so any reference declarded in the DeeDatabase __init__ method are still valid here
    
    def constraintFromForeignKey():
        return eval(r2, globals(), scope)(map.values()) >= r1(map.keys()).rename(map)(map.values())

    #todo if map is None, assume common(r1, r2)?

    #Catch any errors early
    if map is None or not isinstance(map, dict):
        raise RelationException("Foreign key attribute(s) should be passed as a mapping (%s)" % str(map))

    if not isinstance(r2, types.StringType):
        raise RelationException("Foreign key referenced table should be passed as a reference, not literally")

    if eval(r2, globals(), scope)._headingPK() is None:
        raise RelationException("Foreign key referenced table should have a primary key")

    return constraintFromForeignKey

ForeignKey = constraintFromForeignKeyFactory


def constraintFromLambdaFactory(r, f, scope={}):
    '''Returns a generic constraint function'''
    def constraintFromLambda():
        return f(r)

    return constraintFromLambda

Constraint = constraintFromLambdaFactory

def _convertToShorthand(kn):
    if kn == 'constraintFromCandidateKeyFactory':
        return 'Key'
    elif kn == 'constraintFromLambdaFactory':
        return 'Constraint'
    elif kn == 'constraintFromForeignKeyFactory':
        return 'ForeignKey'
    else:
        return kn

def _convertToConstraint(kn):
    if kn == 'constraintFromCandidateKeyFactory':
        return Key
    elif kn == 'constraintFromLambdaFactory':
        return Constraint
    elif kn == 'constraintFromForeignKeyFactory':
        return ForeignKey
    else:
        raise RelationException("Unexpected constraint %s" % kn)

#####################

class Tuple(dict):
    """A relational tuple
       (mostly compatible with a Python dict"""
    def __init__(self, _indict=None, **args):
        if _indict is None:
            _indict = args
        dict.__init__(self, _indict)

        myhash = 0
        for it in self.iteritems(): myhash ^= hash(it)
        self.myhash = myhash

        self.__initialised = True


    #todo simplify from web.py Storage
    def __getattr__(self, item):
        try:
            return self.__getitem__(item)
        except KeyError:
            raise AttributeError(item)

    def __setattr__(self, item, value):
        if not self.__dict__.has_key('_Tuple__initialised'):
            return dict.__setattr__(self, item, value)
        elif self.__dict__.has_key(item):
            dict.__setattr__(self, item, value)
        else:
            self.__setitem__(item, value)

    def attributes(self):
        return set(self.keys())

    def __hash__(self):
        return self.myhash

    def __repr__(self):
        """Return tuple as Python definition syntax, e.g. Tuple(x=1, y=2)"""
        return "Tuple(%s)" % ", ".join(["%s=%s" % (k,`v`) for (k,v) in self.items()])


    #todo __eq__, etc.
    #todo: maybe do via Relation, e.g. Relation.fromTuple(self).remove(['whatever']).toTuple()
    def remove(self, head):
        """Remove one or more columns, e.g. remove(t, ['a','b'])"""
        if not isinstance(head, list):
            raise TupleInvalidOperationException(self, "Heading attribute(s) should be a list (%s)" % str(head))
        Hs=tuple(set(self.keys()).difference(set(head)))
        return Tuple(zip(Hs, dictToTuple(Hs, self)))

    def project(self, head):
        """Tuple PROJECT (shorthand/macro for remove)"""
        if head and not isinstance(head, list):
            raise TupleInvalidOperationException(self, "Heading attribute(s) should be a list (%s)" % str(head))
        validateHeading(head)
        for attr in head:
            if attr not in self.keys():
                raise TupleInvalidOperationException(self, "Unknown attribute: %s" % attr)
        removeHead=set(self.keys()).difference(set(head))
        return self.remove(list(removeHead))

    def extend(self, Hextension = [], extension = lambda t:{}):
        """Tuple EXTEND"""
        #todo implemented as a macro for now
        return Relation.fromTuple(self).extend(Hextension, extension).toTuple()

    def rename(self, newNames):
        """Tuple rename"""
        #todo implemented as a macro for now
        return Relation.fromTuple(self).rename(newNames).toTuple()

    def wrap(self, Hr, wrapname):
        """Tuple wrap"""
        #todo implemented as a macro for now
        return Relation.fromTuple(self).wrap(Hr, wrapname).toTuple()

    def unwrap(self, wrapname):
        """Tuple unwrap"""
        #todo implemented as a macro for now
        return Relation.fromTuple(self).unwrap(wrapname).toTuple()

################

class Relation(object):
    """A relation
       Implements Tutorial D style operators
    """
    def __init__(self, heading, body, constraints={'PK':(Key, None)}):
        """heading is a list/tuple of attributes
           body is a list of dictionaries (i.e. relational Tuples) or a list of lists/tuples matching the order of the heading
           constraints is a dictionary of constraints, e.g. {'CandKey':(Key, ['id'])}
        """

        #todo: overload ot allow heading to be a dictionary with type info (in case empty body)
        #      e.g. {'a':IntType, 'b':StringType}
        
        #todo: add optional parameter for predicate_constant and surface in catalog
        
        validateHeading(heading)

        self._heading = tuple(heading)

        #Add constraints
        self.setConstraints(constraints)

        self.setBody(body)

    def setConstraints(self, constraints):
        self.constraints = {}
        self.constraint_definitions = {}    #for persistence & reference
        for cname, (k, p) in constraints.items():
            self.constraint_definitions[cname] = (k.func_name, p)
            #Note: we evaluate the constraint function now if we can tell we're not in a (database) namespace, else we defer
            #      until DeeDatabase postLoad to ensure consistent namespace both on creation and after pickle
            if 'self' not in sys._getframe(2).f_locals or not isinstance(sys._getframe(2).f_locals['self'], dict):  #todo need a better test: #not str(type(sys._getframe(2).f_locals['self'])).endswith("_Database'>"):  #todo need a better test
                #print "Setting %s after relation created on %s" % (cname, self._heading)
                self.constraints[cname] = k(self, p, sys._getframe(2).f_locals)     #todo improve? remove getframe? 2 ok? was 1

    def _recalc_hash(self):
        myhash=0
        if callable(self._body):
            #todo ok to equate views etc.?
            myhash = hash(self._body.func_name)  #todo ok to assume it's a function?
        else:
            for tup in self._body:
                myhash ^= hash(tup) #todo track call-count

        self.myhash = myhash

    def __hash__(self):
        return self.myhash

    def __getstate__(self):
        odict = self.__dict__.copy() # copy the dict since we change it
        del odict['constraints']              # remove constraints entry: contains function
                                              # todo: persist the function definition!
        return odict

    def __setstate__(self,dict):
        self.__dict__.update(dict)   # update attributes
        self.__dict__.update({'constraints':{}})    #initialise the constraints at least

        #Note: we don't instantiate the constraints yet.
        #      This load is considered part of a DeeDatabase load and they will be done there
        #      e.g. in cases of foreign keys relating to other relations


    def _checkConstraints(self):
        """Check the existing constraints are valid"""
        for (rn, rf) in self.constraints.items():
            if not rf():
                raise RelationConstraintException(self, rn)

    def _dictToTuple(self, d):
        return dictToTuple(self._heading, d)

    def setBody(self, body):
        """(assumes header has been set)
           Guarantees no duplicate tuples (so derived relations with implied PK of all attributes needn't check PK constraint
           - else infinition recursion since constraint check involves projecting to a derived relation...)
        """
        #todo return if body is None or function...
        if callable(body):
            self._body=body
            self._mapToOriginalHeading = {}
            return

        self._body=[]

        self._headingInvert=dict(zip(self._heading, [{} for attr in self._heading]))
        self._recalc_hash() #need in case no rows
        self._addToBody(body)


    def _addToBody(self, body, nocheck=False):
        """(assumes header has been set)"""

        #todo instead, add to secondary storage to allow huge sets

        #todo assert not callable

        for row in body:
            ri=None
            if isinstance(row, dict):
                if len(self._body) > 0:
                    for ci in xrange(len(self._body[0])):
                        #todo allow None?
                        if not isinstance(self._dictToTuple(row)[ci], type(self._body[0][ci])):
                            raise "Invalid type: %s %i %s" % (self._dictToTuple(row), ci, self._body[0]) #todo skip for now, then later raise & rollback
                if not len(self._hashfind(row))>0:
                    ri = len(self._body)
                    self._body.append(self._dictToTuple(row))
                    self.myhash ^= hash(self._dictToTuple(row)) #todo track call-count
                #else error?
            elif isinstance(row, list) or isinstance(row, tuple):
                if len(self._body) > 0:
                    for ci in xrange(len(self._body[0])):
                        #todo allow None?
                        if not isinstance(row[ci], type(self._body[0][ci])):
                            raise "Invalid type: %s %i %s" % (row, ci, self._body[0]) #todo skip for now, then later raise & rollback
                if not len(self._hashfind(row))>0:
                    ri = len(self._body)
                    self._body.append(row) #order matters
                    self.myhash ^= hash(row) #todo track call-count
                #else error?

            #Update inverted references
            if ri is not None:
                for (i, attr) in enumerate(self._heading):
                    if self._body[ri][i] in self._headingInvert[attr]:
                        self._headingInvert[attr][self._body[ri][i]].add(ri)
                    else:
                        self._headingInvert[attr][self._body[ri][i]]=set([ri])

        #todo perform a faster way!
        #Check constraints
        if not nocheck:
            try:
                self._checkConstraints()
            except:
                self._removeFromBody(body, nocheck=True)
                raise
            #todo rollback if fails! & recalc hash: self._removeFromBody(body)?

    def _removeFromBody(self, body, nocheck=False):
        """(assumes header has been set)"""

        #todo assert not callable

        for row in body:
            ri=None
            if isinstance(row, dict):
                if len(self._body) > 0:
                    for ci in xrange(len(self._body[0])):
                        #todo allow None?
                        if not isinstance(self._dictToTuple(row)[ci], type(self._body[0][ci])):
                            raise "Invalid type: %s %i" % (self._dictToTuple(row), ci) #todo skip for now, then later raise & rollback
                ri = self._hashfind(row)
                if len(ri) > 0:
                    ri = list(ri)[0]
                else:
                    ri = None
                #if self._dictToTuple(row) in self._body:  #todo use hash-join:speed
                #    ri = self._body.index(self._dictToTuple(row))
                    #print>>sys.stderr, "row dict in body:", ri, row
                #else:
                    #print>>sys.stderr, "row dict not in body:", row
            elif isinstance(row, list) or isinstance(row, tuple):
                if len(self._body) > 0:
                    for ci in xrange(len(self._body[0])):
                        #todo allow None?
                        if not isinstance(row[ci], type(self._body[0][ci])):
                            raise "Invalid type: %s %i" % (row, ci) #todo skip for now, then later raise & rollback
                ri = self._hashfind(row)
                if len(ri) > 0:
                    ri = list(ri)[0]
                else:
                    ri = None

            self._recalc_hash() #todo: update existing hash:speed

            #Delete the row then update the inverted references
            if ri is not None:
                del(self._body[ri])

                #todo optimise!
                #Keep lower pointers, shift higher row pointers down one and discard this pointer
                for (i, attr) in enumerate(self._heading):
                    for (rik,riv) in self._headingInvert[attr].items():
                        if self._headingInvert[attr][rik] and max(self._headingInvert[attr][rik]) >= ri: #something to update  #todo: replace max with any for speed...
                            self._headingInvert[attr][rik] = set([ori for ori in riv if ori < ri]).union(
                                                             set([cri-1 for cri in riv if cri > ri]))
            #else error?

        #todo perform a faster way!
        #Check constraints
        if not nocheck:
            try:
                self._checkConstraints()
            except:
                self._addToBody(body, nocheck=True)
                raise
            #todo rollback if fails! and recalc hash: self._addToBody(body)?


    def heading(self):
        return set(self._heading)

    def common(self, rel):
        return self.heading().intersection(rel.heading())

    def _headingAttributeIsKey(self, attr):
        for cname, (kn, p) in self.constraint_definitions.items():
            if kn=="constraintFromCandidateKeyFactory": #todo remove: or kn=="Key"
                if not p:
                    return True     #i.e. implies all attributes
                if attr in p:
                    return True
        return False

    def _headingPK(self):
        for cname, (kn, p) in self.constraint_definitions.items():
            if kn=="constraintFromCandidateKeyFactory": #todo remove: or kn=="Key"
                return p or self._heading   #i.e. None -> all
        return None  #note: None here = no PK rather than PK for all attributes

    def _hashfind(self, row):
        rows=None
        if len(self.heading())==0 and len(row)==0:
            if len(self._body)==1:
                return set([0])
            else:
                return set()
        if isinstance(row, dict):
            for attr in row.keys():
                if rows is None:
                    rows = self._headingInvert[attr].get(row[attr], set())
                else:
                    rows = rows.intersection(self._headingInvert[attr].get(row[attr], set()))
                if rows == set():
                    break
        elif isinstance(row, list) or isinstance(row, tuple):
            for (i, attr) in enumerate(self._heading):
                if rows is None:
                    rows = self._headingInvert[attr].get(row[i], set())
                else:
                    rows = rows.intersection(self._headingInvert[attr].get(row[i], set()))
                if rows == set():
                    break

        if rows is None:
            rows=set()
        return rows


    def _scan(self, rel=None):
        """If rel=None full scan, else rel=tuple for join indexed scan returning subset of rows"""
        if rel is None:
            if callable(self._body):
                if self._body.func_code.co_argcount == 0 or (self._body.func_code.co_argcount == 1 and isinstance(self._body, types.MethodType)):   #e.g. a view (function which returns a Relation, i.e. complete set) #todo unless recursive!
                    #todo: avoid implicit iter call here by disallowing view functions from returning lists?
                    #   i.e only Relations, is this possible with relFromCondition etc.?
                    #until then: Relation.__iter__ is allowed!
                    for tup in self._body(): #._scan():
                        yield tup
                else:
                    raise RelationInvalidOperationException(self, "Scanning a functional relation not supported (a)")    #todo ok?
            else:
                for tup in self._body:
                    yield Tuple(zip(self._heading, tup))
        else:
            com=self.common(rel)
            if len(com)==0:
                if callable(self._body):
                    if self._body.func_code.co_argcount == 0 or (self._body.func_code.co_argcount == 1 and isinstance(self._body, types.MethodType)):   #e.g. a view (function which returns a Relation, i.e. complete set) #todo unless recursive!
                        for tup in self._body():
                            yield tup
                    else:
                        for tup in self._body(Tuple()):
                            yield tup
                else:
                    for tup in self._body:
                        yield Tuple(zip(self._heading, tup))
            else:
                assert len(rel._body)==1, "rel must be a tuple" #todo? use COUNT instead = not reliant on internal storage mechanism
                reltupl=[t for t in rel._scan()][0] #todo use relToTuple (i.e. refactor assert + _scan..[0])
                if callable(self._body):
                    if self._body.func_code.co_argcount == 0 or (self._body.func_code.co_argcount == 1 and isinstance(self._body, types.MethodType)):   #e.g. a view (function which returns a Relation, i.e. complete set) #todo unless recursive!
                        for tup in self._body():
                            #Now do join filter manually (no mapping needed)
                            for attr in com:
                                if reltupl[attr] != tup[attr]:
                                    break
                            else:
                                yield tup
                    else:
                        #map to original names (in case renames have happened)
                        for v in self._mapToOriginalHeading.values():
                            if v in reltupl:
                                del(reltupl[v]) #ensure we can't pass originally named values
                        for k in self._mapToOriginalHeading:
                            if k in reltupl:
                                reltupl[self._mapToOriginalHeading[k]] = reltupl[k] #set value for the alias
                                del(reltupl[k]) #remove value for the original name
                        for tup in self._body(reltupl):
                            #map from original names (in case renames have happened)
                            for k in self._mapToOriginalHeading:
                                if self._mapToOriginalHeading[k] in tup:
                                    tup[k] = tup[self._mapToOriginalHeading[k]]
                                    del(tup[self._mapToOriginalHeading[k]])
                            yield tup
                else:
                    #Can use optimised hash join
                    rows=None
                    for attr in com:
                        if rows is None:
                            rows = self._headingInvert[attr].get(reltupl[attr], set())
                        else:
                            rows = rows.intersection(self._headingInvert[attr].get(reltupl[attr], set()))
                    for i in rows:
                        yield Tuple(zip(self._heading, self._body[i]))


    def __iter__(self):
        #todo reinstate!
        #raise RelationInvalidOperationException(self, "RM Proscription 7: No tuple-level operations")
        return self._scan()

    def renderHTML(self, columns=None, sort=None, row_limit=None, link_columns=None, title_columns=False):
        """Render relation to HTML for client transport/display/parsing
           sort = (bool, list of columns) where bool = true -> ascending else descending
           link_columns = dictionary of {column : url-template} pairs for columns to be rendered as hyperlinks (* = all columns, can be overridden)
                          url-template is passed tuple value, e.g. url-template % row
                          (Note: this means any column used in hyperlink is raw, e.g. dates are YYYY-MM-DD HH:MM:SS, i.e. pre-rendering)
           title_columns = True for titled columns with underscores replaced with spaces
        """
        #todo base this on a renderXML() method and use XSLT to convert to HTML (and sort?)
        #todo assert sort columns exist
        def displayName(col):
            if title_columns:
                return col.title().replace('_', ' ')
            else:
                return col

        def head(columns):
            l = []
            for col in columns:
                if self._headingAttributeIsKey(col):
                    l.append("<th><em>%s</em></th>" % displayName(col))
                else:
                    l.append("<th>%s</th>" % displayName(col))

            return "<thead>" + "".join(l) + "</thead>"

        def line(columns, row):
            l = []
            for col in columns:
                if isinstance(row[col], Relation):
                    v = row[col].renderHTML()
                else:
                    if isinstance(row[col], datetime):
                        v = row[col].strftime('%c')
                    else:
                        v = str(row[col])

                s = v
                if '*' in link_columns:
                    s = '<a href="%s">%s</a>' % (link_columns['*'] % row, v)
                if col in link_columns:
                    s = '<a href="%s">%s</a>' % (link_columns[col] % row, v)
                l.append("<td>%s</td>" % s)

            return "<tr>" + "".join(l) + "</tr>"


        if not columns:
            columns=self._heading

        if not link_columns:
            link_columns={}

        result = [head(columns), '<tbody>']

        rows=self.toTupleList(sort)

        i=0
        for tup in rows:
            if row_limit and i>=row_limit:
                break
            result.append(line(columns, tup))
            i += 1

        return '<table>' + "".join(result) + "</tbody></table>"


    def __str__(self):
        columns = xrange(len(self._heading))

        col_width = [len(self._heading[col]) for col in columns]

        if callable(self._body):
            if self._body.func_code.co_argcount == 0 or (self._body.func_code.co_argcount == 1 and isinstance(self._body, types.MethodType)):   #e.g. a view (function which returns a Relation, i.e. complete set) #todo unless recursive!
                for tup in self._body(): #-> ._scan():
                    for col,colname in enumerate(self._heading):
                        n=max([len(s) for s in str(tup[colname]).splitlines()]) #todo fix for empty string!
                        col_width[col] = max(col_width[col], n)
            else:
                for col in columns:
                    col_width[col] = max(col_width[col], 20)     #todo improve?
        else:
            #note: full pass
            for tup in self._body:
                for col in columns:
                    if len(str(tup[col])) > 0:
                        n=max([len(s) for s in str(tup[col]).splitlines()])
                    else:
                        n=0
                    col_width[col] = max(col_width[col], n)

        col_widthmap = dict(zip(self._heading, col_width))

        hline = "%s+\n" % "".join("+".ljust(col_width[col]+3, "-") for col in columns)
        hlineKeyed = "%s+\n" % "".join("+".ljust(col_width[col]+3, "-="[self._headingAttributeIsKey(self._heading[col])]) for col in columns)

        def line(row):
            l = []
            vals={}
            for col in self._heading:
                vals[col] = str(row[col]).splitlines()

            if len(vals)>0:
                for r in xrange(max([len(n) for n in vals.values()])):
                    lr = []
                    for col in self._heading:
                        if r < len(vals[col]):
                            value = vals[col][r]
                        else:
                            value = ""
                        lr.append("| %s" % value.ljust(col_widthmap[col]+1))

                    lr.append("|\n")
                    l.append("".join(lr))
            else:
                l.append("|\n")

            return "".join(l)

        result = [hline]
        result.append(line(dict(zip(self._heading,self._heading))))
        result.append(hlineKeyed)

        for tup in self._scan():
            result.append(line(tup))

        result.append(hline)

        return "".join(result)[:-1] #debug + ("(%s)" % getPerfs())

    def __repr__(self):
        """Return relation as Python definition syntax, e.g. Relation(['x','y'], [{'x':1,'y':2}, {'x':3,'y':4}])"""
        sbody=None
        if callable(self._body):
            if not (self._body.func_code.co_argcount == 0 or (self._body.func_code.co_argcount == 1 and isinstance(self._body, types.MethodType))):   #e.g. a view (function which returns a Relation, i.e. complete set) #todo unless recursive!
                sbody="%s" % inspect.getsource(self._body)  #todo check expands into full definition!
        if sbody is None:
            sbody=str(self.toTupleList())

        return "Relation(%(heading)s,\n%(body)s,\n%(constraints)s)" % {'heading':str(self._heading),
                                                                        'body':sbody.replace('{','\n{'),                                                                        
                                                                        'constraints':"{%s}" % ",\n".join(["'%s':(%s, %s)" % (cname, _convertToShorthand(kn), p) for cname, (kn, p) in self.constraint_definitions.items()])}


    def toTuple(self):
        """Converts a relation to a Tuple (assumes 1 tuple in relation)"""
        if not callable(self._body) and len(self._body)!=1:
            raise RelationInvalidOperationException(self, "Relation must contain a single tuple")

        return [tr for tr in self._scan()][0]

    def fromTuple(tr, constraints=None):
        """Converts a Tuple into a relation"""
        if constraints is None:
            constraints={'PK':(Key, None)}
        return Relation([k for k in tr], [tuple([v for v in tr.values()])], constraints)
    fromTuple = staticmethod(fromTuple)

    def toTupleList(self, sort=None):
        """Converts a relation to a Tuple list
           (order by sort (if passed))
        """
        rows=[tr for tr in self._scan()]

        if sort:
            rows = sorted(rows, cmp=lambda x,y:cmp(([x[v] for v in sort[1]]), [y[v] for v in sort[1]]), reverse=(sort[0]==False))

        return rows

    def fromTupleList(trl, constraints=None):
        """Converts a Tuple list into a relation"""
        #todo remove the need for this constraint? e.g. pass relation heading as a parameter (or overload...)
        if len(trl)==0:
            raise RelationException("Tuple list must contain at least one tuple")

        if constraints is None:
            constraints={'PK':(Key, None)}
        return Relation([k for k in trl[0]], trl, constraints)
    fromTupleList = staticmethod(fromTupleList)


    def dump(self, filename = None):
        import csv
        if callable(self._body):
            if not (self._body.func_code.co_argcount == 0 or (self._body.func_code.co_argcount == 1 and isinstance(self._body, types.MethodType))):   #e.g. a view (function which returns a Relation, i.e. complete set) #todo unless recursive!
                raise RelationInvalidOperationException(self, "Dumping a functional relation not supported")    #todo ok?

        if filename == None:
            filename = self.name
        f=file(filename, "wb")
        f.write(','.join(self._heading)+'\n')
        writer = csv.DictWriter(f, fieldnames = self._heading, dialect='excel')
        writer.writerows(self._scan())       #todo make work for nested rels
        f.close()

    def load(self, filename = None):
        import csv
        if callable(self._body):
            raise RelationInvalidOperationException(self, "Loading a functional relation not supported")    #todo ok?

        if filename == None:
            filename = self.name
        f=file(filename, "rb")
        reader = csv.DictReader(f, dialect='excel')
        for r in reader:
            peek = r
            break
        self._heading = tuple(reader.fieldnames)

        f.seek(0)

        reader = csv.DictReader(f, dialect='excel') #todo make work for nested rels
        self.setBody(reader)

    def project(self, head):
        """Relational PROJECT (shorthand/macro for REMOVE)"""
        if head and not isinstance(head, list):
            raise RelationInvalidOperationException(self, "Heading attribute(s) should be a list (%s)" % str(head))
        validateHeading(head)
        for attr in head:
            if attr not in self.heading():
                raise RelationInvalidOperationException(self, "Unknown attribute: %s" % attr)
        removeHead=self.heading().difference(set(head))
        return REMOVE(self, list(removeHead))

    def __call__(self, head):
        """Calls through to project (shorthand), e.g. r(['id', 'name'])"""
        return self.project(head)


    def remove(self, head):
        """Remove one or more columns (-> REMOVE), e.g. r.remove(['a','b'])"""
        validateHeading(head)
        return REMOVE(self, head)

    def rename(self, newnames):
        """Rename one or more columns, e.g. r.rename({'a':'a1'})
           Note: could implement this with REMOVE and EXTEND (which is itself a macro on top of AND)
           todo: allow prefix/suffix global modifications
        """
        for attr in newnames.keys():
            if attr not in self.heading():
                raise RelationInvalidOperationException(self, "Unknown attribute: %s" % attr)
        heading = tuple([newnames.get(c, c) for c in self._heading])
        validateHeading(heading)

        #Infer constraints  #todo put in one routine?
        #dict([(cname, (_convertToConstraint(kn), p)) for cname, (kn, p) in self.constraint_definitions.items()])
        constraints = {}
        for cname, (kn, p) in self.constraint_definitions.items():
            if _convertToShorthand(kn) == 'Key':
                if p:
                    p = list([newnames.get(c, c) for c in p])
            else:
                continue    #todo: keep other constraints past renamed, e.g. ForeignKey, Constraint
            constraints[cname] = (_convertToConstraint(kn), p)

        if callable(self._body):
            s = Relation(heading, self._body, constraints)
            for (k,v) in newnames.items():
                s._mapToOriginalHeading[v]=s._mapToOriginalHeading.get(k, k)
                if k in s._mapToOriginalHeading:
                    del(s._mapToOriginalHeading[k])
            return s
        else:
            return Relation(heading, self._body, constraints)

    #def where(self, restriction = lambda tup:True):
    #    newHead=self._heading
    #    newBody=[tupl for tupl in self._scan() if restriction(tupl)]
    #    return Relation(newHead, newBody)

    def where(self, restriction = lambda r:True):
        """Restrict rows by condition, e.g. r.where(lambda t: t.x==3)"""
        return RESTRICT(self, restriction)

    def extend(self, Hextension = [], extension = lambda t:{}):
        """Extend rows based on extension (creates pseudo relation for the extension and then joins)"""
        return EXTEND(self, Hextension, extension)

    def group(self, Hr, groupname):
        """Group rows by Hr attributes into a new attribute, groupname"""
        return GROUP(self, Hr, groupname)

    def ungroup(self, groupname):
        """Ungroup rows by into Hr attributes from an attribute, groupname"""
        return UNGROUP(self, groupname)

    def wrap(self, Hr, wrapname):
        """Wraps rows by Hr attributes into a new attribute, wrapname"""
        return WRAP(self, Hr, wrapname)

    def unwrap(self, wrapname):
        """Unwrap rows by into Hr attributes from an attribute, wrapname"""
        return UNWRAP(self, wrapname)

    def insert(self, rel):
        """Insert new relation/tuple (in place) into relation"""
        self.__ior__(rel)

    def delete(self, rel):
        """Delete relation/tuple (in place) from relation"""
        self.__isub__(rel)

    def update(self, restriction = lambda r:True, Hsetting = [], setting = lambda t:{}):
        """Update tuples (in place) in relation, e.g. r.update(lambda t: t.x==3, ['UpdAttr'], lambda u: {'UpdAttr':4})"""
        old = self.where(restriction)
        t1 = old.rename(dict(zip(self._heading, ['_OLD_'+x for x in self._heading])))
        upd = t1.extend(Hsetting, setting)
        Hupdold = ['_OLD_'+x for x in Hsetting]
        Hupdback = upd.heading() - set(Hupdold)
        new = upd.remove(Hupdold).rename(
                           dict(zip([x for x in Hupdback if x.startswith('_OLD_')], [x.lstrip('_OLD_') for x in Hupdback if x.startswith('_OLD_')])))
        #todo: make these two atomic!
        self -= old
        self |= new
        #todo: post-check: if pre-card-self != post-card-self -> rollback+exception



    def __contains__(self, rel):
        """Membership, e.g. t in r1, r1 in r2"""
        if isinstance(rel, Relation):
            return rel <= self   #i.e. subset
        elif isinstance(rel, Tuple):
            if self.heading() == rel.attributes():
                if COUNT(self & Relation.fromTuple(rel)) == 1: #todo re-word as __lt__ ?
                    return True
        return False


    def __eq__(self, rel):
        #todo: prove ok

        #todo: fix & copy elsewhere:
        #if not isinstance(rel, Relation):
        #    raise RelationUnsupportedOperandTypesException(self, "for ==: 'Relation' and '%s'" % type(rel))
        if self.heading() == rel.heading():
            #Note: COUNT check in case of DEE==DUM
            if IS_EMPTY(self - rel) and COUNT(self)==COUNT(rel):
                return True
        return False

    def __ne__(self, rel):
        return not self.__eq__(rel)

    def __lt__(self, rel):
        """Subset"""
        if self.heading() == rel.heading():
            if IS_EMPTY(self - (self & rel)) and COUNT(self)<COUNT(rel):
                return True
        return False

    def __gt__(self, rel):
        """Superset"""
        return rel.__lt__(self)

    def __le__(self, rel):
        """Subset or equal"""
        if self.heading() == rel.heading():
            if IS_EMPTY(self - (self & rel)):
                return True
        return False

    def __ge__(self, rel):
        """Superset or equal"""
        return rel.__le__(self)

    def __and__(self, rel):
        return AND(self, rel)

    def __or__(self, rel):
        return OR(self, rel)

    def __ior__(self, rel):
        #todo assert rel is relation or tuple else: raise RelationInvalidOperationException(self, "insert expects a Relation or a Tuple")
        if callable(self._body):
            raise RelationInvalidOperationException(self, "Cannot assign to a functional relation")

        if isinstance(rel, Tuple):
            return self.__ior__(Relation.fromTuple(rel))

        #Note copied from OR
        if self.heading() != rel.heading():
            raise RelationInvalidOperationException(self, "OR can only handle same reltypes so far: %s" % str(rel._heading))
        #assert self._heading == rel._heading, "OR can only handle same relation types so far"

        Bs=[]
        for tr2 in rel._scan():
            Bs.append(dictToTuple(self._heading, tr2))    #Note deep copies varying dict

        self._addToBody(Bs)
        return self

    def __sub__(self, rel):
        return MINUS(self, rel)

    def __isub__(self, rel):
        #todo assert rel is relation or tuple else: raise RelationInvalidOperationException(self, "delete expects a Relation or a Tuple")
        if callable(self._body):
            raise RelationInvalidOperationException(self, "Cannot assign to a functional relation")

        if isinstance(rel, Tuple):
            return self.__isub__(Relation.fromTuple(rel))

        #Note copied from MINUS
        if self.heading() != rel.heading():
            raise RelationInvalidOperationException(self, "MINUS can only handle same reltypes so far: %s" % str(rel._heading))
        #assert self._heading == rel._heading, "OR can only handle same relation types so far"

        Bs=[]
        for tr2 in rel._scan():
            Bs.append(dictToTuple(self._heading, tr2))

        self._removeFromBody(Bs)
        return self

    #todo __div__   => DIVIDE (simple or general?)
    #todo? __mul__  => AND? COMPOSE?
    
    def __copy__(self):
        #todo retain constraints: return Relation(self._heading, self._body, dict([(cname, (_convertToConstraint(kn), p)) for cname, (kn, p) in self.constraint_definitions.items()]))
        return Relation(self._heading, self._body)
    

    def __len__(self):
        return COUNT(self)


##Wrappers: these wrap a lambda expression and make it behave like a relation so the A algebra can be used,
## e.g. RESTRICT and EXTEND can be implemented in terms of AND
def relationFromCondition(f):
    def wrapper(trx):
        if f(trx):
            return [Tuple()]    #DUM
        else:
            return []           #DEE

    return wrapper

def relationFromExtension(f):
    def wrapper(trx):
        return [f(trx)]

    return wrapper




##Relational operators
##Implements the parts of A needed by the Relation class

### Essential ###
def AND(r1, r2):
    """Natural join
       Ranges from intersection (both relations have same heading)
       through natural join (both relations have one or more common attributes)
       to cartesian (cross) join (neither relation has any attribute in common)
    """
    #Optimise the order
    if callable(r1._body):
        if not callable(r2._body):
            return AND(r2, r1)
        else:
            if not(r1._body.func_code.co_argcount == 0 or (r1._body.func_code.co_argcount == 1 and isinstance(r1._body, types.MethodType))):   #e.g. a view (function which returns a Relation, i.e. complete set) #todo unless recursive!
                if r2._body.func_code.co_argcount == 0 or (r2._body.func_code.co_argcount == 1 and isinstance(r2._body, types.MethodType)):   #e.g. a view (function which returns a Relation, i.e. complete set) #todo unless recursive!
                    return AND(r2, r1)
                else:
                    raise RelationInvalidOperationException(r1, "Cannot AND two functional relations")

    #Can we optimise the hash-join loop? i.e. scan the smallest
    if not callable(r2._body):
        if len(r1._body) > len(r2._body):
            return AND(r2, r1)

    #todo allow a list of relation parameters and call optimiser to order their execution (same for OR, COMPOSE etc)
    Hs=tuple(r1.heading().union(r2.heading()))
    Bs=[]
    relType=Relation(r1._heading, [])
    for tr1 in r1._scan():
        relType.setBody([tr1])
        for tr2 in r2._scan(relType):   #returns only matching row(s) = fast
            tr1.update(tr2)
            Bs.append(dictToTuple(Hs, tr1))    #Note deep copies varying dict

    #todo infer constraints!
    #todo: need mechanism... based on Darwen's algorithm?
##    #Infer constraints  #todo put in one routine?
##    #dict([(cname, (_convertToConstraint(kn), p)) for cname, (kn, p) in self.constraint_definitions.items()])
##    constraints = {}
##    for (rr, prefix) in [(r1, 'L_'), (r2, 'R_')]:
##        for cname, (kn, p) in rr.constraint_definitions.items():
##            if _convertToShorthand(kn) == 'Key':
##                if set(p).issubset(set(Hs)):
##                    constraints[prefix+cname] = (_convertToConstraint(kn), p)

    return Relation(Hs, Bs)    #Note: removes duplicate attributes

def OR(r1, r2):
    """Or/Union
       Equates to union (both relations have same heading)
    """
    if r1.heading() != r2.heading():
        raise RelationInvalidOperationException(r1, "OR can only handle same reltypes so far: %s" % str(r2._heading))
    #assert r1._heading == r2._heading, "OR can only handle same relation types so far"

    #Optimise the order
    if callable(r1._body):
        if not callable(r2._body):
            return OR(r2, r1)
        else:
            if not(r1._body.func_code.co_argcount == 0 or (r1._body.func_code.co_argcount == 1 and isinstance(r1._body, types.MethodType))):   #e.g. a view (function which returns a Relation, i.e. complete set) #todo unless recursive!
                if r2._body.func_code.co_argcount == 0 or (r2._body.func_code.co_argcount == 1 and isinstance(r2._body, types.MethodType)):   #e.g. a view (function which returns a Relation, i.e. complete set) #todo unless recursive!
                    return OR(r2, r1)
                else:
                    raise RelationInvalidOperationException(r1, "Cannot OR two functional relations")

    #todo optimise!
    Hs=tuple(r1.heading().union(r2.heading()))  #==r1._heading
    Bs=[]
    for tr1 in r1._scan():
        Bs.append(dictToTuple(Hs, tr1))    #Note deep copies varying dict
    for tr2 in r2._scan():
        Bs.append(dictToTuple(Hs, tr2))    #Note deep copies varying dict

    #todo infer constraints!
    #todo: need mechanism... based on Darwen's algorithm?
    return Relation(Hs, Bs)

def MINUS(r1, r2):
    """Returns all rows not in the first relation with respect to the second relation
       Equivalent to r1 & (not r2), i.e. r1 minus r2
    """
    if r1.heading() != r2.heading():
        raise RelationInvalidOperationException(r1, "NOT can only handle same reltypes so far: %s" % str(r2._heading))

    #if type(r1.body) is types.FunctionType:
    #    raise "Cannot NOT a virtual relation"

    #Optimise the order
    if callable(r1._body):
        if not callable(r2._body):
            return MINUS(r2, r1)
        else:
            if not(r1._body.func_code.co_argcount == 0 or (r1._body.func_code.co_argcount == 1 and isinstance(r1._body, types.MethodType))):   #e.g. a view (function which returns a Relation, i.e. complete set) #todo unless recursive!
                if r2._body.func_code.co_argcount == 0 or (r2._body.func_code.co_argcount == 1 and isinstance(r2._body, types.MethodType)):   #e.g. a view (function which returns a Relation, i.e. complete set) #todo unless recursive!
                    return MINUS(r2, r1)
                else:
                    raise RelationInvalidOperationException(r1, "Cannot MINUS two functional relations")

    Hs=r1._heading
    Bs=[]
    relType=Relation(r1._heading, [])
    for tr1 in r1._scan():
        relType.setBody([tr1])
        for tr2 in r2._scan(relType):
            break
        else:
            Bs.append(tr1)

    #todo infer constraints!
    #todo: need mechanism... based on Darwen's algorithm?
    return Relation(Hs, Bs)    #Note: removes duplicate attributes

def REMOVE(r, Hr):
    """Remove one or more columns, e.g. remove(r, ['a','b'])"""
    if not isinstance(Hr, list):
        raise RelationInvalidOperationException(r, "Heading attribute(s) should be a list (%s)" % str(Hr))
    Hs=tuple(r.heading().difference(set(Hr)))
    Bs=[dictToTuple(Hs, tr) for tr in r._scan()]

    #todo infer constraints!
    #todo: need mechanism... based on Darwen's algorithm?
    #Infer constraints  #todo put in one routine?
    #dict([(cname, (_convertToConstraint(kn), p)) for cname, (kn, p) in self.constraint_definitions.items()])
    constraints = {}
    for (rr, prefix) in [(r, '')]:
        for cname, (kn, p) in rr.constraint_definitions.items():
            if _convertToShorthand(kn) == 'Key':
                if not p or set(p).issubset(set(Hs)):
                    constraints[prefix+cname] = (_convertToConstraint(kn), p)
    res = Relation(Hs, Bs)     #Note: we don't add constraints here to avoid recursion: add body calls constraint check which calls project which calls remove...
    if constraints:
        res.setConstraints(constraints)
    #else use default (None -> all)
    return res



### Macros ###
def COMPOSE(r1, r2):
    """AND and then REMOVE common attributes
       (macro)"""
    A=list(r1.heading().intersection(r2.heading()))
    return REMOVE(AND(r1, r2), A)

def RESTRICT(r, restriction = lambda trx:True):
    """Restrict rows based on condition
       (creates pseudo relation for the condition and then joins i.e. implemented in terms of AND)
       (macro)"""
    if not callable(restriction):
        raise RelationInvalidOperationException(r, "Restriction should be a function, e.g. lambda t: t.id == 3 (%s)" % restriction)

    #todo: if restriction like trx.x=C1 and trx.y=C2 and etc. then create non-functional relation from Tuple(x=C1, y=C2 etc.) and AND this first = speed

    #todo infer constraints!
    #todo: need mechanism... based on Darwen's algorithm?
    return AND(r, Relation(r._heading, relationFromCondition(restriction)))

def EXTEND(r, Hextension=[], extension = lambda trx:{}):
    """Extend rows based on extension
       (creates pseudo relation for the extension and then joins i.e. implemented in terms of AND)
       (macro)"""
    if not callable(extension):
        raise RelationInvalidOperationException(r, "Extension should be a function, e.g. lambda t: {'new':t.id * 3} (%s)" % extension)
    #Find the heading for the extension

    #todo : somehow infer Hextension from extension (see problem comments below)
    #todo: OR, pass extension = {'a':lambda t:t.A, 'b':lambda t:t.B, 'c':lambda t:t.C, etc.} - too noisy (but less redundant)?
    #todo : otherwise ensure Hextension == extension.keys()! else weirdness

    extheading = set(Hextension)
    validateHeading(extheading)
    if r.heading().intersection(extheading) != set():
        raise RelationInvalidOperationException(r, "EXTEND heading attributes conflict with relation being extended: %s" % r.heading().intersection(extheading))
    #todo infer constraints!
    #todo: need mechanism... based on Darwen's algorithm?
    return AND(r, Relation(r.heading().union(extheading), relationFromExtension(extension)))

def SEMIJOIN(r1, r2):
    """aka MATCHING
       (macro)"""
    return REMOVE(AND(r1, r2), list(r2.heading().difference(r1.heading())))
MATCHING = SEMIJOIN

def SEMIMINUS(r1, r2):
    """aka NOT MATCHING
       (macro)"""
    return MINUS(r1, SEMIJOIN(r1, r2))
NOT_MATCHING=SEMIMINUS

def SUMMARIZE(r1, r2, exps):
    """Summarization
       (macro)

       e.g. SUMMARIZE(r1, r2, {'Z':(SUM, lambda t:t.qty)})
       (Tutorial D equivalent: SUMMARIZE r1 PER r2 ADD agg ( exp ) AS Z)

        TODO better, more flexible syntax might be:
            SUMMARIZE(r1, r2, {'Z': lambda r:SUM(r, lambda t:t.qty)})
            then could do {'Z':lambda r:MAX(r, lambda t:t.qty) - MIN(r, lambda t:t.qty)}
    """
    for (agg, exp) in exps.values():
        if agg not in [COUNT, SUM, AVG, MAX, MIN]:
            raise RelationInvalidOperationException(r1, "SUMMARIZE must be passed one or more aggregate operators (%s)" % agg)
    #todo ensure _Y not used
    r=EXTEND(r2, ["_Y"], lambda t:{"_Y":AND(r1, Relation.fromTuple(t))})
    return REMOVE(EXTEND(r, exps.keys(), lambda u:dict(zip(exps.keys(),
                                          [agg(u["_Y"], exp)
                                           for (agg, exp) in exps.values()]))),
                  ["_Y"]
                 )


def GROUP(r, Hr, groupname):
    """Grouping
       (macro)

       e.g. GROUP(r, ['D', 'E', 'F'], 'X')  (or r.group(['D', 'E', 'F'], 'X')
       (Tutorial D equivalent: r GROUP { D, E, .., F } AS X
    """
    Hs=list(r.heading().difference(set(Hr)))
    return r.extend([groupname], lambda t:{groupname:COMPOSE(r, Relation.fromTuple(t.project(Hs)))
                                           }).project(list(set(Hs).union([groupname])))

def UNGROUP(r, groupname):
    """Ungrouping
       (macro)

       e.g. UNGROUP(r, 'X')  (or r.ungroup('X')
       (Tutorial D equivalent: r UNGROUP X
    """
    #todo assert at least 1 row
    for tr1 in r._scan():
        Hs=tuple(tr1[groupname].heading())
        break
    T=Relation([groupname]+list(Hs),
               [dict(zip([groupname]+t.keys(), [s[groupname]]+t.values()))
                for s in r([groupname])._scan() for t in s[groupname]]
              )
    return COMPOSE(r, T)   #.project(list(set(Hr).union(Hs)))

def WRAP(r, Hr, wrapname):
    """Wrapping
       (macro)

       e.g. WRAP(r, ['D', 'E', 'F'], 'X')  (or r.wrap(['D', 'E', 'F'], 'X')
       (Tutorial D equivalent: r WRAP { D, E, .., F } AS X
    """
    return r.extend([wrapname], lambda t:{wrapname:t.project(Hr)}).remove(Hr)

def UNWRAP(r, wrapname):
    """Unwrapping
       (macro)

       e.g. UNWRAP(r, 'X')  (or r.unwrap('X')
       (Tutorial D equivalent: r UNWRAP X
    """
    #todo assert at least 1 row
    for tr1 in r._scan():
        Hs=tr1[wrapname].keys()
        break

    return r.extend(Hs, lambda t:t[wrapname].project(Hs)).remove([wrapname])

def DIVIDE_SIMPLE(r1, r2):
    """Simple division
       (macro)

        e.g. DIVIDE_SIMPLE(r1, r2)
        (Tutorial D equivalent: r1 DIVIDEBY r2)
    """
    A=list(r1.heading().intersection(r2.heading()))     #todo remove list() - no need?
    #todo ensure _g1 and _g2 are not already used in r1 and r2 respectively
    return (AND(GROUP(r1, A, '_g1'), GROUP(r2, A, '_g2')).where(lambda t:t._g1 >= t._g2).remove(['_g1','_g2']))

#todo: test some more & simplify call
def DIVIDE(r1, r2, r3, r4):
    """General division
       (macro)

    """
    A=list(r1.heading().intersection(r2.heading()))
    #todo ensure _g1 and _g2 are not already used in r1 and r2 respectively
    #todo compress into a single return expression?
    T1=EXTEND(r3, ['_g1'], lambda t:{'_g1':AND(Relation.fromTuple(t.project(A)), r1)})
    print "T1=",T1
    T2=EXTEND(r4, ['_g2'], lambda t:{'_g2':AND(Relation.fromTuple(t.project(A)), r2)})
    print "T2=",T2
    T3=AND(T1, T2).where(lambda t:t._g1 >= t._g2)
    print "T3=",T3
    return T3.remove(['_g1','_g2'])

def GENERATE(extension = {}):
    """Generate a relvar-independent value
       (macro)

        e.g. GENERATE({'pi':3.14})

        #todo: could replace Relation.fromTuple() with GENERATE()
    """
    return EXTEND(TABLE_DEE, extension.keys(), lambda trx:extension)


def TCLOSE(r):
    """Transitive closure (an example of a recursive relational operator)
       (can be optimised for speed)
       (macro)
    """
    if len(r.heading()) != 2:
        raise RelationInvalidOperationException(r, "TCLOSE expects a binary relation, e.g. with a heading ['X', 'Y']")

    _X, _Y = r.heading()

    TTT = r | (COMPOSE( r, r.rename({_Y:'_Z', _X:_Y}) ).rename({'_Z':_Y}))
    if TTT == r:
        return TTT
    else:
        return TCLOSE(TTT)

def QUOTA(r, limit, Hr=None, asc=True):
    """Quota query
       (macro)

       limit is the number of tuples to return (possibly more if a tie, or less if not enough)
       Hr = attribute list to sort by
       asc, if True => ascending sort, else descending
    """
    if not isinstance(Hr, list) or len(Hr)==0:
        raise RelationInvalidOperationException(r, "QUOTA attribute(s) should be a non-empty list (%s)" % str(Hr))

    res = +1
    if asc:
        res = -1

    return REMOVE(
              EXTEND(r, ['_higher'],
                    lambda t:{'_higher':COUNT( r.rename( dict(zip(Hr, ['_'+x for x in Hr])) ).where(
                                                                                              lambda u:cmp(([u['_'+x] for x in Hr]), ([t[x] for x in Hr])) == res
                                                                                                     ) )}
                    ).where(lambda t:t._higher < limit),
              ['_higher'])




##Aggregate operators
def COUNT(r, none=None):
    """Count tuples"""
    if none!=None:
        raise RelationInvalidOperationException(r, "COUNT expects no arguments (%s)" % none) #none accepted for SUMMARIZE generality
    return reduce(lambda x,y: x + 1, (1 for tr in r._scan()), 0)

def SUM(r, expression = lambda trx:None):   #todo None ok? cause error = good?
    """Sum expression"""
    if expression.func_code.co_consts==(None,) and len(r._heading)==1:     #if no expression but 1 attribute, use it
        expression = lambda trx:trx[r._heading[0]]
    return reduce(lambda x,y: x + y, (expression(tr) for tr in r._scan()), 0)

def AVG(r, expression = lambda trx:None):   #todo None ok? cause error = good?
    """Average expression"""
    if expression.func_code.co_consts==(None,) and len(r._heading)==1:     #if no expression but 1 attribute, use it
        expression = lambda trx:trx[r._heading[0]]
    n = v = 0
    for tr in r._scan():
        n += 1
        v += expression(tr)
    if n==0: return -99999  #todo debug: REMOVE! seems to be needed to avoid div-by-0 error in testDate summarize!?
    #todo AVG of empty set should return error
    return v / float(n)    #todo check div op & exception behaviour

def MAX(r, expression = lambda trx:None):   #todo None ok? cause error = good?
    """Max expression"""
    if expression.func_code.co_consts==(None,) and len(r._heading)==1:     #if no expression but 1 attribute, use it
        expression = lambda trx:trx[r._heading[0]]
    init = -sys.maxint
    for tr in r._scan():   #assumes will yield
        if isinstance(expression(tr), (datetime, )):
            init = datetime.min
        break
        #todo etc.

    return reduce(max, (expression(tr) for tr in r._scan()), init)   #todo sys.maxint  #todo increase to biggest long

def MIN(r, expression = lambda trx:None):   #todo None ok? cause error = good?
    """Min expression"""
    if expression.func_code.co_consts==(None,) and len(r._heading)==1:     #if no expression but 1 attribute, use it
        expression = lambda trx:trx[r._heading[0]]
    init = sys.maxint
    for tr in r._scan():   #assumes will yield
        if isinstance(expression(tr), (datetime, )):
            init = datetime.max
        break
        #todo etc.

    return reduce(min, (expression(tr) for tr in r._scan()), init)   #todo increase to smallest long


def ALL(r, expression = lambda trx:None):   #todo None ok? cause error = good?
    """All expression"""
    if expression.func_code.co_consts==(None,) and len(r._heading)==1:     #if no expression but 1 attribute, use it
        expression = lambda trx:trx[r._heading[0]]
    #todo Use Python 2.5's ALL?
    for tr in r._scan():
        if not expression(tr):
            return False
    return True

def ANY(r, expression = lambda trx:None):   #todo None ok? cause error = good?
    """Any expression"""
    if expression.func_code.co_consts==(None,) and len(r._heading)==1:     #if no expression but 1 attribute, use it
        expression = lambda trx:trx[r._heading[0]]
    #todo Use Python 2.5's ANY?
    for tr in r._scan():
        if expression(tr):
            return True
    return False

##Relational constants
TRUE = DEE = TABLE_DEE = Relation([], [{}])
FALSE = DUM = TABLE_DUM = Relation([], [])

#todo enforce: sys schema?: RelationReadOnly = ['TRUE', 'FALSE', 'DEE', 'DUM', 'TABLE_DEE', 'TABLE_DUM']
############

def IS_EMPTY(r):
    return COUNT(r)==0


####################################################################
if __name__=="__main__":
    print __doc__
    print __copyright__
    print "Version", __version__
    print
    print "(See DeeDoc.html for the user guide)"
