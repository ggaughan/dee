from .util.orderedset import OrderedSet
from .util.disassemble import extract_keys

import sys
import types
import copy
from functools import reduce
import dee

from collections import namedtuple

from datetime import datetime

class InvalidAttributeName(Exception):
    def __init__(self, *args):
        pass

class InvalidTuple(Exception):
    def __init__(self, *args):
        pass

class InvalidOperation(Exception):
    def __init__(self, *args):
        pass

class Relation(object):

    def __init__(self, heading, body=None):
        """Construct a relation

           heading lists the fields names.

           The field_names are a single string with each fieldname separated by whitespace and/or commas,
           for example 'x y' or 'x, y'. Alternatively, field_names can be a sequence of strings such as ['x', 'y'].

           Any valid Python identifier may be used for a fieldname except for names starting with an underscore.
           Valid identifiers consist of letters, digits, and underscores but do not start with a digit or underscore
           and cannot be a keyword such as class, for, return, global, pass, or raise.

           body is either:
               a list of tuples - duplicates will be removed
               a lambda expression which will return a list of tuples (i.e. deferred pipeline - note: immediate resolutions)
               a lambda expression which will return a list of tuples (e.g. relationFromCondition)
        """
        self._myhash = None
        self._pipelined  = False
        self._database = None
        self._database_item = None
        try:
            class Tuple(namedtuple('Tuple', heading)):
                __slots__ = ()
                @property
                def heading(self):
                    return OrderedSet(self._fields)
        except ValueError:
            raise InvalidAttributeName
        self.Tuple = Tuple
        self.heading = OrderedSet(self.Tuple._fields)
        if hasattr(body, '__call__'):
            self._body = body
            self._pipelined = (body.func_name == '<lambda>')  #i.e. not parameter-needing, e.g. relationFromCondition wrapper
        else:
            self._body = []
            if body:
                self._add_to_body(body)

    def _as_tuple(self, r):
        """Convert r into a Tuple if possible, else return None"""
        if type(r) is self.Tuple:
            return r
        elif isinstance(r, dict):
            return self.Tuple(**r)
        elif isinstance(r, tuple) and hasattr(r, '_fields'):
            return self.Tuple(**r._asdict())
        elif isinstance(r, list) or isinstance(r, tuple): #assumes caller knows order is correct
            return self.Tuple(*r)
        return None

    @staticmethod
    def from_tuple(tr):
        """Converts a Tuple into a relation"""
        return Relation([k for k in tr], [tuple([v for v in tr.values()])])

    def _add_to_body(self, body):
        if hasattr(body, '__call__'):
            raise Exception("Cannot modify a View body")  #todo use more refined type
        #todo optimise and consider constraints/rollback (done by database?)
        for r in body:
            t = self._as_tuple(r)
            if t is not None:
                if t not in self._body:
                    self._body.append(t)
                    self._myhash = None
            else:
                raise InvalidTuple()

    def _remove_from_body(self, body):
        if hasattr(body, '__call__'):
            raise Exception("Cannot modify a View body")  #todo use more refined type
        #todo optimise and consider constraints/rollback (done by database?)
        for r in body:
            t = self._as_tuple(r)
            if t is not None:
                try:
                    self._body.remove(t)
                    self._myhash = None
                except ValueError:
                    pass
            else:
                raise InvalidTuple()


    def _is_deferred(self):
        """Returns True if the body of this relation is deferred"""
        return self._pipelined
    is_deferred = property(_is_deferred)

    def _scan(self, rel=None):
        #todo: if the body is pipelined, buffer a number of rows
        #      so we can save re-starting the pipeline below a certain limit

        #todo: move the View code out to View._scan
        if rel is None:
            if isinstance(self, View):
                for tup in self._body()._scan():
                    yield tup
            else:
                if self.is_deferred:
                    for tup in self._body():
                        yield self._as_tuple(tup)
                else:
                    if hasattr(self._body, '__call__'):
                        for tup in self._body():
                            yield self._as_tuple(tup)
                    else:
                        for tup in self._body:
                            yield tup
        else:
            com = self.heading & rel.heading
            if len(com)==0:
                #if callable(self._body):
                    #if self._body.func_code.co_argcount == 0 or (self._body.func_code.co_argcount == 1 and isinstance(self._body, types.MethodType)):   #e.g. a view (function which returns a Relation, i.e. complete set) #todo unless recursive!
                        #for tup in self._body():
                            #yield self._as_tuple(tup)
                    #else:
                        #for tup in self._body(self.Tuple()):
                            ##yield self._as_tuple(tup)
                            #yield tup
                #else:
                    #for tup in self._body:
                        ##yield Tuple(zip(self._heading, tup))
                        #yield tup
                if isinstance(self, View):
                    for tup in self._body(self.Tuple(None)):
                        yield self._as_tuple(tup)
                else:
                    if self.is_deferred:
                        for tup in self._body():
                            yield self._as_tuple(tup)
                    else:
                        if hasattr(self._body, '__call__'):
                            for tup in self._body(rel):
                                yield self._as_tuple(tup)
                        else:
                            for tup in self._body:
                                yield tup
            else:
                if isinstance(self, View):
                    if self.is_deferred:
                        #Create result tuple with fixed attributes from rel
                        res = self.Tuple(**dict.fromkeys(self.Tuple._fields, None))
                        res._replace(**rel._asdict())
                        #Call the lambda to retrieve a dictionary result and overlay it on the result tuple
                        for tup in self._body(rel):
                            yield res._replace(**tup)
                    else:
                        #todo:
                        #Create result tuple with fixed attributes from rel
                        res = self.Tuple(**dict.fromkeys(self.Tuple._fields, None))
                        res._replace(**rel._asdict())
                        for tup in self._body(**rel._asdict()):
                            yield res._replace(**tup)
                else:
                    if self.is_deferred:
                        #does this mean pipelining means we miss out on chances to use optimised hash join?
                        #i.e. sometimes better to materialise and hash-join
                        for tup in self._body():
                            t = self._as_tuple(tup)
                            if all([getattr(t, attr) == getattr(rel, attr) for attr in com]):
                                yield t
                    else:
                        if hasattr(self._body, '__call__'):
                            #Create result tuple with fixed attributes from rel
                            res = self.Tuple(**dict.fromkeys(self.Tuple._fields, None))
                            res._replace(**rel._asdict())
                            for tup in self._body(rel):
                                yield res._replace(**tup)
                        else:
                            #todo use optimised hash join
                            for tup in self._body:
                                if all([getattr(tup, attr) == getattr(rel, attr) for attr in com]):
                                    yield tup

    #Note: original intention was _scan would return tuples and _pipeline would yield header then tuples
    #but pipelining seems to always have heading pre-calculated when Relation is declared
    _pipeline = _scan  #todo: for now

    def __hash__(self):
        """Hash"""
        if self._myhash is None:
            myhash=0
            if self.is_deferred:
                #todo ok to equate views etc.?
                myhash = hash(self._body.func_code.co_code)  #todo ok
            else:
                for tup in self._body:
                    myhash ^= hash(tup) #todo track call-count
                #print "iterated hash called on", self.heading

            self._myhash = myhash

        return self._myhash

    def __contains__(self, rel):
        """Membership, e.g. t in r1, r1 in r2"""
        #todo if isinstance(rel, Relation): return rel <= self   #i.e. subset
        t = self._as_tuple(rel)

        if t is not None:
            if self.is_deferred:
                for tup in self._body():
                    if self._as_tuple(tup) == t:
                        return True
            else:
                if t in self._body:
                    return True
        #else error: plain tuple not clear enough?
        return False  #todo: perhaps return None if unknown rel, and False if known but not found

    def __eq__(self, r):
        if set(self.heading) == set(r.heading):
            if len(self) == len(r):
                #todo improve speed! evaluating expressions twice unless we cache (one for len and again for for)
                for nt in self._pipeline():
                    if nt not in r:
                        return False
                return True
        return False

    def __ne__(self, r):
        return not self.__eq__(r)

    def __len__(self):
        return COUNT(self)

    def __and__(self, r):
        return AND(self, r)

    def __or__(self, r):
        return OR(self, r)

    def __ior__(self, rel):
        """In-place OR, i.e. insertion"""
        #todo assert rel is relation or tuple else: raise RelationInvalidOperationException(self, "insert expects a Relation or a Tuple")

        if self.is_deferred:
            self._materialise()

        #todo remove: if isinstance(rel, tuple):
        if self._as_tuple(rel):
            return self.__ior__(Relation.from_tuple(rel))

        #Note copied from OR
        if set(self.heading) != set(rel.heading):
            raise InvalidOperation("OR can only handle same reltypes so far: %s" % str(rel.heading))

        body=[]
        for tr2 in rel._scan():
            body.append(tuple([getattr(tr2, attr) for attr in self.heading]))  #Note deep copies varying dict

        self._add_to_body(body)

        if self._database:
            #todo: work out which tuples to insert and call persist routines from here
            #for now we drop and then set new self
            cc = self._database._constraint_checking  #leave off if already off, e.g. in a with
            self._database._constraint_checking = False
            try:
                del self._database[self._database_item]
            finally:
                self._database._constraint_checking = cc
            try:
                self._database.__setattr__(self._database_item, self)
            except Exception, e:  #todo trap Constraint exceptions specifically
                self._remove_from_body(body)
                #todo? perhaps turn off constraint checking to be sure we can revert
                self._database.__setattr__(self._database_item, self)
                raise

        return self

    def __sub__(self, rel):
        return MINUS(self, rel)

    def __isub__(self, rel):
        """In-place MINUS, i.e. deletion"""
        #todo assert rel is relation or tuple else: raise RelationInvalidOperationException(self, "delete expects a Relation or a Tuple")

        if self.is_deferred:
            self._materialise()

        if self._as_tuple(rel):
            return self.__isub__(Relation.from_tuple(rel))

        #Note copied from MINUS
        if set(self.heading) != set(rel.heading):
            raise InvalidOperation("MINUS can only handle same relation types so far: %s" % str(rel.heading))

        body=[]
        for tr2 in rel._scan():
            body.append(tuple([getattr(tr2, attr) for attr in self.heading]))  #Note deep copies varying dict

        #todo save before so we can db.check_constraints

        self._remove_from_body(body)

        if self._database:
            #todo: work out which tuples to delete and call persist routines from here
            #for now we drop and then set new self
            cc = self._database._constraint_checking  #leave off if already off, e.g. in a with
            self._database._constraint_checking = False
            try:
                del self._database[self._database_item]
            finally:
                self._database._constraint_checking = cc
            try:
                self._database.__setattr__(self._database_item, self)
            except Exception, e:  #todo trap Constraint exceptions specifically
                self._add_to_body(body)
                #todo? perhaps turn off constraint checking to be sure we can revert
                self._database.__setattr__(self._database_item, self)
                raise

        return self

    def insert(self, rel):
        """Insert new relation/tuple (in place) into relation"""
        self.__ior__(rel)

    def delete(self, rel):
        """Delete relation/tuple (in place) from relation"""
        self.__isub__(rel)

    def update(self, restriction = lambda r:True, setting = lambda t:{}):
        """Update tuples (in place) in relation, e.g. r.update(lambda t: t.x==3, ['UpdAttr'], lambda u: {'UpdAttr':4})"""
        if not hasattr(setting, '__call__'):
            raise InvalidOperation("setting should be a function, e.g. lambda t: {'new':t.id * 3} (%s)" % setting)

        hsetting = extract_keys(setting)    #find the heading from the setting

        old = self.where(restriction)
        t1 = old.rename(dict(zip(self.heading, ['OLD_'+x for x in self.heading])))
        upd = t1.extend(setting)
        hupdold = set(['OLD_'+x for x in hsetting])
        hupdback = upd.heading - hupdold
        new = upd.remove(hupdold).rename(dict(zip([x for x in hupdback if x.startswith('OLD_')],
                                                  [x.lstrip('OLD_') for x in hupdback if x.startswith('OLD_')]))
                                        )
        #todo: make these two atomic!
        self -= old
        self |= new
        #todo: post-check: if pre-card-self != post-card-self -> rollback+exception

    def rename(self, newnames):
        """Rename one or more columns, e.g. r.rename({'a':'a1'})
           Note: could implement this with REMOVE and EXTEND (which is itself a macro on top of AND)
           todo: allow prefix/suffix global modifications
        """
        for attr in newnames.keys():
            if attr not in self.heading:
                raise InvalidOperation(self, "Unknown attribute: %s" % attr)
        heading = tuple([newnames.get(c, c) for c in self.heading])

        #Validate the new heading
        try:
            class Tuple(namedtuple('Tuple', heading)):
                __slots__ = ()
                @property
                def heading(self):
                    return OrderedSet(self._fields)
        except ValueError:
            raise InvalidAttributeName

        #todo: infer and update constraints

        if isinstance(self, View):
            #todo return a View not a Relation here?
            s = Relation(heading, self._body)
            #Keep track of original headings so we can call using kwargs
            for (k, v) in newnames.items():
                s._mapToOriginalHeading[v] = s._mapToOriginalHeading.get(k, k)
                if k in s._mapToOriginalHeading:
                    del(s._mapToOriginalHeading[k])
            return s
        else:
            if self.is_deferred:
                #todo: perhaps defer longer?
                body = []
                for tr1 in self._pipeline():
                    #body.append(Tuple(*tr1))  #todo is this faster/better?
                    body.append(tuple([getattr(tr1, attr) for attr in self.heading]))
                return Relation(heading, body)
            else:
                body = []
                for tr1 in self._body:
                    #body.append(Tuple(*tr1))  #todo is this faster/better?
                    body.append(tuple([getattr(tr1, attr) for attr in self.heading]))
                return Relation(heading, body)

    def project(self, head):
        """Relational PROJECT (shorthand/macro for REMOVE)"""
        #todo handle single string -> [string]?
        for attr in head:  #todo improve with type checks and a - b test for leftovers
            if attr not in self.heading:
                raise InvalidOperation(self, "Unknown attribute: %s" % attr)
        remove_head = self.heading - set(head)
        return REMOVE(self, remove_head)

    def where(self, restriction=lambda r:True):
        """Restrict rows by condition, e.g. r.where(lambda t: t.x==3)"""
        return RESTRICT(self, restriction)

    def extend(self, extension=lambda t:{}):
        """Extend rows based on extension

           t must return a dictionary of attribute names and expressions,
           e.g. lambda t:{'total':t.quantity * t.price}

           (creates pseudo relation for the extension and then joins)"""
        return EXTEND(self, extension)

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

    def remove(self, head):
        """Remove one or more columns (-> REMOVE), e.g. r.remove(['a','b'])"""
        #todo validateHeading(head)
        return REMOVE(self, head)

    def __call__(self, head):
        """Calls through to project (shorthand), e.g. r(['id', 'name'])"""
        #todo test with set literal syntax, e.g. {'a', 'b'}
        return self.project(head)

    def __repr__(self):
        """Return relation as Python definition syntax, e.g. Relation(['x','y'], [{'x':1,'y':2}, {'x':3,'y':4}])"""
        sbody = None
        if sbody is None:
            sbody = "[%s]" % (",\n".join([str(row._asdict()) for row in self.to_tuple_list()]))

        return "Relation([%(heading)s],\n%(body)s)" % {'heading':",".join(["'%s'" % attr for attr in self.heading]),
                                                     'body':sbody
                                                     }

    #todo remove def __repr__(self):
    #    return "%s %s" % (self.heading, self._body)  #todo return Python

    #todo __unicode__
    def __str__(self):
        columns = range(len(self.heading))

        col_width = [len(list(self.heading)[col]) for col in columns]

        if self.is_deferred:
            #todo only if isinstance(self, View)?
            if True:
                for tup in self._scan(): #-> ._scan():
                    for col,colname in enumerate(self.heading):
                        n=max([len(s) for s in str(getattr(tup, colname)).splitlines()]) #todo fix for empty string!
                        col_width[col] = max(col_width[col], n)
            else:
                for col in columns:
                    col_width[col] = max(col_width[col], 20)     #todo improve?
        else:
            #note: full pass
            for tup in self._body:
                for col in columns:
                    if len(str(tup[col])) > 0:
                        n = max([len(s) for s in str(tup[col]).splitlines()])
                    else:
                        n = 0
                    col_width[col] = max(col_width[col], n)

        col_widthmap = dict(zip(self.heading, col_width))

        hline = "%s+\n" % "".join("+".ljust(col_width[col]+3, "-") for col in columns)
        #todo: hlineKeyed = "%s+\n" % "".join("+".ljust(col_width[col]+3, "-="[self._headingAttributeIsKey(self._heading[col])]) for col in columns)

        def line(row):
            l = []
            vals = {}
            for col in self.heading:
                if isinstance(row, dict):
                    vals[col] = str(row[col]).splitlines()
                else:
                    vals[col] = str(getattr(row, col)).splitlines()

            if len(vals)>0:
                for r in range(max([len(n) for n in vals.values()])):
                    lr = []
                    for col in self.heading:
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
        result.append(line(dict(zip(self.heading, self.heading))))
        #todo result.append(hlineKeyed)
        result.append(hline)

        for tup in self._scan():
            result.append(line(tup))

        result.append(hline)

        return "".join(result)[:-1] #debug + ("(%s)" % getPerfs())

    #todo temporary: rename/remove
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
                if False: #todo self._headingAttributeIsKey(col):
                    l.append("<th><strong>%s</strong></th>" % displayName(col))
                else:
                    l.append("<th>%s</th>" % displayName(col))

            return "<thead>" + "".join(l) + "</thead>"

        def line(columns, row):
            l = []
            for col in columns:
                if isinstance(getattr(row, col), Relation):
                    v = getattr(row, col).renderHTML()
                else:
                    if isinstance(getattr(row, col), datetime):
                        v = getattr(row, col).strftime('%c')
                    else:
                        v = str(getattr(row, col))

                s = v
                if '*' in link_columns:
                    s = '<a href="%s">%s</a>' % (link_columns['*'] % row, v)
                if col in link_columns:
                    s = '<a href="%s">%s</a>' % (link_columns[col] % row, v)
                l.append("<td>%s</td>" % s)

            return "<tr>" + "".join(l) + "</tr>"


        if not columns:
            columns = self.heading

        if not link_columns:
            link_columns = {}

        result = [head(columns), '<tbody>']

        rows = self.to_tuple_list(sort)

        i = 0
        for tup in rows:
            if row_limit and i >= row_limit:
                break
            result.append(line(columns, tup))
            i += 1

        return '<table>' + "".join(result) + "</tbody></table>"

    def to_tuple_list(self, key=lambda t:t, reverse=False):
        """Converts a relation to a Tuple list
        """
        rows = sorted([tr for tr in self._scan()], key=key, reverse=reverse)
        return rows

    def _materialise(self):
        """Materialises a relation body from a deferred pipeline
        """
        if self.is_deferred:
            new_body = [tr for tr in self._scan()]
            self._body = []
            if new_body:
                self._add_to_body(new_body)
            self._pipelined=False
        #else no need
Rel = Relation  #alias

#todo remove
def test(a):
    for x in xrange(3):
        yield (x+1, x+2, x+3)
test.heading = ['a','b','c']

class View(Relation):

    def __init__(self, body):
        """Construct a virtual relation

           body is either:
               a lambda expression which will return a Relation expression
               a generator which will yield a set of tuples

           The heading will be derived from the body expression if it's a relation expression
           or from the generator parameters otherwise.
        """
        if body.func_name == '<lambda>':
            heading = body().heading  #note: may cause body expression to be calculated #todo so cache it now?
        else:
            #we are wrapping a generator as a relation, so use its parameter list
            heading = body.func_code.co_varnames[:body.func_code.co_argcount]
        self._myhash = None
        self._pipelined  = False
        self._database = None
        self._database_item = None
        try:
            class Tuple(namedtuple('Tuple', heading)):
                __slots__ = ()
                @property
                def heading(self):
                    return OrderedSet(self._fields)
        except ValueError:
            raise InvalidAttributeName
        self.Tuple = Tuple
        self.heading = OrderedSet(self.Tuple._fields)
        if hasattr(body, '__call__'):
            self._body = body
            self._pipelined = (body.func_name == '<lambda>')  #i.e. not parameter-needing, e.g. generator
        else:
            raise Exception("body needs to be a deferred Relation expression, e.g. lambda:R1 & R2")  #todo use more refined type

    #todo heading property if we need to refresh it on every call

    def __ior__(self, rel):
        """In-place OR, i.e. insertion"""
        raise InvalidOperation("Cannot assign to a view")

    def __isub__(self, rel):
        """In-place MINUS, i.e. deletion"""
        raise InvalidOperation("Cannot assign to a view")

    def _materialise(self):
        """Materialises a relation body from a deferred pipeline
        """
        raise InvalidOperation("Cannot materialise a view")

##Wrappers: these wrap a lambda expression and make it behave like a relation so the A algebra can be used,
## e.g. RESTRICT and EXTEND can be implemented in terms of AND
def relationFromCondition(f):
    def wrapper(trx):
        if f(trx):
            return [{}]    #DEE, i.e. True
        else:
            return []      #DUM, i.e. False

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
    #Ensure any callable is in r2 position (e.g. needed for EXTEND)
    #todo is this too loose, e.g. use is_deferred/_is_pipelined too
    if hasattr(r1._body, '__call__') and not r1.is_deferred:
        if not hasattr(r2._body, '__call__') or r2.is_deferred:
            return AND(r2, r1)

    #todo optimise the order
    hs = r1.heading | r2.heading

    res = Relation(hs, lambda:_and(r1, r2))  #defer
    if True:  #todo: if not deferrable
        res._materialise()
    return res

def _and(r1, r2):
    hs = r1.heading | r2.heading

    seen = set()  #todo: optimise if possible (e.g. if PK in both)
    hs1 = hs & r1.heading
    hs2 = hs - r1.heading
    hcommon = r1.heading & r2.heading
    r2_Ttuple = namedtuple('Ttuple', r2.heading)
    r2_Ttuple.heading = OrderedSet(r2.heading)
    for tr1 in r1._pipeline():
        if hasattr(r2._body, '__call__') and not r2.is_deferred:
            # pass only those parameters expected by the view def
            tr1_matching = r2_Ttuple(**dict(zip(r2.heading, [getattr(tr1, attr) if hasattr(tr1, attr) else None for attr in r2.heading])))
        else:
            tr1_matching = tr1
        for tr2 in r2._pipeline(tr1_matching):
            #if all([getattr(tr1, attr) == getattr(tr2, attr) for attr in hcommon]): #todo remove when scan works (& hcommon)
            t = tuple([getattr(tr1, attr) for attr in hs1] + [getattr(tr2, attr) for attr in hs2])
            if t in seen:
                continue
            seen.add(t)
            yield t

def OR(r1, r2):
    """Or/Union
       Equates to union (both relations have same heading)
    """
    if set(r1.heading) != set(r2.heading):
        raise InvalidOperation("OR can only handle same reltypes so far: %s" % str(r2.heading))

    #todo optimise the order
    hs = r1.heading | r2.heading  #== r1.heading

    res = Relation(hs, lambda:_or(r1, r2))  #defer
    if True:  #todo: if not deferrable
        res._materialise()
    return res

def _or(r1, r2):
    hs = r1.heading | r2.heading  #== r1.heading

    seen = set()  #todo: optimise? e.g. track smallest i.e. ensure r1 is smallest
    for tr1 in r1._pipeline():
        t = tuple([getattr(tr1, attr) for attr in hs])
        seen.add(t)
        yield t  #assumes r1 has no duplicates
    for tr2 in r2._pipeline():
        t = tuple([getattr(tr2, attr) for attr in hs])
        if t in seen:
            continue
        yield t  #assumes r2 has no duplicates

def MINUS(r1, r2):
    """Returns all rows not in the first relation with respect to the second relation
       Equivalent to r1 & (not r2), i.e. r1 minus r2
    """
    if set(r1.heading) != set(r2.heading):
        raise InvalidOperation("NOT can only handle same relation types so far: %s" % str(r2.heading))

    #todo optimise the order
    hs = r1.heading

    res = Relation(hs, lambda:_minus(r1, r2))  #defer
    if True:  #todo: if not deferrable
        res._materialise()
    return res

def _minus(r1, r2):
    if set(r1.heading) != set(r2.heading):
        raise InvalidOperation("NOT can only handle same relation types so far: %s" % str(r2.heading))

    hs = r1.heading
    #Note: assumes source has no duplicates
    for tr1 in r1._pipeline():
        for tr2 in r2._pipeline(tr1):
            break
        else:
            yield tr1

def REMOVE(r, hr):
    """Remove one or more columns, e.g. REMOVE(r, ['a','b'])"""
    try:
        hs = r.heading - set(hr)
    except:
        raise InvalidOperation("Heading attribute(s) should be iterable (%s)" % str(hr))

    res = Relation(hs, lambda:_remove(r, hr))  #defer
    if True:  #todo: if not deferrable
        res._materialise()
    return res

def _remove(r, hr):
    hs = r.heading - set(hr)
    seen = set()  #todo: optimise, e.g. no need if PK remains
    for tr in r._pipeline():
        t = tuple([getattr(tr, attr) for attr in hs])
        if t in seen:
            continue
        seen.add(t)
        yield t

### Macros ###
def COMPOSE(r1, r2):
    """AND and then REMOVE common attributes
       (macro)"""
    a = r1.heading & r2.heading
    return REMOVE(AND(r1, r2), a)

def RESTRICT(r, restriction = lambda trx:True):
    """Restrict rows based on condition
       (creates pseudo relation for the condition and then joins i.e. implemented in terms of AND)
       (macro)"""
    if not hasattr(restriction, '__call__'):
        raise InvalidOperation("Restriction should be a function, e.g. lambda t: t.id == 3 (%s)" % restriction)

    #todo: if restriction like trx.x=C1 and trx.y=C2 and etc. then create non-functional relation from Tuple(x=C1, y=C2 etc.) and AND this first = speed

    return AND(r, Relation(r.heading, relationFromCondition(restriction)))
#todo? WHERE=RESTRICT

def EXTEND(r, extension = lambda trx:{}):
    """Extend rows based on extension
       (creates pseudo relation for the extension and then joins i.e. implemented in terms of AND)
       (macro)"""
    if not hasattr(extension, '__call__'):
        raise InvalidOperation("Extension should be a function, e.g. lambda t: {'new':t.id * 3} (%s)" % extension)

    hextension = extract_keys(extension)    #find the heading from the extension
    extheading = OrderedSet(hextension)
    if r.heading & extheading != set():
        raise InvalidOperation("EXTEND heading attributes conflict with relation being extended: %s" % (r.heading & extheading))

    return AND(r, Relation(r.heading | extheading, relationFromExtension(extension)))

def _EXTEND_EXPLICIT(r, Hextension=None, extension = lambda trx:{}):
    """Extend rows based on extension with explicit heading
       (creates pseudo relation for the extension and then joins i.e. implemented in terms of AND)
       (macro)"""
    if Hextension is None:
        Hextension = []
    if not hasattr(extension, '__call__'):
        raise InvalidOperation("Extension should be a function, e.g. lambda t: {'new':t.id * 3} (%s)" % extension)

    hextension = set(Hextension)
    extheading = OrderedSet(hextension)
    if r.heading & extheading != set():
        raise InvalidOperation("_EXTEND_EXPLICIT heading attributes conflict with relation being extended: %s" % (r.heading & extheading))

    return AND(r, Relation(r.heading | extheading, relationFromExtension(extension)))

def MATCHING(r1, r2):
    """aka SEMIJOIN
    (macro)"""
    return REMOVE(AND(r1, r2), (r2.heading - r1.heading))

#todo: does NOT_MATCHING == not MATCHING ? - if so, remove this:
def NOT_MATCHING(r1, r2):
    """aka SEMIMINUS
    (macro)"""
    return MINUS(r1, MATCHING(r1, r2))

def IMAGE(r):
    """Image relation based on outer range tuple
       (macro, but with a Python trick to avoid us having to pass the range variable)

       e.g. S.where(lambda t: IMAGE(SP)(['PNO']) == P(['PNO']))
       (Tutorial D equivalent: S WHERE (!!SP){PNO} = P{PNO})
    """
    outer_scope = sys._getframe(1)
    if len(outer_scope.f_code.co_varnames) != 1:
        raise InvalidOperation("IMAGE needs a range variable (e.g. use within RESTRICT or EXTEND): %s" % (r.heading))
    range_var = outer_scope.f_locals[outer_scope.f_code.co_varnames[0]]
    if not hasattr(range_var, '_asdict'):
        raise InvalidOperation("IMAGE needs a range variable (e.g. use within RESTRICT or EXTEND): %s" % (r.heading))
    tx = Relation.from_tuple(range_var._asdict())

    return REMOVE(MATCHING(r, tx), r.heading & tx.heading)

def GROUP(r, Hr, groupname):
    """Grouping
       (macro)

       e.g. GROUP(r, ['D', 'E', 'F'], 'X')  (or r.group(['D', 'E', 'F'], 'X')
       (Tutorial D equivalent: r GROUP { D, E, .., F } AS X
    """
    Hs = r.heading - Hr
    #todo remove:
    #for t in r._scan():
    #    x = Relation.from_tuple(dict(zip(Hs, tuple([getattr(t, attr) for attr in Hs]))))
    #    #todo break?
    y = _EXTEND_EXPLICIT(r, [groupname],
                         lambda t:{groupname:COMPOSE(r, Relation.from_tuple(
                                           dict(zip(Hs, tuple([getattr(t, attr) for attr in Hs])))
                                   ))})

    return y.project(Hs | set([groupname]))

def UNGROUP(r, groupname):
    """Ungrouping
       (macro)

       e.g. UNGROUP(r, 'X')  (or r.ungroup('X'))
       (Tutorial D equivalent: r UNGROUP X)
    """
    #todo assert at least 1 row - document this limitation
    for tr1 in r._pipeline():
        Hs = list(getattr(tr1, groupname).heading)
        break
    Ttuple = namedtuple('Ttuple', [groupname]+Hs)
    T = Relation([groupname]+Hs,
                 [Ttuple._make([getattr(s, groupname)] + [getattr(t, f) for f in t._fields])
                  for s in r([groupname])._pipeline() for t in getattr(s, groupname)._pipeline()]
                )

    return COMPOSE(r, T)

def WRAP(r, Hr, wrapname):
    """Wrapping
       (macro)

       e.g. WRAP(r, ['D', 'E', 'F'], 'X')  (or r.wrap(['D', 'E', 'F'], 'X')
       (Tutorial D equivalent: r WRAP { D, E, .., F } AS X
    """
    Ttuple = namedtuple('Ttuple', Hr)
    return _EXTEND_EXPLICIT(r, [wrapname], lambda t:{wrapname:Ttuple._make([getattr(t, f) for f in Hr])}).remove(Hr)

def UNWRAP(r, wrapname):
    """Unwrapping
       (macro)

       e.g. UNWRAP(r, 'X')  (or r.unwrap('X')
       (Tutorial D equivalent: r UNWRAP X
    """
    #todo assert at least 1 row
    for tr1 in r._pipeline():
        Hs = getattr(tr1, wrapname)._fields
        break

    Ttuple = namedtuple('Ttuple', Hs)
    return _EXTEND_EXPLICIT(r, Hs, lambda t:Ttuple._make([getattr(getattr(t, wrapname), f) for f in Hs])._asdict()).remove([wrapname])

###

def GENERATE(extension = {}):
    """Generate a relvar-independent value
       (macro)

        e.g. GENERATE({'pi':3.14})

        #todo: could replace Relation.fromTuple() with GENERATE()
    """
    return _EXTEND_EXPLICIT(dee.DEE, extension.keys(), lambda trx:extension)


def TCLOSE(r):
    """Transitive closure (an example of a recursive relational operator)
       (macro) (not optimised for speed)
    """
    if len(r.heading) != 2:
        raise InvalidOperation("TCLOSE expects a binary relation, "
                               "e.g. with a heading ['X', 'Y']")

    _X, _Y = r.heading
    TTT = r | (COMPOSE(r, r.rename({_Y:'_Z', _X:_Y})).rename({'_Z':_Y}))
    if TTT == r:
        return TTT
    else:
        return TCLOSE(TTT)


##Aggregate operators
def COUNT(r):
    """Count tuples"""
    if isinstance(r, View):
        return reduce(lambda x,y: x + 1, (1 for tr in r._scan()), 0)
    else:
        if r.is_deferred:
            return reduce(lambda x,y: x + 1, (1 for tr in r._pipeline()), 0)
        else:
            return len(r._body)

#todo: revisit: temp for image tests
def SUM(r, expression = lambda trx:None):   #todo None ok? cause error = good?
    """Sum expression"""
    if expression.__code__.co_consts == (None,) and len(r.heading) == 1:     #if no expression but 1 attribute, use it
        attr = [i for i in r.heading][0]
        expression = lambda trx:getattr(trx, attr)
    return reduce(lambda x,y: x + y, (expression(tr) for tr in r._pipeline()), 0)
