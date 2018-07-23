"""Database persistence plug-in for SQLite
"""

import sqlite3
from ..relation import Relation
from .persist import Persist
import os

class SQLite(Persist):
    def __init__(self, database, persistence_location=None, debug=False):
        """
            Note: passing ':memory:' for persistence_location will store in memory only

                  passing '' for persistence_location will store in memory but allow paging to disk
                  e.g. for large databases
        """
        self.database = database
        if persistence_location is None:
            persistence_location = '/tmp/dee.db'
        self.persistence_location = persistence_location
        self.debug = debug

    def connect(self):
        #self.con = sqlite3.connect(self.persistence_location, isolation_level='EXCLUSIVE')  #todo use database name
        #self.con = sqlite3.connect(self.persistence_location, isolation_level=None)  #todo use database name
        self.con = sqlite3.connect(self.persistence_location, isolation_level=None, check_same_thread=False)  #todo use database name
        #self.con = sqlite3.connect(self.persistence_location, isolation_level='')  #todo use database name
        #self.con = sqlite3.connect(self.persistence_location)  #todo use database name

    def disconnect(self):
        self.con.close()

    def delete(self):
        #todo check not connected
        if self.persistence_location != ':memory:' and self.persistence_location != '':
            os.remove(self.persistence_location)

    def store(self, item, before, after):
        #todo Issues:
        #  nested relations RVAs use repr()

        if before.heading != after.heading:
            #Table structure has changed
            #for now, drop and re-create it with the after rows
            #todo: in future could maybe drop/add/rename columns and still apply diffs
            before = Relation(after.heading)

        deletes = (before - after).to_tuple_list()
        inserts = (after - before).to_tuple_list()

        if self.debug:
            print("%s deletes:" % item)
            print(deletes)
            print("%s inserts:" % item)
            print(inserts)
            print

        cur = self.con.cursor()
        # Create the table if it's new or had no rows or has changed structure (so we can re-create column types based on new inserts)
        if not len(before) or before.heading != after.heading:
            if self.debug:
                print("creating table")
            #Note: previous tx must have committed
            cur.executescript("DROP TABLE IF EXISTS %s;" % item)
            #todo: what effect does the implicit commit have on our user transaction?!
            cur.execute("CREATE TABLE %s (%s)" % (item, ",".join(after.heading)))

        #Note: other databases would need to convert to native types here
        cur.executemany("DELETE FROM %s WHERE %s" % (item, " AND ".join(["%s=?" % a for a in after.heading])),
                    [tuple([getattr(delete, a) for a in after.heading]) for delete in deletes])

        cur.executemany("INSERT INTO %s (%s) VALUES (%s)" % (item, ",".join(after.heading), ",".join(["?" for a in after.heading])),
                    [tuple([repr(getattr(insert, a)) for a in after.heading]) for insert in inserts])

    def drop(self, item):
        if self.debug:
            print("%s drop" % item)
            print

        cur = self.con.cursor()
        # Drop the table
        #Note: previous tx must have committed
        cur.executescript("DROP TABLE IF EXISTS %s;" % item)
        #todo: what effect does the implicit commit have on our user transaction?!

    def begin(self):
        #cur = self.con.cursor()
        #cur.execute("SAVEPOINT inner")
        pass

    def commit(self):
        #self.con.commit()
        #cur = self.con.cursor()
        #cur.execute("RELEASE inner")
        pass

    def rollback(self):
        #self.con.rollback()
        #cur = self.con.cursor()
        #cur.execute("ROLLBACK inner")
        pass

    def begin_outer(self):
        #cur = self.con.cursor()
        ##cur.execute("SAVEPOINT outer")
        #cur.execute("BEGIN")
        pass

    def commit_outer(self):
        #cur = self.con.cursor()
        ##cur.execute("RELEASE outer")
        #cur.execute("COMMIT")
        self.con.commit()

    def rollback_outer(self):
        #cur = self.con.cursor()
        ##cur.execute("ROLLBACK")
        #cur.execute("ROLLBACK")
        self.con.rollback()

    def get_relations(self):
        cur = self.con.cursor()
        names = []
        for row in cur.execute("SELECT name from sqlite_master"):
            names.append(row[0])

        for name in names:
            attrs = []
            for row in cur.execute("PRAGMA table_info(%s)" % name):
                attrs.append(row[1])

            cur.execute("SELECT %s FROM %s" % (",".join(attrs), name))
            rows = cur.fetchall()
            body = []
            #eval the repr here to retrieve the original type info
            for row in rows:
                cols = []
                for col in row:
                    cols.append(eval(col))
                body.append(tuple(cols))

            if self.debug:
                print("Restoring %s with %s tuples" % (name, len(body)))

            yield (name, Relation(attrs, body))
