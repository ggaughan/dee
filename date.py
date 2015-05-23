from Dee import Relation, Key, ForeignKey, Tuple, QUOTA, MAX, MIN, IS_EMPTY, COUNT
from DeeDatabase import Database

class date_Database(Database):
    def __init__(self, name):
        """Define initial relvars and their initial values here
           (Called once on database creation)"""
        Database.__init__(self, name)

        if 'S' not in self:
            print "Adding S..."
            self.S = Relation(['S#', 'SNAME',    'STATUS', 'CITY'],
                        [('S1', 'Smith',    20,     'London'),
                         ('S2', 'Jones',    10,     'Paris'),
                         ('S3', 'Blake',    30,     'Paris'),
                         ('S4', 'Clark',    20,     'London'),
                         ('S5', 'Adams',    30,     'Athens'),
                        ],
                        {'pk':(Key,['S#'])})

        if 'P' not in self:
            print "Adding P..."
            self.P = Relation(['P#', 'PNAME',    'COLOR',    'WEIGHT', 'CITY'],
                        [('P1', 'Nut',      'Red',      12,     'London'),
                         ('P2', 'Bolt',     'Green',    17,     'Paris'),
                         ('P3', 'Screw',    'Blue',     17,     'Rome'),
                         ('P4', 'Screw',    'Red',      14,     'London'),
                         ('P5', 'Cam',      'Blue',     12,     'Paris'),
                         ('P6', 'Cog',      'Red',      19,     'London'),
                        ],
                        {'pk':(Key,['P#'])})

        if 'SP' not in self:
            print "Adding SP..."
            self.SP = Relation(['S#', 'P#', 'QTY'],
                         [('S1', 'P1', 300),
                          ('S1', 'P2', 200),
                          ('S1', 'P3', 400),
                          ('S1', 'P4', 200),
                          ('S1', 'P5', 100),
                          ('S1', 'P6', 100),
                          ('S2', 'P1', 300),
                          ('S2', 'P2', 400),
                          ('S3', 'P2', 200),
                          ('S4', 'P2', 200),
                          ('S4', 'P4', 300),
                          ('S4', 'P5', 400),
                         ],
                         {'pk':(Key,['S#', 'P#']),
                          'fkS':(ForeignKey, ('S', {'S#':'S#'})),
                          'fkP':(ForeignKey, ('P', {'P#':'P#'})),
                          }
                        )



    def _vinit(self):
        """Define virtual relvars/relconsts
           (Called repeatedly, e.g. after database load from disk or commit)
        """
        Database._vinit(self)



#Load or create the database
date = Database.open(date_Database, "date")

###################################
if __name__=="__main__":
    print date.relations
