import unittest

from dee import DEE, DUM
from dee.relation import Relation, InvalidAttributeName, AND, REMOVE, OR, EXTEND, COMPOSE, RESTRICT
from collections import namedtuple

import timeit

class TestSpeed(unittest.TestCase):
    
    def test_insert_speed(self):
        data = []
        for i in xrange(10000):
            data.append((i,i*2,i*3,i*4))
        
        t = timeit.Timer("from dee.relation import Relation; r = Relation(['a','b','c','d'], %s); print len(r)" % str(data))

        print t.timeit(1)
        

if __name__ == '__main__':
    unittest.main()