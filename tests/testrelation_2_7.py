"""Tests using syntax only available in Python 2.7+
"""

import unittest

from dee import DEE, DUM
from dee.relation import Relation, InvalidAttributeName, AND
from collections import namedtuple

class TestRelation(unittest.TestCase):
    def test_new_set_literal(self):
        r1 = Relation(['a','b','c','d'], 
                      {(10,20,20,30),
                       (10,20,30,40),
                       (30,40,20,30),
                       (30,40,30,40),
                      })
        self.assertEqual(AND(r1, DEE),
                         r1)
        
if __name__ == '__main__':
    unittest.main()