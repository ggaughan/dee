#todo test results directly against persistence backend too

import unittest

from dee.database import Database, InvalidDatabaseItem
from dee.relation import Relation, View, AND, COUNT

from dee.util.persist_sqlite import SQLite

class TestDatabase(unittest.TestCase):

    def setUp(self):
        self.d = Database()
        if self.d:
            self.d.delete()
        
    def tearDown(self):
        if self.d:
            self.d.delete()
    
    def test_construction(self):
        self.d = Database()
        
    def test_construction_with_explicit_engine(self):
        self.d = Database(persistence_engine=SQLite)
        
    def test_invalid_item(self):
        self.d = Database()
        try:
            self.d.r1 = ['not', 'a', 'relation']
        except InvalidDatabaseItem:
            pass
        else:
            self.fail()
            
    def test_valid_item(self):
        self.d = Database()
        self.d.r1 = Relation(['a', 'b'], [])
        self.assertEqual(self.d.r1, Relation(['a', 'b'], []))
        self.assertNotEqual(self.d.r1, Relation(['a', 'b', 'c'], []))
        self.assertEqual(self.d.catalog.relvars, Relation(['name'], [('r1',)]))
        
    def test_valid_item_deep1(self):
        self.d = Database()
        a1 = Relation(['a', 'b'], [])
        self.d.r1 = a1
        self.assertEqual(self.d.r1, Relation(['a', 'b'], []))
        self.assertEqual(self.d.r1, a1)
        self.assertNotEqual(self.d.r1, Relation(['a', 'b', 'c'], []))
        
        self.assertFalse(self.d.r1.is_deferred)  #note: implementation detail - need not be?
        self.assertFalse(isinstance(self.d.r1, View))
        
    def test_valid_item_deep2(self):
        self.d = Database()
        a1 = Relation(['a', 'b'], [])
        self.d.r1 = a1
        self.d.r2 = a1
        self.assertEqual(self.d.r1, Relation(['a', 'b'], []))
        self.assertEqual(self.d.r1, a1)
        self.assertEqual(self.d.r1, self.d.r2)
        
        self.assertFalse(self.d.r1 is a1)
        self.assertFalse(self.d.r1 is self.d.r2)
        
    def test_with_1(self):
        self.d = Database(debug=True)
        a1 = Relation(['a', 'b'], [[10, 20]])
        b1 = Relation(['b', 'c'], [[20, 30]])
        #todo: perhaps we need 
        #  with a1 as a, b1 as b:
        #     r1=a1
        #     r2=b1
        #todo consider v1,v2 = a1,a2 as multiple-assignment instead?
        with self.d:
            self.d.r1 = a1
            self.d.r2 = b1
        self.assertEqual(self.d.r1, a1)

    def test_with_alternative(self):
        self.d = Database(debug=True)
        a1 = Relation(['a', 'b'], [[10, 20]])
        b1 = Relation(['b', 'c'], [[20, 30]])
        
        self.d.r1, self.d.r2 = a1, b1  #i.e. rhs calculated first, then assigment (semi)atomically
                                       #BUT constraints checked after each assignment = too early
        
        self.assertEqual(self.d.r1, a1)
        self.assertEqual(self.d.r2, b1)

    def test_with_2(self):
        self.d = Database(debug=True)
        a1 = Relation(['a', 'b'], [[10, 20]])
        b1 = Relation(['b', 'c'], [[20, 30]])
        
        with self.d:
            self.d.r1, self.d.r2 = a1, b1  #i.e. rhs calculated first, then assigment & constraints can be atomic
        
        self.assertEqual(self.d.r1, a1)
        self.assertEqual(self.d.r2, b1)

    def test_with_2_final_syntax(self):
        self.d = Database(debug=True)
        a1 = Relation(['a', 'b'], [[10, 20]])
        b1 = Relation(['b', 'c'], [[20, 30]])
        
        with self.d: self.d.r1, self.d.r2 = a1, b1  #i.e. rhs calculated first, then assigment & constraints can be atomic
        
        self.assertEqual(self.d.r1, a1)
        self.assertEqual(self.d.r2, b1)
        
    def test_with_3(self):
        self.d = Database(debug=True)
        a1 = Relation(['a', 'b'], [[10, 20]])
        b1 = Relation(['b', 'c'], [[20, 30]])
        
        with self.d: self.d.r1, self.d.r2 = a1, b1  #i.e. rhs calculated first, then assigment & constraints can be atomic
        with self.d: self.d.r1, self.d.r2 = self.d.r2, self.d.r1  #i.e. atomic swap
        
        self.assertEqual(self.d.r1, b1)
        self.assertEqual(self.d.r2, a1)
        
    def test_update_1(self):
        self.d = Database(debug=True)
        self.d.r1 = Relation(['a', 'b'], [(1,2), (3,4), (5,6)])
        self.d.r1 = Relation(['a', 'b'], [(1,2), (5,6)])
        self.assertEqual(self.d.r1, Relation(['a', 'b'], [(1,2), (5,6)]))

    def test_update_2(self):
        self.d = Database(debug=True)
        self.d.r1 = Relation(['a', 'b'], [(1,2), (3,4), (5,6)])
        self.d.r1 = Relation(['a', 'b', 'c'], [(1,2,3), (5,6,7)])
        self.assertEqual(self.d.r1, Relation(['a', 'b', 'c'], [(1,2,3), (5,6,7)]))

    def test_update_3(self):
        self.d = Database(debug=True)
        self.d.r1c = Relation(['a', 'b'], [(1,Relation(['c','d'], [(10,20),(30,40)]))])
        self.assertEqual(self.d.r1c, Relation(['a', 'b'], [(1, Relation(['d','c'], [(40,30), (20,10)]))]))

    def test_drop_1(self):
        self.d = Database(debug=True)
        self.d.r1 = Relation(['a', 'b'], [(1,2), (3,4), (5,6)])
        self.assertTrue({'name':'r1'} in self.d.catalog.relvars)
        del self.d.r1
        self.assertFalse({'name':'r1'} in self.d.catalog.relvars)

        
    def test_deferred_1(self):
        self.d = Database(debug=True)
        
        self.d.r1 = Relation(['a', 'b'], [(10, 20),
                                          (30, 40),
                                         ])
        self.d.r2 = Relation(['b', 'c'], [(20, 30),
                                          (30, 40),
                                         ])
        
        self.d.r3 = AND(self.d.r1, self.d.r2)
        
        answer = Relation(['a','b','c'], 
                          [(10,20,30)])
        
        self.assertEqual(self.d.r3,
                         answer)

        self.assertEqual(AND(self.d.r1, self.d.r2),
                         self.d.r3)

        #modify sources
        self.d.r2 = Relation(['b','c'],[{'c': 40, 'b': 30}])
        
        #deferred was persisted as it was at time of assignment
        self.assertEqual(self.d.r3,
                         answer)

    def test_insert_1(self):
        self.d = Database(debug=True)
        self.d.r1 = Relation(['a', 'b'], [(1,2), (3,4), (5,6)])
        self.d.r1 |= Relation(['a', 'b'], [(3,4), (7,8)])
        self.assertEqual(self.d.r1, Relation(['a', 'b'], [(1,2), (3, 4), (5,6), (7,8)]))
        
    def test_delete_1(self):
        self.d = Database(debug=True)
        self.d.r1 = Relation(['a', 'b'], [(1,2), (3,4), (5,6)])
        self.d.r1 -= Relation(['a', 'b'], [(3,4)])
        self.assertEqual(self.d.r1, Relation(['a', 'b'], [(1,2), (5,6)]))
        
    def test_update_1(self):
        self.d = Database(debug=True)
        self.d.r1 = Relation(['a', 'b'], [(10, 20),
                                          (20, 30),
                                          (30, 40),
                                         ])
        r2 = Relation(['a','b'], 
                      [(10, 20),
                       (20, 60),
                       (30, 80),
                      ])

        self.assertNotEqual(self.d.r1, r2)
        
        self.d.r1.update(lambda t:t.b>20, lambda u:{'b':u.OLD_b * 2})

        self.assertEqual(self.d.r1, r2)

    def test_constraint_1(self):
        self.d = Database(debug=True)
        self.d.r1 = Relation(['a', 'b'], [(10, 20),
                                          (20, 30),
                                          (30, 40),
                                         ])
        r2 = Relation(['a','b'], 
                      [(10, 20),
                       (20, 30),
                       (30, 40),
                      ])
        
        self.assertEqual(self.d.r1, r2)
        
        self.d.constraints = [lambda:COUNT(self.d.r1) == 3]
        try:
            self.d.r1 |= Relation(['a', 'b'], [(3,4), (7,8)])
        except Exception, e:
            pass  #todo assert raised!

        self.assertEqual(self.d.r1, r2)
        
        try:
            self.d.r1 = Relation(['a', 'b'], [(3,4), (7,8)])
        except Exception, e:
            pass  #todo assert raised!

        self.assertEqual(self.d.r1, r2)

    def test_constraint_2(self):
        self.d = Database(debug=True)
        self.d.r1 = Relation(['a', 'b'], [(10, 20),
                                          (20, 30),
                                          (30, 40),
                                         ])
        r2 = Relation(['a','b'], 
                      [(10, 20),
                       (20, 30),
                       (30, 40),
                      ])
        
        self.assertEqual(self.d.r1, r2)
        
        self.d.constraints = [lambda:COUNT(self.d.r1) == 3]
        try:
            self.d.r1 -= Relation(['a', 'b'], [(10,20)])
        except Exception, e:
            pass  #todo assert raised!

        self.assertEqual(self.d.r1, r2)

    def test_constraint_3(self):
        self.d = Database(debug=True)
        self.d.r1 = Relation(['a', 'b'], [(10, 20),
                                          (20, 30),
                                          (30, 40),
                                         ])
        r2 = Relation(['a','b'], 
                      [(10, 20),
                       (20, 30),
                       (30, 40),
                      ])

        r3 = Relation(['a','b'], 
                      [(10, 20),
                       (30, 40),
                      ])

        r4 = Relation(['a','b'], 
                      [(10, 20),
                       (30, 40),
                       (3,4),
                       (7,8),
                      ])
        
        self.assertEqual(self.d.r1, r2)
        
        self.d.constraints = [lambda:1 < COUNT(self.d.r1) < 5]
        self.d.r1 -= Relation(['a', 'b'], [(20,30)])  #should get no exception

        self.assertEqual(self.d.r1, r3)
        
        #todo: why is db cursor closed when we do this:
        #self.d.r1 |= Relation(['a', 'b'], [(3,4), (7,8)])  #should get no exception
        
        #self.assertEqual(self.d.r1, r4)
        
        
    #todo HERE: test_view
        
    #todo 
        
        
    #todo stress test!
        
if __name__ == '__main__':
    unittest.main()