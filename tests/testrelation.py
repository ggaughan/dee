import unittest

from dee import DEE, DUM
from dee.relation import Relation, View, InvalidAttributeName, AND, REMOVE, OR, EXTEND, \
                         COMPOSE, RESTRICT, WRAP, UNWRAP, GENERATE, InvalidOperation
from collections import namedtuple
import math

class TestRelation(unittest.TestCase):
    
    def test_construction_dum(self):
        r = Relation([], 
                     [])
        self.assertTrue(r.heading == set())
        self.assertEqual(r, DUM)
        self.assertNotEqual(r, DEE)
        self.assertFalse(r.is_deferred)  #note: implementation detail - need not be
        self.assertFalse(isinstance(r, View))
        
    def test_construction_dee(self):
        r = Relation([], 
                     [{}])
        self.assertTrue(r.heading == set())
        self.assertTrue(r == DEE)
        self.assertFalse(r == DUM)
        
    def test_construction_invalid_attrname(self):
        self.assertRaises(InvalidAttributeName, Relation, ['def'], [])
        
    def test_construction_invalid_attrnames(self):
        self.assertRaises(InvalidAttributeName, Relation, ['a','b','a'], [])
        
    def test_construction_degree_1(self):
        r = Relation(['a'], [])
        self.assertTrue(r.heading == set('a'))

    def test_construction_degree_1_nobody(self):
        r = Relation(['a'])
        self.assertTrue(r.heading == set('a'))
        
    def test_construction_degree_1_str(self):
        r = Relation(['a'], [])
        self.assertTrue(str(r) == """+---+\n| a |\n+---+\n+---+""")
        
    def test_construction_degree_2(self):
        r = Relation(['a', 'b'], [])
        self.assertTrue(r.heading == set(['a', 'b']))
        self.assertTrue(r.heading == set(['b', 'a']))
        
    def test_construction_degree_2b(self):
        r = Relation(['b', 'a'], [])
        self.assertTrue(r.heading == set(['a', 'b']))
        self.assertTrue(r.heading == set(['b', 'a']))

    def test_construction_degree_3(self):
        r = Relation('b a', [])
        self.assertTrue(r.heading == set(['a', 'b']))
        self.assertTrue(r.heading == set(['b', 'a']))
        
    def test_construction_degree_3b(self):
        r = Relation('b,a', [])
        self.assertTrue(r.heading == set(['a', 'b']))
        self.assertTrue(r.heading == set(['b', 'a']))
        
    def test_construction_degree_3c(self):
        r = Relation('b, a', [])
        self.assertTrue(r.heading == set(['a', 'b']))
        self.assertTrue(r.heading == set(['b', 'a']))

    def test_ordered_heading_1(self):
        r1 = Relation(['a','b','c','d'], 
                      [(10,20,20,30),
                       (10,20,30,40),
                       (30,40,20,30),
                       (30,40,30,40),
                      ])
        r2 = AND(r1, DEE)
        self.assertEqual(r1.heading, r2.heading)
        
    def test_tuple_1(self):
        r = Relation(['a', 'b'], 
                     [(10, 20)])
        self.assertTrue(r.heading == r._body[0].heading)  #note: inner access
        self.assertFalse(r.is_deferred)  #note: implementation detail - need not be
        self.assertFalse(isinstance(r, View))
        
    def test_contains_1(self):
        r = Relation(['a', 'b'], 
                     [(10, 20)])
        nt = namedtuple('tmp', ['a', 'b'])
        self.assertTrue(nt(a=10, b=20) in r)
        
    def test_contains_2(self):
        r = Relation(['a', 'b'],
                     [(10, 20)])
        nt = namedtuple('tmp', ['a', 'b'])
        self.assertFalse(nt(a=11, b=20) in r)

    def test_contains_3(self):
        r = Relation(['a', 'b'], 
                     [(10, 20)])
        nt = namedtuple('tmp', ['a', 'b'])
        self.assertTrue(nt(a=10, b=21) not in r)

    def test_contains_4(self):
        r = Relation(['a', 'b'], 
                     [(10, 20)])
        t = r.Tuple(a=10, b=20)
        self.assertTrue(t in r)

    def test_and(self):
        r1=Relation(['a', 'b'],
                    [(1,2),
                     (3,4)
                    ])

        r2=Relation(['b', 'c'], 
                    [(2,5), 
                     (6,7)
                    ])

        r3=Relation(['d', 'e'], 
                    [(8,9), 
                     (10,11)
                    ])

        r4=Relation(['a', 'b'], 
                    [(3,4), 
                     (12,13), 
                     (14,15)
                    ])  #Note 3,9 should be 3,4 or cause & intersect error below

        self.assertEqual(r1, Relation(['b', 'a'],
                                      [(2,1), 
                                       (4,3)
                                      ]),
                             'eq (orderless)')

        self.assertNotEqual(r1, Relation(['a', 'b'],
                                         [(2,1), 
                                          (4,3)
                                         ]) , '!eq (orderless)')

        self.assertNotEqual(r1, Relation(['b', 'a'],
                                         [(2,1), 
                                          (4,3), 
                                          (9,9)
                                         ]) , '!eq (orderless) extra')


        self.assertEqual(r1 & r2, Relation(['a', 'c', 'b'],
                                           [(1,5,2)
                                           ]) , '& natural join')

        self.assertEqual(r1 & r2, Relation(['a', 'b', 'c'],
                                           [(1,2,5)]), '& natural join (orderless)')


        self.assertEqual(r1 & r3, Relation(['a', 'b', 'd', 'e'],
                                           [(1,2,8,9), 
                                            (1,2,10,11), 
                                            (3,4,8,9), 
                                            (3,4,10,11)
                                           ]) , '& cartesian/cross join')

        self.assertEqual(r1 & r4, Relation(['a', 'b'],
                                           [(3,4)]) , '& intersect')

        self.assertEqual(r1 & r4, Relation(['a', 'b'],
                                           [(3,4)]) , '& intersect (de-duped)')

        self.assertEqual(r1 | r4, Relation(['a', 'b'],
                                           [(1,2), 
                                            (3,4), 
                                            (12,13), 
                                            (14,15)
                                           ]) , '| union')

        #self.assertRaises(Exception , r1 | r3, '| general')
        
        #self.assertTrue((r1 & r2).is_deferred)  #note: implementation detail - need not be
        #self.assertFalse((r1 & r2).is_deferred)  #note: implementation detail - need not be
        self.assertFalse(isinstance((r1 & r2), View))
        
    def test_and_1(self):
        r1 = Relation(['a', 'b'], [(10, 20),
                                   (30, 40),
                                  ])
        r2 = Relation(['b', 'c'], [(20, 30),
                                   (30, 40),
                                  ])
        r3 = Relation(['a','b','c'], 
                      [(10,20,30)])
        self.assertEqual(AND(r1, r2),
                         r3)

    def test_and_2(self):
        r1 = Relation(['a', 'b'], 
                      [(10, 20),
                       (30, 40),
                      ])
        r2 = Relation(['c', 'd'], 
                      [(20, 30),
                       (30, 40),
                      ])
        r3 = Relation(['a','b','c','d'], 
                      [(10,20,20,30),
                       (10,20,30,40),
                       (30,40,20,30),
                       (30,40,30,40),
                      ])
        self.assertEqual(AND(r1, r2),
                         r3)

    def test_and_3(self):
        r1 = Relation(['a','b','c','d'],
                      [(10,20,20,30),
                       (10,20,30,40),
                       (30,40,20,30),
                       (30,40,30,40),
                      ])
        self.assertEqual(AND(r1, DEE),
                         r1)

    def test_and_4(self):
        r1 = Relation(['a','b','c','d'], 
                      [(10,20,20,30),
                       (10,20,30,40),
                       (30,40,20,30),
                       (30,40,30,40),
                      ])
        self.assertEqual(AND(r1, DUM),
                         Relation(['a','b','c','d']))
        
    def test_and_5(self):
        r1 = Relation(['a', 'b'], [(10, 20),
                                   (30, 40),
                                  ])
        r2 = Relation(['a', 'b'], [(10, 20),
                                   (30, 40),
                                  ])
        r3 = Relation(['a','b'], 
                      [(10,20),
                       (30,40)])
        self.assertEqual(AND(r1, r2),
                         r3)

    def test_and_6(self):
        """Cross join"""
        r1 = Relation(['a', 'b'], [(10, 20),
                                   (30, 40),
                                  ])
        r2 = Relation(['c', 'd'], [(50, 60),
                                  ])
        r3 = Relation(['a','b', 'c', 'd'], 
                      [(10,20,50,60),
                       (30,40,50,60)])
        self.assertEqual(AND(r1, r2),
                         r3)
        
    def test_minus_1(self):
        A=Relation(['a', 'b'],
                   [(1, Relation(['c'], 
                                 [(100,)])),
                    (2, Relation(['c'], 
                                 [(200,)])),
                   ])

        B=Relation(['a', 'b'],
                   [(1, Relation(['c'], 
                                 [(100,)])),
                    (2, Relation(['c'], 
                                 [(200,)])),
                   ])

        self.assertEqual(A(['a']) - B(['a']), 
                         Relation(['a'], 
                                  []), "Compare simple 1")
        self.assertEqual(A - B, 
                         Relation(['a', 'b'], 
                                  []), "Compare complex 1")       
               
    def test_remove_1(self):
        r1 = Relation(['a','b','c','d'], 
                      [(10,20,20,30),
                       (10,20,30,40),
                       (30,40,20,30),
                       (30,40,30,40),
                      ])
        self.assertEqual(REMOVE(r1, ['a','b','d']),
                         Relation(['c'], 
                                  [(20,), 
                                   (30,)
                                  ]))
        
    def test_remove_2(self):
        r1=Relation(['a', 'b', 'c'],
                    [(1,2,8),
                     (3,4,9),
                     (5,7,8)
                    ])

        self.assertEqual(REMOVE(r1, ['c','b']), 
                         Relation(['a'],
                                  [(1,),
                                   (3,),
                                   (5,)
                                  ]), 
                         'remove')

        self.assertEqual(r1.project(['a']),
                         Relation(['a'],
                                  [(1,),
                                   (3,),
                                   (5,)
                                  ]),
                         'remove project')
       
    def test_or_1(self):
        r1 = Relation(['a', 'b'], 
                      [(10, 20),
                       (30, 40),
                      ])
        r2 = Relation(['b', 'a'], 
                      [(30, 20),
                       (40, 30),
                      ])
        r3 = Relation(['a','b'],
                      [(10,20),
                       (30,40),
                       (20,30),
                      ])
        self.assertEqual(OR(r1, r2),
                         r3)
        
    def test_compose_1(self):
        r1 = Relation(['a', 'b'],
                      [(10, 20),
                       (30, 40),
                      ])
        r2 = Relation(['b', 'c'], 
                      [(20, 30),
                       (30, 40),
                      ])
        r3 = Relation(['a','c'], 
                      [(10,30)])
        self.assertEqual(COMPOSE(r1, r2),
                         r3)


    def test_extend_1(self):
        r1 = Relation(['a', 'b'], 
                      [(10, 20),
                       (30, 40),
                      ])
        r2 = Relation(['a','b','c','d'],
                      [(10,20,20,40),
                       (30,40,60,80),
                      ])
        self.assertEqual(EXTEND(r1, lambda t:{'c':t.a*2, 'd':t.b*2}),
                         r2)

    def test_extend_2(self):
        r1 = Relation(['a', 'b'], 
                      [(10, 20),
                       (30, 40),
                      ])
        r2 = Relation(['a','b','c','d'],
                      [(10,20,20,40),
                       (30,40,60,80),
                      ])
        #todo removed: dynamic names not allowed (yet): col1 = 'c'
        r = EXTEND(r1, lambda t:{'c':t.a*2, 'd':t.b*2})
        self.assertEqual(r,
                         r2)
        
    def test_restrict_1(self):
        r4=Relation(['a', 'b'], 
                    [(3,4), 
                     (12,13), 
                     (14,15)
                    ])

        self.assertEqual(r4 & Relation(['a'],
                                       [(12,)]), 
                         Relation(['a','b'],
                                  [(12,13)]))

        self.assertEqual(RESTRICT(r4, lambda t:t.a==12), 
                         Relation(['a','b'],
                                  [(12,13)]))
        
    def test_where(self):
        r4=Relation(['a', 'b'],
                    [(3,4), 
                     (12,13), 
                     (14,15)
                    ])

        self.assertEqual(r4 & Relation(['a'],
                                       [(12,)]), 
                         Relation(['a','b'],
                                  [(12,13)]))

        self.assertEqual(r4.where(lambda t:t.a==12), 
                         Relation(['a','b'],
                                  [(12,13)]))
        

    def test_rename_1(self):
        r1=Relation(['a', 'b', 'c'],
                    [(1,2,8),
                     (3,4,9),
                     (5,7,8)
                    ])

        self.assertEqual(r1.rename({'c':'sea','b':'bee'}), 
                         Relation(['a', 'bee', 'sea'],
                                  [(1,2,8),
                                   (3,4,9),
                                   (5,7,8)
                                  ]))

    def test_extend_1(self):
        r1=Relation(['a', 'b', 'c'],
                    [(1,2,8),
                     (3,4,9),
                     (5,7,8)
                    ])

        self.assertEqual(r1.extend(lambda t:{'d':t.a + t.b})(['d']), 
                         Relation(['d'],
                                  [(3,),
                                   (7,),
                                   (12,)
                                  ]))

    def test_wrap_1(self):
        r1=Relation(['a', 'b', 'c'],
                    [(1,2,8),
                     (3,4,9),
                     (5,7,8)
                    ])

        r = WRAP(r1, ['b','c'], 'wrapped')
        self.assertEqual(r,
                         Relation(['a', 'wrapped'],
                                  [(1, (2,8)),
                                   (3, (4,9)),
                                   (5, (7,8))
                                  ]))

        r = WRAP(r1, ['c','b'], 'wrapped')
        self.assertEqual(r,
                         Relation(['a', 'wrapped'],
                                  [(1, (8,2)),
                                   (3, (9,4)),
                                   (5, (8,7))
                                  ]))
        
        #todo fix, e.g. WRAP (etc.?) to return {} or as_tuple to recurse to normalise WRAPpings
        #nt = namedtuple('nt', 'b c')
        #self.assertEqual(r,
                         #Relation(['a', 'wrapped'],
                                  #[(1, nt(b=2, c=8)),
                                   #(3, nt(b=4, c=9)),
                                   #(5, nt(b=7, c=8))
                                  #]))

    def test_unwrap_1(self):
        r1=Relation(['a', 'b', 'c'],
                    [(1,2,8),
                     (3,4,9),
                     (5,7,8)
                    ])

        self.assertEqual(UNWRAP(WRAP(r1, ['b','c'], 'wrapped'), 'wrapped'), 
                         r1)
        

    def test_view_1(self):
        r1 = Relation(['a','b','c','d'],
                      [(10,20,20,40),
                       (30,40,60,80),
                      ])

        r2 = r1.extend(lambda t: {"e": t.a * 2})
        r2view = View(#['a','b','c','d','e'], 
                          lambda: r1.extend(lambda t: {"e": t.a * 2})
                         )
        
        r3 = Relation(['a','b','c','d','e'],
                      [(10,20,20,40,20),
                       (30,40,60,80,60),
                      ])

        r3b = Relation(['a','b','c','d','e'],
                      [(30,40,60,80,60),
                      ])
        
        self.assertEqual(r2view, r3)
        self.assertEqual(r2view, r2)

        r1 = Relation(['a','b','c','d'],
                      [(30,40,60,80),
                      ])
        
        #view takes on latest source data
        self.assertEqual(r2view, r3b)
        self.assertNotEqual(r2view, r2)
        
    def test_view_2(self):
        r1 = Relation(['a', 'b'], [(10, 20),
                                   (30, 40),
                                  ])
        r2 = Relation(['b', 'c'], [(20, 30),
                                   (30, 40),
                                  ])
        r3 = Relation(['a','b','c'], 
                      [(10,20,30)])

        r = r1 & r2
        rview = View(#['a','b','c'],
                         lambda: r1 & r2)

        self.assertEqual(r, r3)
        self.assertEqual(rview, r3)
        self.assertEqual(rview, r)
        
    def test_view_3(self):
        r1 = Relation(['a', 'b'], [(10, 20),
                                   (30, 40),
                                  ])
        r2 = Relation(['b', 'c'], [(20, 30),
                                   (30, 40),
                                  ])
        r3 = Relation(['a','b','c'], 
                      [(10,20,30)])

        rview = View(#r1.heading | r2.heading,
                         lambda: r1 & r2)
        
        self.assertEqual(rview, r3)
        
        
    def test_insert_1(self):
        r1 = Relation(['a', 'b'], [(10, 20),
                                   (30, 40),
                                  ])
        r2 = Relation(['a', 'b'], [(20, 30),
                                   (30, 40),
                                  ])
        r3 = Relation(['a','b'], 
                      [(10,20),
                       (20,30),
                       (30,40),
                       ])

        self.assertNotEqual(r1, r3)
        
        r1 |= r2

        self.assertEqual(r1, r3)
        
    def test_delete_1(self):
        r1 = Relation(['a', 'b'], [(10, 20),
                                   (30, 40),
                                  ])
        r2 = Relation(['a', 'b'], [(20, 30),
                                   (30, 40),
                                  ])
        r3 = Relation(['a','b'], 
                      [(10,20),
                       ])

        self.assertNotEqual(r1, r3)
        
        r1 -= r2

        self.assertEqual(r1, r3)

    def test_update_1(self):
        r1 = Relation(['a', 'b'], [(10, 20),
                                   (20, 30),
                                   (30, 40),
                                  ])
        r2 = Relation(['a','b'], 
                      [(10, 20),
                       (20, 60),
                       (30, 80),
                      ])

        self.assertNotEqual(r1, r2)
        
        r1.update(lambda t:t.b>20, lambda u:{'b':u.OLD_b * 2})

        self.assertEqual(r1, r2)

    def test_generate1(self):
        r1 = GENERATE({'pi':3.14})
        
        r2 = Relation(['pi'], [(3.14,)])
        
        self.assertEqual(r1, r2)
        
    def test_generate1(self):
        r1 = GENERATE({'pi':3.14})
        
        r2 = Relation(['pi'], [(3.14,)])
        
        self.assertEqual(r1, r2)
        
    def test_gen_relation1(self):
        r1 = View(plus_gen)
        r2 = Relation(['x', 'y', 'z'],
                      [(2, 3, 5),]
                     )

        self.assertEqual(r1 & GENERATE({'x':2, 'y':3}), r2)

    def test_gen_relation2(self):
        r1 = View(plus_gen)
        r2 = Relation(['x', 'y', 'z'],
                      [(2, 3, 5),]
                     )

        self.assertEqual(r1 & GENERATE({'x':2, 'z':5}), r2)

    def test_gen_relation3(self):
        r1 = View(plus_gen)
        r2 = Relation(['x', 'y', 'z'],
                      [(2, 3, 5),]
                     )

        self.assertEqual(r1 & GENERATE({'x':2, 'y':3, 'z':5}), r2)

    def test_gen_relation4(self):
        r1 = View(plus_gen)
        r2 = Relation(['x', 'y', 'z'],
                      [(2, 3, 5),]
                     )

        self.assertNotEqual(r1 & GENERATE({'x':2, 'y':4, 'z':5}), r2)

    def test_gen_relation5(self):
        """From Writings 1989-1991 p90"""
        r1 = Relation(['n'],
                      [(1,),
                       (0,),
                       (4,),
                       (9,),
                       ]
                     )
        
        r2 = Relation(['n', 'sqrt_n'],
                      [(1, Relation(['pr', 'nr'], [(1, -1)])),
                       (0, Relation(['pr', 'nr'], [(0, 0)])),
                       (4, Relation(['pr', 'nr'], [(2, -2)])),
                       (9, Relation(['pr', 'nr'], [(3, -3)])),
                      ]
                     )

        self.assertEqual(r1.extend(lambda t:{'sqrt_n':GENERATE({'pr':math.sqrt(t.n), 'nr':-math.sqrt(t.n)})}), r2)

    def test_pipeline_1(self):
        """Check pipelining snapshots at definition-time"""
        r1 = Relation(['a', 'b'], [(10, 20),
                                   (30, 40),
                                  ])
        r2 = Relation(['c', 'd'], [(50, 60),
                                  ])
        r3 = Relation(['a','b', 'c', 'd'], 
                      [(10,20,50,60),
                       (30,40,50,60)])
        
        r = AND(r1, r2)
        
        self.assertEqual(r,
                         r3)
        
        #note: insert via assignment
        r2 = r2 | GENERATE({'c':70, 'd':80})

        self.assertEqual(r,
                         r3)

    def test_pipeline_2(self):
        """Check pipelining snapshots at definition-time"""
        r1 = Relation(['a', 'b'], [(10, 20),
                                   (30, 40),
                                  ])
        r2 = Relation(['c', 'd'], [(50, 60),
                                  ])
        r3 = Relation(['a','b', 'c', 'd'], 
                      [(10,20,50,60),
                       (30,40,50,60)])
        
        r = AND(r1, r2)
        
        self.assertEqual(r,
                         r3)
        
        #note: insert in-place (so no re-assignment, so no snapshot = very bad)
        r2.insert({'c':70, 'd':80})
        
        r2 = r2 | GENERATE({'c':70, 'd':80})

        self.assertEqual(r,
                         r3)
        
        
    #todo plus_gen with no params...
       
    #todo test repr
        
       
def plus_gen(x=None, y=None, z=None):
    "Plus generator yielding tuples. Could just as well be called minus_gen"
    if x is not None and y is not None and z is None:
        yield {'x':x, 'y':y, 'z':x + y}
    elif x is not None and y is None and z is not None:
        yield {'x':x, 'y':z - x, 'z':z}
    elif x is None and y is not None and z is not None:
        yield {'x':z - y, 'y':y, 'z':z}
    elif x is not None and y is not None and z is not None:
        if x + y == z:
            yield {'x':x, 'y':y, 'z':z}  #i.e. True   #note: could just as well return {} since we'll be joining
        #else yield nothing, i.e. False
    else:
        #Note: we could go further and return tuples given just one attribute
        #      or indeed we could start yielding infinite combinations if no attributes are passed
        #      (but then non-relational?)
        raise InvalidOperation("Infinite rows") #no pair of x,y or z
        
if __name__ == '__main__':
    unittest.main()