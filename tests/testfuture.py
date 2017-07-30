"""Tests for experimenting with crazy, future language ideas
   (ignore syntax)
"""

import unittest

from dee import DEE, DUM
from dee.relation import Relation, View, InvalidAttributeName, AND, GENERATE, COMPOSE
from collections import namedtuple


# REMEMBER for view defs: degree of in = degree of out

# def while_gen(a,b,c,d,condition):
#     if condition:
#         yield {'condition':True, 'a':a, 'b':b, 'c':c, 'd':d}

def while_gen2(condition):
    if condition:
        yield {'condition':True}

# def while_gen3(condition):
#     #print "condition=",condition
#     if condition:
#         #yield DEE  #breaks! returns |+| (string?)
#         yield {'condition':True}
#     else:
#         pass  #OR raise StopIteration() OR nothing
#         #yield DUM  #breaks!

def loop_gen(start, end, i=None):
    if i is None:
        for i in range(start, end+1):
            yield {'start':start, 'end':end, 'i':i}
loop = View(loop_gen)

global_test = []
def print_gen(value):
    print "DEBUG:", value  # side-effect
    global_test.append(value)  # side-effect
    yield {'value':value}
debug = View(print_gen)


#todo: IO with files


class TestRelation(unittest.TestCase):
    def setUp(self):
        self.r1 = Relation(['a','b','c','d'],
                           {(10,20,20,30),
                            (10,20,30,40),
                            (30,40,20,30),
                            (30,40,30,40),
                           })

    def test_while_basic(self):
        while_view = View(while_gen2)

        rt = Relation(["condition"], {(True,)})
        rf = Relation(["condition"], {})

        self.assertEqual(while_view & Relation(["condition"], {(True,)}),
                         rt)

        self.assertEqual(while_view & Relation(["condition"], {(False,)}),
                         rf)  # todo DUM?

        self.assertEqual(while_view & GENERATE({"condition":False}),
                         rf)
        self.assertEqual(while_view & GENERATE({"condition":True}),
                         rt)

    def test_while(self):
        while_view = View(while_gen2)

        #print self.r1.extend(lambda t:{"condition":t.a==30})

        self.assertEqual(while_view & self.r1.extend(lambda t:{"condition":t.a==30}),
                         Relation(['a','b','c','d', 'condition'],
                                   {#(10,20,20,30),
                                    #(10,20,30,40),
                                    (30,40,20,30, True),
                                    (30,40,30,40, True),
                                   })
        )

        self.assertEqual((while_view & self.r1.extend(lambda t:{"condition":t.a==30})).remove(['condition']),
                         Relation(['a','b','c','d'],
                                   {#(10,20,20,30),
                                    #(10,20,30,40),
                                    (30,40,20,30),
                                    (30,40,30,40),
                                   })
        )

        #print r1.extend(lambda t:{"condition":True}).project(['condition'])
        #print while_view & r1.extend(lambda t:{"condition":True}).project(['a','condition'])
        #print r1.extend(lambda t:{"while":t.a==30}).where(while)

    def test_loop(self):
        #print loop & GENERATE({'start':1, 'end':5})

        self.assertEqual((loop & GENERATE({'start':1, 'end':5})
                         ).project(['i']),
                        Relation(['i'],
                                 {(1,),
                                  (2,),
                                  (3,),
                                  (4,),
                                  (5,),
                                 })
        )

        self.assertEqual((loop & GENERATE({'start':1, 'end':5})).extend(lambda t:{'extra':t.i*10}),
                        Relation(['start', 'end', 'i', 'extra'],
                                 {(1, 5, 1, 10),
                                  (1, 5, 2, 20),
                                  (1, 5, 3, 30),
                                  (1, 5, 4, 40),
                                  (1, 5, 5, 50),
                                 })
        )

    def test_debug(self):
        global global_test
        global_test = []

        COMPOSE(loop, GENERATE({'start':1, 'end':5})
                     ).rename({'i':'value'}) & debug

        self.assertEqual(global_test, [1, 2, 3, 4, 5])

    def test_debug2(self):
        global global_test
        global_test = []

        debug & self.r1.rename({'a':'value'}).project(['value'])

        self.assertEqual(global_test, [10, 30])


if __name__ == '__main__':
    unittest.main()