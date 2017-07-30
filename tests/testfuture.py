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
import sys
import contextlib

@contextlib.contextmanager
def smart_open(filename=None, mode='r'):
    if filename and filename != '-':
        fh = open(filename, mode)
    else:
        if mode == 'r':
            fh = sys.stdin
        else:
            # todo assert mode=='w'
            fh = sys.stdout
    try:
        yield fh
    finally:
        if fh is not sys.stdout and fh is not sys.stdin:
            fh.close()

#     outdatas * file * myfilenames   #each outdatas needs filename col! (simple to extend('dummy') for single file: no need: cross join degeneration should do fine!!!)
def file_gen(filename="testfile.dat", data=None):
    # todo pass 'type' parameter, e.g. csv, binary, lines etc.?
    # todo pass size parameters, e.g. amount to read or write

    if data is None:
        with smart_open(filename) as f:
            #for r in f.readlines():
            #    yield {'filename':filename, 'data':r}
            yield {'filename':filename, 'data':Relation(['i', 'line'],
                                                [(i, r,) for i, r in enumerate(f.readlines())]
                                                )}
    else:
        with smart_open(filename, 'w') as f:
            for r in data.to_tuple_list(key=lambda t:t.i):
                f.write(r.line + '\n')
            # todo: return success?
            yield {'filename':filename, 'data':data}



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

        def test(t):
            """Test function def referenced by extend in loop"""
            x = t.i * 100
            return "result is {}".format(x)

        self.assertEqual((loop & GENERATE({'start':1, 'end':5})).extend(lambda t:{'extra':test(t)}),
                        Relation(['start', 'end', 'i', 'extra'],
                                 {(1, 5, 1, "result is 100"),
                                  (1, 5, 2, "result is 200"),
                                  (1, 5, 3, "result is 300"),
                                  (1, 5, 4, "result is 400"),
                                  (1, 5, 5, "result is 500"),
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

    def test_file_read(self):
        file_view = View(file_gen)

        print file_view & GENERATE({'filename':'tests/testfile.dat'})

        #todo test with multiple files

    # todo need a way to test with stdin + ctrl+d
    # def test_file_read2(self):
    #     file_view = View(file_gen)

    #     print file_view & GENERATE({#'filename':'tests/testfile.dat'
    #                               })

    #     #todo test with multiple files

    def test_file_write(self):
        file_view = View(file_gen)

        print file_view & GENERATE({'filename':'tests/testfile.dat',
                                    'data':Relation(['i','line'],
                                                    [{'i':0, 'line':'This is a new file'},
                                                     {'i':1, 'line':' with multiple lines'},
                                                    ])
                                   })

        #todo test with multiple files

    def test_file_write2(self):
        file_view = View(file_gen)

        print file_view & GENERATE({#'filename':'tests/testfile.dat',
                                    'data':Relation(['i','line'],
                                                    [{'i':0, 'line':'This is a new file'},
                                                     {'i':1, 'line':' with multiple lines'},
                                                    ])
                                   })

        #todo test with multiple files


if __name__ == '__main__':
    unittest.main()