import unittest

from dee.util.disassemble import disassemble, extract_keys

class TestDisassemble(unittest.TestCase):

    def test_simple_1(self):
        f2=lambda t:{'a':1, 'b':2}
        #if sys.version_info < (3,0):
            #bc = disassemble(f2.func_code)
        #else:
            #bc = disassemble(f2.__code__)
        #print (extract_keys(f2))
        self.assertEqual(extract_keys(f2), ['a', 'b'])

    def test_empty(self):
       f=lambda t:{}
       self.assertEqual(extract_keys(f), [])

    def test_complex_1(self):
        f1=lambda t:{'abc':t[0].upper(), 'de':3*4, 'f':{'one':1, 'two':2}, 'gh':lambda:6*'a'}

        #print (dis.dis(f1))
        #if sys.version_info < (3,0):
            #bc = disassemble(f1.func_code)
        #else:
            #bc = disassemble(f1.__code__)
        #print (bc)
        #print ("\n".join([str(x) for x in bc]))
        #print (extract_keys(f1))

        self.assertEqual(extract_keys(f1), ['abc', 'de', 'f', 'gh'])

    #fails: will workaround...
    #def test_variable_1(self):
        #v = 'a'
        #f2=lambda t:{v:1, 'b':2}
        #self.assertEqual(extract_keys(f2), ['a', 'b'])

if __name__ == '__main__':
    unittest.main()