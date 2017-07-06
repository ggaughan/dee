"""Given a lambda that returns a dictionary, extract the keys
"""

import dis
import sys

from opcode import *

def disassemble(co, lasti=-1):
    """Disassemble a code object."""
    #tweaked code from Python dis module
    code = co.co_code
    labels = []
    linestarts = {}
    n = len(code)
    i = 0
    extended_arg = 0
    free = None
    bytecodes = []
    while i < n:
        c = code[i]
        if sys.version_info < (3,0):
            op = ord(c)
        else:
            op = c
        bytecode = [op]
        i = i+1
        if op >= HAVE_ARGUMENT:
            if sys.version_info < (3,0):
                oparg = ord(code[i]) + ord(code[i+1])*256 + extended_arg
            else:
                oparg = (code[i]) + (code[i+1])*256 + extended_arg
            extended_arg = 0
            i = i+2
            if op == EXTENDED_ARG:
                extended_arg = oparg*65536#L
            bytecode.append(repr(oparg))
            if op in hasconst:
                bytecode.append(repr(co.co_consts[oparg]))
            elif op in hasname:
                bytecode.append(co.co_names[oparg])
            elif op in hasjrel:
                bytecode.append(repr(i + oparg))
            elif op in haslocal:
                bytecode.append(co.co_varnames[oparg])
            elif op in hascompare:
                bytecode.append(cmp_op[oparg])
            elif op in hasfree:
                if free is None:
                    free = co.co_cellvars + co.co_freevars
                bytecode.append(free[oparg])
        bytecodes.append(bytecode)
    return bytecodes

def extract_keys(co):
    """Extract keys from bytecodes (from lambda)
       Note: version specific
       Note: 2.4 needs #BUILD_MAP: DUP_TOP + STORE_SUBSCR  - any DUP_TOP <BUILD_MAP ..D/S> STORE_SUBSCR  ... is a new nest
    """
    if sys.version_info < (3,0):
        bytecodes = disassemble(co.func_code)
    else:
        bytecodes = disassemble(co.__code__)
    
    if sys.version_info < (2,6):
        keys = []
        nkeys = []
        if bytecodes:
            bc = bytecodes[0]
            #todo assert opname[bc[0]] == 'BUILD_MAP':
            lastbc = None
            duptop = 0
            for bc in bytecodes:
                if opname[bc[0]] == 'BUILD_MAP':
                    nkeys.append(0)
                    
                if opname[bc[0]] == 'DUP_TOP':
                    n = nkeys.pop()
                    nkeys.append(1)
                    
                if opname[bc[0]] == 'STORE_SUBSCR':
                    n = nkeys.pop()
                    if len(nkeys) == 0 or (len(nkeys) == 1 and n == 0):
                        keys.append(lastbc[2].strip("'"))  #todo assert LOAD_CONST
                    if n > 0:
                        nkeys.append(0)
    
                lastbc = bc
                        
            assert nkeys == [0]  #todo remove
            #todo assert ends with RETURN_VALUE
            return keys
        return None
    
    #2.6+
    elif sys.version_info >= (2,6):
        keys = []
        nkeys = []
        if bytecodes:
            bc = bytecodes[0]
            #todo assert opname[bc[0]] == 'BUILD_MAP':
            lastbc = None
            for bc in bytecodes:
                if opname[bc[0]] == 'BUILD_MAP':
                    nkeys.append(int(bc[1]))
    
                if opname[bc[0]] == 'STORE_MAP':
                    n = nkeys.pop()-1
                    if len(nkeys) == 0:
                        keys.append(lastbc[2].strip("'"))  #todo assert LOAD_CONST
                    if n > 0:
                        nkeys.append(n)
                        
                lastbc = bc
                        
            assert nkeys == [] or nkeys == [0] #todo remove
            #todo assert ends with RETURN_VALUE
            return keys
        return None
