import unittest

from dee.database import Database, InvalidDatabaseItem
from dee.relation import Relation, RESTRICT, COUNT, GROUP, UNGROUP, IMAGE, SUM, InvalidOperation
from dee import DEE, DUM

class TestDate(unittest.TestCase):
    S = Relation(['SNO', 'SNAME',    'STATUS', 'CITY'],
                [('S1', 'Smith',    20,     'London'),
                 ('S2', 'Jones',    10,     'Paris'),
                 ('S3', 'Blake',    30,     'Paris'),
                 ('S4', 'Clark',    20,     'London'),
                 ('S5', 'Adams',    30,     'Athens'),
                ]
                )

    P = Relation(['PNO', 'PNAME',    'COLOR',    'WEIGHT', 'CITY'],
                [('P1', 'Nut',      'Red',      12,     'London'),
                 ('P2', 'Bolt',     'Green',    17,     'Paris'),
                 ('P3', 'Screw',    'Blue',     17,     'Rome'),
                 ('P4', 'Screw',    'Red',      14,     'London'),
                 ('P5', 'Cam',      'Blue',     12,     'Paris'),
                 ('P6', 'Cog',      'Red',      19,     'London'),
                ]
                )
    SP = Relation(['SNO', 'PNO', 'QTY'],
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
                 ]
                )
    
    def setUp(self):
        self.d = Database(debug=True)
        
        if 'S' not in self.d or self.d.S != self.S:
            print("(Re)adding S...")
            self.d.S = self.S

        if 'P' not in self.d or self.d.P != self.P:
            print("(Re)adding P...")
            self.d.P = self.P

        if 'SP' not in self.d or self.d.SP != self.SP:
            print("(Re)adding SP...")
            self.d.SP = self.SP
            
    def test_delete(self):
        db = Database(debug=True)
        db.P = RESTRICT(self.d.P, lambda t:t.COLOR=='Blue')
        self.assertEqual(COUNT(db.P), 2)

    #def test_extend(self):
        #self.assertEqual( SUM(S.extend(['XYZ'], lambda t:{'XYZ':(2*t.STATUS) + 1}), lambda t:t.XYZ),
                          #225,
                          #'SUM...')

        #self.assertEqual( ANY( S.extend(['TEST'], lambda t:{'TEST':t.STATUS > 20}), lambda t:t.TEST),
                          #True,
                          #'ANY...')

    def test_group1(self):
        r = GROUP(self.d.SP.remove(['QTY']), ['PNO'], 'PNO_REL')
        self.assertEqual(r,
                         Relation(['SNO', 'PNO_REL'],
                                  [('S1', Relation(['PNO'], [('P1',),('P2',),('P3',),('P4',),('P5',),('P6',)])),
                                   ('S2', Relation(['PNO'], [('P1',),('P2',),])),
                                   ('S3', Relation(['PNO'], [('P2',),])),
                                   ('S4', Relation(['PNO'], [('P2',),('P4',),('P5',),])),
                                  ]
                                 ),
                         )
        
    def test_ungroup1(self):
        self.assertEqual(UNGROUP(GROUP(self.d.SP.remove(['QTY']), ['PNO'], 'PNO_REL'), 'PNO_REL'),
                         self.d.SP.remove(['QTY']))
        
    def test_image1(self):
        self.assertEqual(self.d.S.where(lambda t: IMAGE(self.d.SP)(['PNO']) == self.d.P(['PNO'])),
                         Relation(['SNO', 'SNAME', 'STATUS', 'CITY'],
                                  [('S1', 'Smith', 20, 'London'),
                                  ])
                        )

    def test_image2(self):
        r = self.d.S.extend(lambda t: {'TOTQ':SUM(IMAGE(self.d.SP), lambda u:u.QTY)})
        self.assertEqual(r,
                         Relation(['SNO', 'SNAME', 'STATUS', 'CITY', 'TOTQ'],
                                 [('S1', 'Smith',    20,     'London', 1300),
                                  ('S2', 'Jones',    10,     'Paris',   700),
                                  ('S3', 'Blake',    30,     'Paris',   200),
                                  ('S4', 'Clark',    20,     'London',  900),
                                  ('S5', 'Adams',    30,     'Athens',    0),
                                 ])
                        )

    def test_image_idea_where1(self):
        #was trying to and extension with DEE to get filter without where...
        self.assertEqual(self.d.S.where(lambda t: IMAGE(DEE)),
                         self.d.S
                        )

    def test_image_idea_where2(self):
        self.assertEqual(self.d.S.where(lambda t: IMAGE(DUM)),
                         self.d.S & DUM
                        )
        
    def test_image_bad1(self):
        self.assertRaises(InvalidOperation, IMAGE, self.d.SP)
        
    def test_extend_image2(self):
        #Note: error in Database Explorations Page 260: projected onto S#, TOTQ[D] in book
        self.assertEqual(self.d.S.extend(lambda t: {'TOTQD':SUM(IMAGE(self.d.SP)(['QTY']))}),
                         Relation(['SNO', 'SNAME', 'STATUS', 'CITY', 'TOTQD'],
                                 [('S1', 'Smith',    20,     'London', 1000),
                                  ('S2', 'Jones',    10,     'Paris',   700),
                                  ('S3', 'Blake',    30,     'Paris',   200),
                                  ('S4', 'Clark',    20,     'London',  900),
                                  ('S5', 'Adams',    30,     'Athens',    0),
                                 ])
                         )

    def test_extend_image3(self):
        #equivalent image and group expressions: from Database Explorations Chapter 14 Example 16
        self.assertEqual(self.d.SP(['SNO']).extend(lambda t: {'PQ':IMAGE(self.d.SP)}),
                         GROUP(self.d.SP, ['PNO', 'QTY'], 'PQ')
                        )

    def test_extend_image4(self):
        #idempotency of image based on Database Explorations Chapter 14 Example 17
        self.assertEqual(self.d.S.where(lambda tt:IMAGE(IMAGE(self.d.SP))),
                         self.d.S.where(lambda tt:IMAGE(self.d.SP))
                        )

    def test_extend_image5(self):
        #image count with dee from Database Explorations Chapter 14 Example 18
        self.assertEqual(DEE.extend(lambda t:{'NSP':COUNT(self.d.SP)}),
                         DEE.extend(lambda t:{'NSP':COUNT(IMAGE(self.d.SP))})
                        )
        
    def test_extend_image6(self):
        #image count with dee from Database Explorations Chapter 14 Example 18
        self.assertEqual(DEE.extend(lambda t:{'NSP':COUNT(self.d.SP)}),
                         Relation.from_tuple({'NSP':COUNT(self.d.SP)})
                        )
        
        
if __name__ == '__main__':
    unittest.main()