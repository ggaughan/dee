"""Initialise Sample Darwen database"""

from dee.database import Database
from dee.relation import Relation, View, GENERATE

db = Database(debug=True, persistence_location='darwen.db')

db.IS_CALLED = Relation(['StudentId', 'Name'],
                                 [('S1', 'Anne'),
                                  ('S2', 'Boris'),
                                  ('S3', 'Cindy'),
                                  ('S4', 'Devinder'),
                                  ('S5', 'Boris'),
                                 ]
                                )

db.IS_ENROLLED_ON = Relation(['StudentId', 'CourseId'],
                                     [('S1', 'C1'),
                                      ('S1', 'C2'),
                                      ('S2', 'C1'),
                                      ('S3', 'C3'),
                                      ('S4', 'C1'),
                                     ]
                                    )

db.COURSE = Relation(['CourseId', 'Title'],
                              [('C1', 'Database'),
                               ('C2', 'HCI'),
                               ('C3', 'Op Systems'),
                               ('C4', 'Programming'),
                              ]
                             )

db.EXAM_MARK = Relation(['StudentId', 'CourseId', 'Mark'],
                                 [('S1', 'C1', 85),
                                  ('S1', 'C2', 49),
                                  ('S2', 'C1', 49),
                                  ('S3', 'C3', 66),
                                  ('S4', 'C1', 93),
                                 ]
                                )

def vC_ER(self=None):
    return db.COURSE.extend(lambda t:{'Exam_Result':
                                      (db.EXAM_MARK & GENERATE({'CourseId':t.CourseId}))(['StudentId', 'Mark'])}
                             )(['CourseId', 'Exam_Result']) #fixed

db.C_ER = View(lambda: db.COURSE.extend(lambda t:{'Exam_Result':
                                      (db.EXAM_MARK & GENERATE({'CourseId':t.CourseId}))(['StudentId', 'Mark'])}
                             )(['CourseId', 'Exam_Result'])
              )
#todo {'pk':(Key,['CourseId'])}


#print db.C_ER
