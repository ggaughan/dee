from Dee import Relation, Key, Tuple, QUOTA, MAX, MIN, IS_EMPTY, COUNT, GENERATE
from DeeDatabase import Database

class darwen_Database(Database):
    def __init__(self, name):
        """Define initial relvars and their initial values here
           (Called once on database creation)"""
        Database.__init__(self, name)

        if 'IS_CALLED' not in self:
            print "Adding IS_CALLED..."
            self.IS_CALLED = Relation(['StudentId', 'Name'],
                                 [('S1', 'Anne'),
                                  ('S2', 'Boris'),
                                  ('S3', 'Cindy'),
                                  ('S4', 'Devinder'),
                                  ('S5', 'Boris'),
                                 ]
                                )

        if 'IS_ENROLLED_ON' not in self:
            print "Adding IS_ENROLLED_ON..."
            self.IS_ENROLLED_ON = Relation(['StudentId', 'CourseId'],
                                     [('S1', 'C1'),
                                      ('S1', 'C2'),
                                      ('S2', 'C1'),
                                      ('S3', 'C3'),
                                      ('S4', 'C1'),
                                     ]
                                    )

        if 'COURSE' not in self:
            print "Adding COURSE..."
            self.COURSE = Relation(['CourseId', 'Title'],
                              [('C1', 'Database'),
                               ('C2', 'HCI'),
                               ('C3', 'Op Systems'),
                               ('C4', 'Programming'),
                              ]
                             )

        if 'EXAM_MARK' not in self:
            print "Adding EXAM_MARK..."
            self.EXAM_MARK = Relation(['StudentId', 'CourseId', 'Mark'],
                                 [('S1', 'C1', 85),
                                  ('S1', 'C2', 49),
                                  ('S2', 'C1', 49),
                                  ('S3', 'C3', 66),
                                  ('S4', 'C1', 93),
                                 ]
                                )

    def _vinit(self):
        """Define virtual relvars/relconsts
           (Called repeatedly, e.g. after database load from disk or commit)
        """
        Database._vinit(self)

        if 'C_ER' not in self:
            print "Defining C_ER..."
            #this will always be the case, even when re-loading: we don't store relations with callable bodies
            self.C_ER = Relation(['CourseId', 'Exam_Result'],
                                           self.vC_ER,
                                           {'pk':(Key,['CourseId'])})

    def vC_ER(self):
        return self.COURSE.extend(['Exam_Result'], lambda t:{'Exam_Result':
                                                             (self.EXAM_MARK & GENERATE({'CourseId':t.CourseId})
                                                             )(['StudentId', 'Mark'])}
                                 )(['CourseId', 'Exam_Result']) #fixed


#Load or create the database
darwen = Database.open(darwen_Database, "darwen")

###################################
if __name__=="__main__":
    print darwen.relations
