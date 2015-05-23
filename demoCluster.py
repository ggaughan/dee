"""Demo Cluster"""
from DeeCluster import Cluster

from date import *
from darwen import *

class demo_Cluster(Cluster):
    def __init__(self, name):
        """Define initial databases
           (Called once on cluster creation)"""
        Cluster.__init__(self, name)

        self.date = date
        self.darwen = darwen

#Create the cluster
demoCluster = demo_Cluster("demo")

###################################
if __name__=="__main__":
    print demoCluster.databases
