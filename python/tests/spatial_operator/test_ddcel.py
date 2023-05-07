import os

from sedona.core.SpatialRDD import LineStringRDD
from sedona.core.spatialOperator.quadtree_ddcel import QuadTreeDDCEL
from sedona.core.enums import FileDataSplitter
from tests import tests_resource
from tests.test_base import TestBase

dataPath = os.path.join(tests_resource, "california.csv")
splitter = FileDataSplitter.WKT
carryInputData = False

capacity = 2000
max_height = 100


class TestDDCEL(TestBase):

    def test_ddcel(self):
        data = LineStringRDD(self.sc, dataPath, splitter, carryInputData)
        ddcel = QuadTreeDDCEL(self.sc, data, capacity, max_height)
        with open("debug", "w") as debug:
            debug.write(str(ddcel.vertices.take(10)) + "\n")
            debug.write(str(ddcel.halfedges.take(10)) + "\n")
            for face in ddcel.faces:
                debug.write(str(face.take(10)) + "\n")
