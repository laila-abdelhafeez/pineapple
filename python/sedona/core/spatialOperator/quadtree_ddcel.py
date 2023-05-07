from typing import Union

from pyspark.context import SparkContext

from sedona.core.spatialOperator.rdd import DDCEL
from sedona.utils.decorators import require
from sedona.core.SpatialRDD.spatial_rdd import SpatialRDD
from sedona.core.enums.ddcel_enums import RemMethod, RepartitioningScheme, RemMethodJvm, RepartitioningSchemeJvm


class QuadTreeDDCEL:

    @require(["QuadTreeDDCEL"])
    def __init__(self, sc: SparkContext, spatialNetwork: SpatialRDD, capacity: int, maxTreeLevel: int,
                 remMethod: Union[str, RemMethod] = RemMethod.DDCEL_IC,
                 repartitioningScheme: Union[str, RepartitioningScheme] = RepartitioningScheme.MU,
                 sampleFraction: float = 0.2, minTreeLevel: int = -1):
        self._sc = sc
        self._jvm = spatialNetwork._jvm
        rem_method = self.get_rem_method_jvm(remMethod).jvm_instance
        rep_scheme = self.get_rep_scheme_jvm(repartitioningScheme).jvm_instance
        self.ddcel = self._jvm.QuadTreeDDCEL(spatialNetwork._srdd, rem_method, rep_scheme, sampleFraction, capacity,
                                             maxTreeLevel, minTreeLevel)

    @property
    def vertices(self):
        self.jvertices = self.ddcel.getVertices()
        return DDCEL(self.jvertices, self._sc).to_rdd()

    @property
    def halfedges(self):
        self.jhalfedges = self.ddcel.getHalfEdges()
        return DDCEL(self.jhalfedges, self._sc).to_rdd()

    @property
    def faces(self):
        self.j = self.ddcel.j()
        self.jfaces = []
        faces = []
        for i in range(self.j):
            jface = self.ddcel.getFacesAt(i)
            self.jfaces.append(jface)
            faces.append(DDCEL(jface, self._sc).to_rdd())
        return faces

    def get_rem_method_jvm(self, remMethod):
        if type(remMethod) == str:
            rem_method = RemMethodJvm(self._jvm, RemMethod.from_string(remMethod))
        elif type(remMethod) == RemMethod:
            rem_method = RemMethodJvm(self._jvm, remMethod)
        else:
            raise TypeError("remMethod should be str or RemMethod")
        return rem_method

    def get_rep_scheme_jvm(self, repartitioningScheme):
        if type(repartitioningScheme) == str:
            rep_scheme = RepartitioningSchemeJvm(self._jvm, RepartitioningScheme.from_string(repartitioningScheme))
        elif type(repartitioningScheme) == RepartitioningScheme:
            rep_scheme = RepartitioningSchemeJvm(self._jvm, repartitioningScheme)
        else:
            raise TypeError("repartitioningScheme should be str or RepartitioningScheme")
        return rep_scheme
