
from sedona.utils.decorators import require
from sedona.core.SpatialRDD.spatial_rdd import SpatialRDD
from sedona.core.spatialOperator.rdd import SedonaPairRDD


class SGPACQuery:

    @classmethod
    @require(["SGPACQuery", "GeoSerializerData"])
    def sgpac2L(self, data: SpatialRDD, polygonLayer: SpatialRDD):
        jvm = data._jvm
        srdd = jvm.SGPACQuery.SGPAC_2L(data._srdd, polygonLayer._srdd)
        return SedonaPairRDD(srdd).to_rdd()


    @classmethod
    @require(["SGPACQuery", "GeoSerializerData"])
    def sgpac1L(self, data: SpatialRDD, polygonLayer: SpatialRDD):
        jvm = data._jvm
        srdd = jvm.SGPACQuery.SGPAC_1L(data._srdd, polygonLayer._srdd)
        return SedonaPairRDD(srdd).to_rdd()


    @classmethod
    @require(["SGPACQuery", "GeoSerializerData"])
    def sgpacQO(self, data: SpatialRDD, polygonLayer: SpatialRDD, estimatorCellCount: int):
        jvm = data._jvm
        srdd = jvm.SGPACQuery.SGPAC_QO(data._srdd, polygonLayer._srdd, estimatorCellCount)
        return SedonaPairRDD(srdd).to_rdd()

