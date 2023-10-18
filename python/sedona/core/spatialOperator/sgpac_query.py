from pyspark import RDD

from sedona.utils.decorators import require
from sedona.core.SpatialRDD.spatial_rdd import SpatialRDD


class SGPACQuery:

    @classmethod
    @require(["SGPACQuery"])
    def sgpac2L(cls, data: SpatialRDD, polygonLayer: SpatialRDD) -> RDD:
        """
        Two-Level Clipping
        :param data: SpatialRDD
        :param polygonLayer: SpatialRDD
        :return: key-value PairRDD
        """
        jvm = data._jvm
        srdd = jvm.SGPACQuery.SGPAC_2L(data._srdd, polygonLayer._srdd)
        return srdd

    @classmethod
    @require(["SGPACQuery"])
    def sgpac1L(cls, data: SpatialRDD, polygonLayer: SpatialRDD) -> RDD:
        """
        One-Level Clipping
        :param data: SpatialRDD
        :param polygonLayer: SpatialRDD
        :return: key-value PairRDD
        """
        jvm = data._jvm
        srdd = jvm.SGPACQuery.SGPAC_1L(data._srdd, polygonLayer._srdd)
        return srdd

    @classmethod
    @require(["SGPACQuery"])
    def sgpacQO(cls, data: SpatialRDD, polygonLayer: SpatialRDD, estimatorCellCount: int) -> RDD:
        """
        Query Optimizer
        :param data: SpatialRDD
        :param polygonLayer: SpatialRDD
        :return: key-value PairRDD
        """
        jvm = data._jvm
        srdd = jvm.SGPACQuery.SGPAC_QO(data._srdd, polygonLayer._srdd, estimatorCellCount)
        return srdd


    @classmethod
    @require(["SGPACQuery"])
    def sgpacFR(cls, data: SpatialRDD, polygonLayer: SpatialRDD) -> RDD:
        """
        Filter-Refine
        :param data: SpatialRDD
        :param polygonLayer: SpatialRDD
        :return: key-value PairRDD
        """
        jvm = data._jvm
        srdd = jvm.SGPACQuery.FilterRefine(data._srdd, polygonLayer._srdd)
        return srdd

    @classmethod
    @require(["SGPACQuery"])
    def sgpacJoin(cls, data: SpatialRDD, polygonLayer: SpatialRDD) -> RDD:
        """
        Join
        :param data: SpatialRDD
        :param polygonLayer: SpatialRDD
        :return: key-value PairRDD
        """
        jvm = data._jvm
        srdd = jvm.SGPACQuery.Join(data._srdd, polygonLayer._srdd)
        return srdd