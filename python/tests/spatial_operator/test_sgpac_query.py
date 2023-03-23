import os

from sedona.core.enums import FileDataSplitter, GridType, IndexType

from sedona.core.formatMapper import WktReader

from sedona.core.SpatialRDD import PointRDD
from sedona.core.spatialOperator.sgpac_query import SGPACQuery
from tests.test_base import TestBase
from tests.tools import tests_resource

dataPath = os.path.join(tests_resource, "data1M.csv")
offset = 0
splitter = FileDataSplitter.CSV
carryInputData = True

grid_type = GridType.QUADTREE
num_partitions = 200

indexType = IndexType.RTREE
buildIndexOnSpatialPartitionedRDD = True

polygonLayerPath = os.path.join(tests_resource, "ne_countries.csv")
wktColumn = 0
allowInvalidGeometries = False
skipSyntacticallyInvalidGeometries = True


class TestSGPACQuery(TestBase):

    def test_sgpac(self):
        data = PointRDD(self.sc, dataPath, offset, splitter, carryInputData)
        data.analyze()
        data.spatialPartitioning(grid_type, num_partitions)
        data.buildIndex(indexType, buildIndexOnSpatialPartitionedRDD)

        polygon_layer = WktReader.readToGeometryRDD(self.sc, polygonLayerPath, wktColumn, allowInvalidGeometries,
                                                    skipSyntacticallyInvalidGeometries)
        polygon_layer.spatialPartitioning(data.getPartitioner())

        expected_result = SGPACQuery.sgpacJoin(data, polygon_layer)
        expected_map = expected_result.collectAsMap()

        fr_result = SGPACQuery.sgpacFR(data, polygon_layer)
        fr_map = fr_result.collectAsMap()

        sgpac1l_result = SGPACQuery.sgpac1L(data, polygon_layer)
        sgpac1l_map = sgpac1l_result.collectAsMap()

        sgpac2l_result = SGPACQuery.sgpac2L(data, polygon_layer)
        sgpac2l_map = sgpac2l_result.collectAsMap()

        sgpac_qo_result = SGPACQuery.sgpacQO(data, polygon_layer, 10)
        sgpac_qo_map = sgpac_qo_result.collectAsMap()

        assert all((fr_map.get(k) == v for k, v in expected_map.items()))
        assert all((sgpac1l_map.get(k) == v for k, v in expected_map.items()))
        assert all((sgpac2l_map.get(k) == v for k, v in expected_map.items()))
        assert all((sgpac_qo_map.get(k) == v for k, v in expected_map.items()))

