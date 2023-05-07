from abc import ABC
from typing import List, Any

import attr
from pyspark import CPickleSerializer

from sedona.utils.binary_parser import BinaryParser, BinaryBuffer


class Vertex:
    def __init__(self, coordinates, incident_hedge):
        self.coordinates = coordinates
        self.incident_hedge = incident_hedge

    def __str__(self):
        return f"{self.coordinates}"

    def __repr__(self):
        return f"Vertex({self.coordinates}, incident edges={self.incident_hedge})"


class HalfEdge:
    def __init__(self, hedge, twin_hedge, next_hedge, incident_face, is_dangle, is_cutedge):
        self.hedge = hedge
        self.twin_hedge = twin_hedge
        self.next_hedge = next_hedge
        self.incident_face = incident_face
        self.is_dangle = bool(is_dangle)
        self.is_cutedge = bool(is_cutedge)

    def __str__(self):
        return f"{self.hedge}"

    def __repr__(self):
        return f"HalfEdge({self.hedge}, twin={self.twin_hedge}, next={self.next_hedge}, " \
               f"incident face={self.incident_face}, {'Dangle' if self.is_dangle else ''}," \
               f"{'Cut Edge' if self.is_cutedge else ''})"

class Face:
    def __init__(self, face, area):
        self.face = face
        self.area = area

    def __str__(self):
        return f"{self.face}"

    def __repr__(self):
        return f"Face({self.face}, area={self.area})"


@attr.s
class AbstractDDCELParser(ABC):

    @classmethod
    def serialize(cls, obj: List[Any], binary_buffer: 'BinaryBuffer') -> bytearray:
        raise NotImplemented()

    @classmethod
    def deserialize(cls, bin_parser: 'BinaryParser'):
        raise NotImplementedError("Parser has to implement deserialize method")



@attr.s
class VertexParser(AbstractDDCELParser):
    name = "VertexParser"

    @classmethod
    def deserialize(cls, bin_parser: 'BinaryParser') -> Vertex:
        geom_len = bin_parser.read_int()
        coordinates = bin_parser.read_geometry(geom_len)

        incident_hedge = []
        while bin_parser.has_next():
            geom_len = bin_parser.read_int()
            incident_hedge.append(bin_parser.read_geometry(geom_len))

        return Vertex(coordinates, incident_hedge)

    @classmethod
    def serialize(cls, obj: Vertex, binary_buffer: 'BinaryBuffer') -> bytearray:
        raise NotImplementedError("Currently this operation is not supported")


@attr.s
class HalfEdgeParser(AbstractDDCELParser):
    name = "HalfEdgeParser"

    @classmethod
    def deserialize(cls, bin_parser: 'BinaryParser') -> HalfEdge:
        hedge = bin_parser.read_geometry(bin_parser.read_int())
        twin_hedge = bin_parser.read_geometry(bin_parser.read_int())
        next_hedge = bin_parser.read_geometry(bin_parser.read_int())
        incident_face = bin_parser.read_geometry(bin_parser.read_int())
        _ = bin_parser.read_int()
        is_dangle = bin_parser.read_int()
        _ = bin_parser.read_int()
        is_cutedge = bin_parser.read_int()

        return HalfEdge(hedge, twin_hedge, next_hedge, incident_face, is_dangle, is_cutedge)

    @classmethod
    def serialize(cls, obj: Vertex, binary_buffer: 'BinaryBuffer') -> bytearray:
        raise NotImplementedError("Currently this operation is not supported")


@attr.s
class FaceParser(AbstractDDCELParser):
    name = "FaceParser"

    @classmethod
    def deserialize(cls, bin_parser: 'BinaryParser') -> Face:
        face = bin_parser.read_geometry(bin_parser.read_int())
        _ = bin_parser.read_int()
        area = bin_parser.read_double()

        return Face(face, area)

    @classmethod
    def serialize(cls, obj: Vertex, binary_buffer: 'BinaryBuffer') -> bytearray:
        raise NotImplementedError("Currently this operation is not supported")


PARSERS = {
    1: VertexParser(),
    2: HalfEdgeParser(),
    3: FaceParser()
}


class DDCELPickler(CPickleSerializer):

    def __init__(self):
        super().__init__()

    def loads(self, obj, encoding="bytes"):
        binary_parser = BinaryParser(obj)
        entry_type = binary_parser.read_int()
        parsed_row = PARSERS[entry_type].deserialize(binary_parser)
        return parsed_row

    def dumps(self, obj):
        raise NotImplementedError()
